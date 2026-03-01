"""
Music Duplicate Finder & Mover  v3.1
======================================
Compares an unsorted music folder against an organized library and categorises
every file into one of four buckets:

  EXACT       filename + metadata + size + bitrate + duration all match
              → auto-moved to Duplicates folder (no review needed)

  DUPLICATE   matches on chosen criteria, unsorted is NOT better quality
              → Notepad review, then optionally moved to Duplicates folder

  BETTER      matches on chosen criteria, unsorted IS higher bitrate
              → Notepad review, then optionally moved to Better Quality folder

  NO MATCH    no match found in organized library
              → left untouched (or caught by fingerprint pass)

Features
---------
  • Multi-threaded scanning      (parallel metadata reads, 3-4x faster)
  • Organized folder caching     (skip re-scanning unchanged files)
  • Track duration matching      (reduces false positives)
  • Fuzzy metadata matching      (catches slight tag inconsistencies)
  • Exact-match size tolerance   (configurable %, handles tag-size differences)
  • tqdm progress bars           (see scan progress in real time)
  • JSON config file             (no need to edit the script)
  • Resume capability            (picks up after a crash or interruption)
  • CSV report                   (full record of every file and action)
  • "Why not exact" reasons      (explains why duplicates didn't auto-move)
  • AcoustID fingerprint pass    (optional: catches renamed/retagged duplicates)

Supports: MP3, FLAC, AAC/M4A

Requirements:
    pip install mutagen tqdm rapidfuzz

Optional (for fingerprint matching):
    Download fpcalc from https://acoustid.org/chromaprint and add to PATH
    No API key required — fingerprints are compared locally.

Usage:
    python find_music_duplicates.py                    # normal run
    python find_music_duplicates.py --dry-run          # preview only
    python find_music_duplicates.py --clear-cache      # delete cache and start fresh
    python find_music_duplicates.py --clear-resume     # discard saved resume state
    python find_music_duplicates.py --config my.json   # use a custom config file
"""

import os
import sys
import csv
import json
import shutil
import logging
import tempfile
import hashlib
import subprocess
import threading
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Optional dependencies with graceful fallbacks ──────────────────────────

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("WARNING: 'tqdm' not installed. Progress bars disabled.")
    print("         Run:  pip install tqdm\n")

try:
    from rapidfuzz import fuzz as _fuzz
    def fuzzy_score(a: str, b: str) -> float:
        return _fuzz.token_sort_ratio(a, b)
    FUZZY_AVAILABLE = True
except ImportError:
    import difflib
    def fuzzy_score(a: str, b: str) -> float:
        return difflib.SequenceMatcher(None, a, b).ratio() * 100
    FUZZY_AVAILABLE = False
    print("WARNING: 'rapidfuzz' not installed. Falling back to difflib for fuzzy matching.")
    print("         For better performance run:  pip install rapidfuzz\n")

try:
    from mutagen import File as MutagenFile
except ImportError:
    print("ERROR: 'mutagen' is not installed.")
    print("       Run:  pip install mutagen")
    sys.exit(1)

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════════

DEFAULT_CONFIG_FILE = "music_config.json"
SUPPORTED_EXTENSIONS = {".mp3", ".flac", ".aac", ".m4a"}
DRY_RUN       = "--dry-run"      in sys.argv
CLEAR_CACHE   = "--clear-cache"  in sys.argv
CLEAR_RESUME  = "--clear-resume" in sys.argv

# Parse --config flag
_cfg_flag = next((sys.argv[i + 1] for i, a in enumerate(sys.argv) if a == "--config" and i + 1 < len(sys.argv)), None)
CONFIG_FILE = _cfg_flag or DEFAULT_CONFIG_FILE


def load_config(path: str) -> dict:
    """Load and validate the JSON config file."""
    cfg_path = Path(path)
    if not cfg_path.exists():
        print(f"ERROR: Config file not found: {cfg_path}")
        print(f"       Please create '{DEFAULT_CONFIG_FILE}' next to this script,")
        print(f"       or download the sample config included with the script.")
        sys.exit(1)

    with open(cfg_path, encoding="utf-8") as f:
        cfg = json.load(f)

    folders  = cfg.get("folders", {})
    required = ["organized", "unsorted", "duplicates", "better_quality"]
    for key in required:
        if not folders.get(key):
            print(f"ERROR: Config missing folders.{key}")
            sys.exit(1)

    return cfg


CFG = load_config(CONFIG_FILE)

ORGANIZED_FOLDER      = CFG["folders"]["organized"]
UNSORTED_FOLDER       = CFG["folders"]["unsorted"]
DUPLICATES_FOLDER     = CFG["folders"]["duplicates"]
BETTER_QUALITY_FOLDER = CFG["folders"]["better_quality"]

_match_cfg            = CFG.get("matching", {})
DEFAULT_MODE          = str(_match_cfg.get("mode", "4"))
FUZZY_ENABLED         = bool(_match_cfg.get("fuzzy_enabled", True))
FUZZY_THRESHOLD       = float(_match_cfg.get("fuzzy_threshold", 88))
USE_DURATION          = bool(_match_cfg.get("use_duration", True))
DURATION_TOLERANCE    = float(_match_cfg.get("duration_tolerance_seconds", 2))
EXACT_SIZE_TOLERANCE  = float(_match_cfg.get("exact_match_size_tolerance_percent", 3.0)) / 100.0

_perf_cfg             = CFG.get("performance", {})
_max_threads          = int(_perf_cfg.get("max_threads", 0))
MAX_THREADS           = _max_threads if _max_threads > 0 else (os.cpu_count() or 4)
CACHE_ENABLED         = bool(_perf_cfg.get("cache_enabled", True))
CACHE_FILE            = _perf_cfg.get("cache_file", "music_cache.json")

_resume_cfg           = CFG.get("resume", {})
RESUME_ENABLED        = bool(_resume_cfg.get("enabled", True))
RESUME_FILE           = _resume_cfg.get("resume_file", "music_resume.json")

_out_cfg              = CFG.get("output", {})
_log_folder           = _out_cfg.get("log_folder", "")
LOG_FOLDER            = _log_folder if _log_folder else DUPLICATES_FOLDER

_acoustid_cfg         = CFG.get("acoustid", {})
ACOUSTID_OFFER        = bool(_acoustid_cfg.get("enabled", False))
FPCALC_PATH           = _acoustid_cfg.get("fpcalc_path", "fpcalc")
FP_SIMILARITY_THRESHOLD = float(_acoustid_cfg.get("similarity_threshold", 85))
FP_CACHE_FILE         = _acoustid_cfg.get("fp_cache_file", "music_fp_cache.json")


# ══════════════════════════════════════════════════════════════════════════════
#  PROGRESS BAR HELPER
# ══════════════════════════════════════════════════════════════════════════════

def make_pbar(total: int, desc: str, unit: str = "file"):
    """Return a tqdm bar if available, otherwise a no-op stub."""
    if TQDM_AVAILABLE:
        return tqdm(total=total, desc=desc, unit=unit, ncols=80, dynamic_ncols=True)
    class _NoopBar:
        def update(self, n=1): pass
        def close(self): pass
        def set_postfix_str(self, s): pass
    return _NoopBar()


# ══════════════════════════════════════════════════════════════════════════════
#  CACHE
# ══════════════════════════════════════════════════════════════════════════════

_cache_lock = threading.Lock()

def _cache_key(path: Path) -> str:
    """Stable cache key: path + mtime + size."""
    stat = path.stat()
    raw  = f"{path}|{stat.st_mtime}|{stat.st_size}"
    return hashlib.md5(raw.encode()).hexdigest()


def load_cache(cache_file: str) -> dict:
    p = Path(cache_file)
    if p.exists():
        try:
            with open(p, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_cache(cache: dict, cache_file: str):
    try:
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        print(f"WARNING: Could not save cache: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  RESUME STATE
# ══════════════════════════════════════════════════════════════════════════════

def load_resume(resume_file: str) -> dict | None:
    p = Path(resume_file)
    if p.exists():
        try:
            with open(p, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return None


def save_resume(state: dict, resume_file: str):
    try:
        with open(resume_file, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"WARNING: Could not save resume state: {e}")


def clear_resume(resume_file: str):
    try:
        Path(resume_file).unlink(missing_ok=True)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
#  METADATA & SCANNING
# ══════════════════════════════════════════════════════════════════════════════

def get_file_info(path: Path) -> dict:
    """Read all metadata from a file in a single mutagen call."""
    size = path.stat().st_size
    meta = {"title": "", "artist": "", "album": "", "bitrate": None, "duration": None}

    try:
        audio = MutagenFile(path)
        if audio is not None:
            easy = MutagenFile(path, easy=True)
            if easy:
                def first(tag):
                    val = easy.get(tag)
                    return val[0].strip().lower() if val else ""
                meta["title"]  = first("title")
                meta["artist"] = first("artist")
                meta["album"]  = first("album")
            if hasattr(audio, "info"):
                info = audio.info
                if hasattr(info, "bitrate"):
                    meta["bitrate"] = info.bitrate
                if hasattr(info, "length"):
                    meta["duration"] = info.length
    except Exception:
        pass

    return {
        "path":     path,
        "filename": path.name.lower(),
        "size":     size,
        "metadata": meta,
    }


def get_file_info_cached(path: Path, cache: dict) -> dict:
    """Return cached file info if valid, otherwise scan and update cache."""
    key = _cache_key(path)
    with _cache_lock:
        if key in cache:
            entry = cache[key]
            entry["path"] = path
            return entry

    info = get_file_info(path)

    serialisable = {k: v for k, v in info.items() if k != "path"}
    serialisable["path_str"] = str(path)

    with _cache_lock:
        cache[key] = serialisable

    return info


def scan_folder_threaded(folder: Path, desc: str, cache: dict | None = None) -> list[dict]:
    """Recursively collect and read metadata for all supported music files."""
    all_paths = [
        p for p in folder.rglob("*")
        if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS
    ]

    results = []
    pbar    = make_pbar(len(all_paths), desc)

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        if cache is not None:
            futures = {executor.submit(get_file_info_cached, p, cache): p for p in all_paths}
        else:
            futures = {executor.submit(get_file_info, p): p for p in all_paths}

        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                pbar.set_postfix_str(f"Error: {e}")
            pbar.update(1)

    pbar.close()
    return results


# ══════════════════════════════════════════════════════════════════════════════
#  MATCHING
# ══════════════════════════════════════════════════════════════════════════════

def _str_match(a: str, b: str) -> bool:
    if not FUZZY_ENABLED:
        return a == b
    if a == b:
        return True
    if not a and not b:
        return True
    if not a or not b:
        return False
    return fuzzy_score(a, b) >= FUZZY_THRESHOLD


def metadata_matches(u_meta: dict, o_meta: dict) -> bool:
    if not (_str_match(u_meta.get("title", ""),  o_meta.get("title", "")) and
            _str_match(u_meta.get("artist", ""), o_meta.get("artist", "")) and
            _str_match(u_meta.get("album", ""),  o_meta.get("album", ""))):
        return False

    if USE_DURATION:
        u_dur = u_meta.get("duration")
        o_dur = o_meta.get("duration")
        if u_dur is not None and o_dur is not None:
            if abs(u_dur - o_dur) > DURATION_TOLERANCE:
                return False

    return True


def find_match(unsorted_file: dict, organized_index: dict, mode: str) -> dict | None:
    filename = unsorted_file["filename"]
    u_meta   = unsorted_file["metadata"]

    filename_match = None
    metadata_match = None

    if filename in organized_index:
        filename_match = organized_index[filename][0]

    if mode in {"2", "3", "4"}:
        for entries in organized_index.values():
            for entry in entries:
                if metadata_matches(u_meta, entry["metadata"]):
                    metadata_match = entry
                    break
            if metadata_match:
                break

    if mode == "1":
        return filename_match
    elif mode == "2":
        return metadata_match
    elif mode == "3":
        return filename_match or metadata_match
    elif mode == "4":
        # FIX: Both conditions must be satisfied by the SAME file.
        # Previously a filename match against one file and a metadata match
        # against a different file could both be True, causing false positives.
        if filename_match and metadata_matches(u_meta, filename_match["metadata"]):
            return filename_match
        return None
    return None


def is_exact_match(u: dict, o: dict) -> bool:
    """True only if filename, size (within tolerance), bitrate, and duration all match."""
    if u["filename"] != o["filename"]:
        return False

    # Size tolerance: differences within the threshold are tag/artwork overhead.
    if EXACT_SIZE_TOLERANCE == 0:
        if u["size"] != o["size"]:
            return False
    else:
        larger = max(u["size"], o["size"])
        if larger > 0 and abs(u["size"] - o["size"]) / larger > EXACT_SIZE_TOLERANCE:
            return False

    u_br = u["metadata"].get("bitrate") or 0
    o_br = o["metadata"].get("bitrate") or 0
    if u_br != o_br:
        return False

    # Duration within 1 second (floating-point tolerance)
    u_dur = u["metadata"].get("duration")
    o_dur = o["metadata"].get("duration")
    if u_dur is not None and o_dur is not None:
        if abs(u_dur - o_dur) > 1:
            return False

    if not metadata_matches(u["metadata"], o["metadata"]):
        return False

    return True


def why_not_exact(u: dict, o: dict) -> str:
    """
    Return a short human-readable reason why two matched files did not
    qualify as an exact match. Shown in the Notepad review list and log.
    """
    reasons = []

    larger = max(u["size"], o["size"])
    if larger > 0:
        pct = abs(u["size"] - o["size"]) / larger * 100
        if pct > EXACT_SIZE_TOLERANCE * 100:
            reasons.append(f"size diff: {pct:.1f}% (tolerance: {EXACT_SIZE_TOLERANCE * 100:.1f}%)")

    u_br = u["metadata"].get("bitrate") or 0
    o_br = o["metadata"].get("bitrate") or 0
    if u_br != o_br:
        u_kbps = f"{u_br // 1000}" if u_br else "?"
        o_kbps = f"{o_br // 1000}" if o_br else "?"
        reasons.append(f"bitrate: {u_kbps} kbps vs {o_kbps} kbps")

    u_dur = u["metadata"].get("duration")
    o_dur = o["metadata"].get("duration")
    if u_dur is not None and o_dur is not None and abs(u_dur - o_dur) > 1:
        reasons.append(f"duration: {format_duration(u_dur)} vs {format_duration(o_dur)}")

    return ", ".join(reasons) if reasons else "minor tag difference"


def unsorted_is_better(u: dict, o: dict) -> bool:
    u_bitrate = u["metadata"].get("bitrate") or 0
    o_bitrate = o["metadata"].get("bitrate") or 0
    # FIX: Bitrate only — size alone at the same bitrate just means more tag
    # data (embedded art, extra tags), not better audio quality.
    return u_bitrate > o_bitrate


def categorise(unsorted_files: list, organized_index: dict, mode: str,
               already_processed: set | None = None) -> dict:
    """Sort files into exact / duplicate / better / no_match buckets."""
    exact     = []
    duplicate = []
    better    = []
    no_match  = []

    pbar = make_pbar(len(unsorted_files), "Categorising", "file")

    for uf in unsorted_files:
        path_str = str(uf["path"])
        if already_processed and path_str in already_processed:
            pbar.update(1)
            continue

        match = find_match(uf, organized_index, mode)
        item  = {"unsorted": uf, "match": match}

        if match is None:
            no_match.append(item)
        elif is_exact_match(uf, match):
            exact.append(item)
        elif unsorted_is_better(uf, match):
            better.append(item)
        else:
            item["why_not_exact"] = why_not_exact(uf, match)
            duplicate.append(item)

        pbar.update(1)

    pbar.close()
    return {"exact": exact, "duplicate": duplicate, "better": better, "no_match": no_match}


# ══════════════════════════════════════════════════════════════════════════════
#  FORMATTING HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def format_size(b: int) -> str:
    if b < 1024:       return f"{b} B"
    elif b < 1024**2:  return f"{b/1024:.1f} KB"
    elif b < 1024**3:  return f"{b/(1024**2):.1f} MB"
    else:              return f"{b/(1024**3):.2f} GB"

def format_bitrate(br) -> str:
    return "Unknown" if br is None else f"{br // 1000} kbps"

def format_duration(d) -> str:
    if d is None:
        return "Unknown"
    m, s = divmod(int(d), 60)
    return f"{m}:{s:02d}"


# ══════════════════════════════════════════════════════════════════════════════
#  NOTEPAD HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def build_notepad_list(title: str, items: list, extra_header: str, dry_run: bool) -> str:
    DIVIDER = "-" * 68
    lines   = []

    lines.append("=" * 68)
    lines.append(f"  {title}: {len(items)}")
    if extra_header:
        lines.append(f"  {extra_header}")
    if dry_run:
        lines.append("  *** DRY RUN: No files will be moved ***")
    lines.append("=" * 68)
    lines.append("")
    lines.append("  Use the numbers below to select which files to move.")
    lines.append("  Examples: 'all'  |  '1,3,5'  |  '1-10'  |  '1,3-5,7'  |  'none'")
    lines.append("")
    lines.append("=" * 68)

    for i, dup in enumerate(items, start=1):
        u = dup["unsorted"]
        o = dup["match"]
        lines.append("")
        lines.append(DIVIDER)
        lines.append("")
        lines.append(f"  [{i}]")
        lines.append(f"  UNSORTED : {u['path']}")
        lines.append(f"             Size: {format_size(u['size'])}  |  "
                     f"Bitrate: {format_bitrate(u['metadata'].get('bitrate'))}  |  "
                     f"Duration: {format_duration(u['metadata'].get('duration'))}")
        lines.append(f"  MATCHES  : {o['path']}")
        lines.append(f"             Size: {format_size(o['size'])}  |  "
                     f"Bitrate: {format_bitrate(o['metadata'].get('bitrate'))}  |  "
                     f"Duration: {format_duration(o['metadata'].get('duration'))}")

        if dup.get("match_method"):
            lines.append(f"  METHOD   : {dup['match_method']}")

        if dup.get("why_not_exact"):
            lines.append(f"  WHY NOT AUTO: {dup['why_not_exact']}")

        lines.append("")

    lines.append(DIVIDER)
    lines.append("")
    return "\r\n".join(lines)


def open_in_notepad(content: str) -> str:
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", prefix="music_duplicates_",
        delete=False, encoding="utf-8",
    )
    tmp.write(content)
    tmp.close()
    subprocess.Popen(["notepad.exe", tmp.name])
    return tmp.name


# ══════════════════════════════════════════════════════════════════════════════
#  SELECTION HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def parse_selection(user_input: str, total: int) -> set[int]:
    user_input = user_input.strip().lower()
    if user_input == "all":
        return set(range(total))
    if user_input in {"none", "0", ""}:
        return set()

    selected = set()
    for part in user_input.replace(" ", "").split(","):
        if "-" in part:
            try:
                start, end = part.split("-", 1)
                for i in range(int(start) - 1, int(end)):
                    if 0 <= i < total:
                        selected.add(i)
            except ValueError:
                print(f"  Skipping invalid range: '{part}'")
        else:
            try:
                idx = int(part) - 1
                if 0 <= idx < total:
                    selected.add(idx)
                else:
                    print(f"  Number {part} is out of range, skipping.")
            except ValueError:
                print(f"  Skipping invalid entry: '{part}'")
    return selected


def prompt_selection(label: str, items: list) -> set[int]:
    print()
    print("=" * 70)
    print(f"  {label}")
    print("  Examples: 'all'  |  '1,3,5'  |  '1-10'  |  '1,3-5,7'  |  'none'")
    print("=" * 70)

    while True:
        raw = input("  Your selection: ").strip()
        if raw.lower() in {"none", "0", ""}:
            print("\n  Nothing selected.")
            return set()
        selected_indices = parse_selection(raw, len(items))
        confirm = input(f"\n  You selected {len(selected_indices)} file(s). Proceed? (y/n): ").strip().lower()
        if confirm == "y":
            return selected_indices
        elif confirm == "n":
            print("  Selection cancelled. Please re-enter.")
        else:
            print("  Please enter y or n.")


# ══════════════════════════════════════════════════════════════════════════════
#  FILE MOVING
# ══════════════════════════════════════════════════════════════════════════════

def move_file(src: Path, dest_root: Path, source_root: Path) -> Path:
    try:
        rel = src.relative_to(source_root)
    except ValueError:
        rel = src.name

    dest = dest_root / rel
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists():
        stem, suffix, counter = dest.stem, dest.suffix, 1
        while dest.exists():
            dest = dest.parent / f"{stem}_({counter}){suffix}"
            counter += 1

    shutil.move(str(src), str(dest))
    return dest


def process_batch(items, selected_indices, dest_folder, unsorted, log, move_label, dry_run,
                  resume_state: dict | None = None) -> tuple[int, list[dict]]:
    """Move or preview selected items. Returns (count, list of CSV row dicts)."""
    count    = 0
    csv_rows = []
    pbar     = make_pbar(len(selected_indices), f"Moving {move_label}", "file")

    for i, dup in enumerate(items):
        if i not in selected_indices:
            continue

        u     = dup["unsorted"]
        o     = dup["match"]
        u_sz  = format_size(u["size"])
        o_sz  = format_size(o["size"])
        u_br  = format_bitrate(u["metadata"].get("bitrate"))
        o_br  = format_bitrate(o["metadata"].get("bitrate"))
        u_dur = format_duration(u["metadata"].get("duration"))
        o_dur = format_duration(o["metadata"].get("duration"))
        reason = dup.get("why_not_exact", "")
        method = dup.get("match_method", "")

        if dry_run:
            try:
                rel = u["path"].relative_to(unsorted)
            except ValueError:
                rel = u["path"].name
            dest_path = str(dest_folder / rel)
            log.info(f"[{move_label} - WOULD MOVE] {u['path']}")
            log.info(f"             Size: {u_sz}  |  Bitrate: {u_br}  |  Duration: {u_dur}")
            log.info(f"  Matches  : {o['path']}")
            log.info(f"             Size: {o_sz}  |  Bitrate: {o_br}  |  Duration: {o_dur}")
            if method:
                log.info(f"  Method   : {method}")
            if reason:
                log.info(f"  Why not auto: {reason}")
            log.info(f"  Would go : {dest_path}\n")
            action = f"Would move to {move_label}"
        else:
            dest      = move_file(u["path"], dest_folder, unsorted)
            dest_path = str(dest)
            log.info(f"[{move_label} - MOVED]   {u['path']}")
            log.info(f"          Size: {u_sz}  |  Bitrate: {u_br}  |  Duration: {u_dur}")
            log.info(f"  Matches : {o['path']}")
            log.info(f"          Size: {o_sz}  |  Bitrate: {o_br}  |  Duration: {o_dur}")
            if method:
                log.info(f"  Method  : {method}")
            if reason:
                log.info(f"  Why not auto: {reason}")
            log.info(f"  Moved to: {dest_path}\n")
            action = f"Moved to {move_label}"

            if resume_state is not None:
                resume_state["processed"].add(str(u["path"]))
                save_resume(
                    {"processed": list(resume_state["processed"]), "mode": resume_state["mode"]},
                    RESUME_FILE,
                )

        csv_rows.append({
            "category":      move_label,
            "action":        action,
            "unsorted":      u,
            "match":         o,
            "dest_path":     dest_path,
            "why_not_exact": reason,
            "match_method":  method,
        })

        count += 1
        pbar.update(1)

    pbar.close()
    return count, csv_rows


# ══════════════════════════════════════════════════════════════════════════════
#  AUDIO FINGERPRINTING  (optional — requires fpcalc / Chromaprint)
# ══════════════════════════════════════════════════════════════════════════════

def check_fpcalc(fpcalc_path: str) -> bool:
    """Return True if fpcalc is available and working."""
    try:
        result = subprocess.run(
            [fpcalc_path, "-version"],
            capture_output=True, text=True, timeout=5,
        )
        return result.returncode == 0
    except Exception:
        return False


def get_fingerprint(path: Path, fpcalc_path: str) -> str | None:
    """Generate a raw Chromaprint fingerprint string for an audio file."""
    try:
        result = subprocess.run(
            [fpcalc_path, "-raw", str(path)],
            capture_output=True, text=True, timeout=60,
        )
        for line in result.stdout.splitlines():
            if line.startswith("FINGERPRINT="):
                return line.split("=", 1)[1].strip()
    except Exception:
        pass
    return None


def fingerprint_similarity(fp1: str, fp2: str) -> float:
    """
    Compare two raw Chromaprint fingerprints, return 0–100 similarity.
    Fingerprints are comma-separated 32-bit integers. Similarity is measured
    by the fraction of bits that agree across the overlap of both fingerprints.
    """
    try:
        ints1 = list(map(int, fp1.split(",")))
        ints2 = list(map(int, fp2.split(",")))
        length = min(len(ints1), len(ints2))
        if length == 0:
            return 0.0
        total_bits = length * 32
        diff_bits  = sum(bin(a ^ b).count("1") for a, b in zip(ints1[:length], ints2[:length]))
        return (1 - diff_bits / total_bits) * 100
    except Exception:
        return 0.0


def build_fp_index(organized_files: list, fpcalc_path: str, fp_cache: dict) -> list[dict]:
    """
    Generate (or load from cache) fingerprints for all organized files.
    Returns a list of {file, fingerprint} dicts.
    """
    index = []
    pbar  = make_pbar(len(organized_files), "Fingerprinting library", "file")

    for f in organized_files:
        path_str = str(f["path"])
        fp = fp_cache.get(path_str) or get_fingerprint(f["path"], fpcalc_path)
        if fp:
            fp_cache[path_str] = fp
            index.append({"file": f, "fingerprint": fp})
        pbar.update(1)

    pbar.close()
    return index


def fingerprint_pass(no_match_items: list, org_fp_index: list, fpcalc_path: str,
                     fp_cache: dict, threshold: float) -> tuple[list, list, list]:
    """
    Run fingerprint matching on files that had no filename/metadata match.

    For each no-match file:
      1. Generate its fingerprint (or load from cache)
      2. Compare against all organized library fingerprints
      3. If best similarity >= threshold, categorise as duplicate or better

    Returns (new_duplicates, new_better, still_no_match).
    """
    new_duplicates = []
    new_better     = []
    still_no_match = []

    pbar = make_pbar(len(no_match_items), "Fingerprint matching", "file")

    for item in no_match_items:
        u        = item["unsorted"]
        path_str = str(u["path"])

        fp_u = fp_cache.get(path_str) or get_fingerprint(u["path"], fpcalc_path)
        if fp_u:
            fp_cache[path_str] = fp_u

        best_match = None
        best_score = 0.0

        if fp_u:
            for entry in org_fp_index:
                score = fingerprint_similarity(fp_u, entry["fingerprint"])
                if score > best_score:
                    best_score = score
                    if score >= threshold:
                        best_match = entry["file"]

        if best_match:
            method     = f"audio fingerprint ({best_score:.0f}% match)"
            match_item = {
                "unsorted":      u,
                "match":         best_match,
                "match_method":  method,
                "why_not_exact": "matched by fingerprint only (different filename/tags)",
            }
            u_br = u["metadata"].get("bitrate") or 0
            o_br = best_match["metadata"].get("bitrate") or 0
            if u_br > o_br:
                new_better.append(match_item)
            else:
                new_duplicates.append(match_item)
        else:
            still_no_match.append(item)

        pbar.update(1)

    pbar.close()
    return new_duplicates, new_better, still_no_match


# ══════════════════════════════════════════════════════════════════════════════
#  CSV REPORT
# ══════════════════════════════════════════════════════════════════════════════

def write_csv_report(csv_path: str, all_rows: list, mode_label: str, run_label: str, timestamp: str):
    fieldnames = [
        "Run Mode", "Timestamp", "Match Mode",
        "Category", "Action", "Match Method",
        "Unsorted Path",
        "Unsorted Size (bytes)", "Unsorted Size",
        "Unsorted Bitrate (bps)", "Unsorted Bitrate",
        "Unsorted Duration",
        "Unsorted Title", "Unsorted Artist", "Unsorted Album",
        "Match Path",
        "Match Size (bytes)", "Match Size",
        "Match Bitrate (bps)", "Match Bitrate",
        "Match Duration",
        "Match Title", "Match Artist", "Match Album",
        "Filename Match", "Metadata Match", "Size Match", "Bitrate Match", "Duration Match",
        "Size Diff (bytes)", "Bitrate Diff (bps)", "Duration Diff (sec)",
        "Why Not Exact",
        "Destination Path",
    ]

    def make_row(row: dict) -> dict:
        u      = row["unsorted"]
        o      = row.get("match")
        u_meta = u["metadata"]
        o_meta = o["metadata"] if o else {}

        u_br  = u_meta.get("bitrate") or 0
        o_br  = (o_meta.get("bitrate") or 0) if o else 0
        u_dur = u_meta.get("duration")
        o_dur = o_meta.get("duration") if o else None

        fn_match   = (u["filename"] == o["filename"])          if o else ""
        meta_match = metadata_matches(u_meta, o_meta)          if o else ""
        sz_match   = (u["size"] == o["size"])                  if o else ""
        br_match   = (u_br == o_br)                            if o else ""
        dur_match  = (abs(u_dur - o_dur) <= DURATION_TOLERANCE
                      if (u_dur is not None and o_dur is not None and o) else "")

        return {
            "Run Mode":              run_label,
            "Timestamp":             timestamp,
            "Match Mode":            mode_label,
            "Category":              row["category"],
            "Action":                row["action"],
            "Match Method":          row.get("match_method", "filename/metadata"),
            "Unsorted Path":         str(u["path"]),
            "Unsorted Size (bytes)": u["size"],
            "Unsorted Size":         format_size(u["size"]),
            "Unsorted Bitrate (bps)": u_br,
            "Unsorted Bitrate":      format_bitrate(u_meta.get("bitrate")),
            "Unsorted Duration":     format_duration(u_dur),
            "Unsorted Title":        u_meta.get("title", ""),
            "Unsorted Artist":       u_meta.get("artist", ""),
            "Unsorted Album":        u_meta.get("album", ""),
            "Match Path":            str(o["path"]) if o else "",
            "Match Size (bytes)":    o["size"] if o else "",
            "Match Size":            format_size(o["size"]) if o else "",
            "Match Bitrate (bps)":   o_br if o else "",
            "Match Bitrate":         format_bitrate(o_meta.get("bitrate")) if o else "",
            "Match Duration":        format_duration(o_dur) if o else "",
            "Match Title":           o_meta.get("title", "") if o else "",
            "Match Artist":          o_meta.get("artist", "") if o else "",
            "Match Album":           o_meta.get("album", "") if o else "",
            "Filename Match":        fn_match,
            "Metadata Match":        meta_match,
            "Size Match":            sz_match,
            "Bitrate Match":         br_match,
            "Duration Match":        dur_match,
            "Size Diff (bytes)":     (u["size"] - o["size"]) if o else "",
            "Bitrate Diff (bps)":    (u_br - o_br) if o else "",
            "Duration Diff (sec)":   (round(u_dur - o_dur, 2)
                                      if (u_dur is not None and o_dur is not None and o) else ""),
            "Why Not Exact":         row.get("why_not_exact", ""),
            "Destination Path":      row.get("dest_path", ""),
        }

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows([make_row(r) for r in all_rows])


# ══════════════════════════════════════════════════════════════════════════════
#  MATCH MODE SELECTION
# ══════════════════════════════════════════════════════════════════════════════

MODE_DESCRIPTIONS = {
    "1": "Filename only",
    "2": "Metadata only",
    "3": "Either (filename OR metadata)",
    "4": "Both (filename AND metadata)",
}

def choose_match_mode() -> str:
    print()
    print("=" * 60)
    print("  How should duplicates be detected?")
    print(f"  (Default from config: {DEFAULT_MODE} - {MODE_DESCRIPTIONS[DEFAULT_MODE]})")
    print("=" * 60)
    print("  1 - Filename only       (filenames must match)")
    print("  2 - Metadata only       (title, artist & album must match)")
    print("  3 - Either              (filename OR metadata matches)")
    print("  4 - Both (recommended)  (filename AND metadata must both match)")
    print(f"  [Enter] to use default ({DEFAULT_MODE})")
    print("=" * 60)

    while True:
        choice = input("  Enter 1, 2, 3, or 4 (or press Enter for default): ").strip()
        if choice == "":
            choice = DEFAULT_MODE
        if choice in {"1", "2", "3", "4"}:
            print(f"\n  Selected: {MODE_DESCRIPTIONS[choice]}\n")
            return choice
        print("  Invalid choice. Please enter 1, 2, 3, or 4.")


def prompt_fingerprint_matching() -> bool:
    """Ask whether to run the audio fingerprint pass this run."""
    print()
    print("=" * 60)
    print("  Audio Fingerprint Matching (Chromaprint)")
    print("=" * 60)
    print("  Reads actual audio content to catch duplicates with different")
    print("  filenames or tags (renamed / retagged files).")
    print()
    print("  NOTE: Slower — fpcalc decodes each audio file fully.")
    print("        Results are cached so future runs are much faster.")
    print("=" * 60)

    while True:
        choice = input("  Use fingerprint matching this run? (y/n): ").strip().lower()
        if choice == "y":
            return True
        elif choice == "n":
            return False
        print("  Please enter y or n.")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    organized     = Path(ORGANIZED_FOLDER)
    unsorted      = Path(UNSORTED_FOLDER)
    duplicates    = Path(DUPLICATES_FOLDER)
    better_folder = Path(BETTER_QUALITY_FOLDER)
    log_folder    = Path(LOG_FOLDER)

    for folder, label in [(organized, "Organized"), (unsorted, "Unsorted")]:
        if not folder.exists():
            print(f"ERROR: {label} folder not found: {folder}")
            return

    # ── Handle CLI flags ──
    if CLEAR_CACHE:
        Path(CACHE_FILE).unlink(missing_ok=True)
        print("Cache cleared.")
    if CLEAR_RESUME:
        clear_resume(RESUME_FILE)
        print("Resume state cleared.")
    if CLEAR_CACHE or CLEAR_RESUME:
        if len(sys.argv) == 2:
            return

    # ── Check for resume state ──
    resume_state      = None
    already_processed = set()

    if RESUME_ENABLED and not CLEAR_RESUME:
        saved = load_resume(RESUME_FILE)
        if saved:
            print("\n" + "=" * 60)
            print("  A previous run was interrupted.")
            print(f"  {len(saved.get('processed', []))} file(s) were already processed.")
            resume_choice = input("  Resume from where it left off? (y/n): ").strip().lower()
            if resume_choice == "y":
                already_processed = set(saved.get("processed", []))
                resume_state = {"processed": already_processed, "mode": saved.get("mode", DEFAULT_MODE)}
                print(f"  Resuming. {len(already_processed)} files will be skipped.\n")
            else:
                clear_resume(RESUME_FILE)

    # ── Choose match mode ──
    mode = resume_state["mode"] if (resume_state and resume_state.get("mode")) else choose_match_mode()
    mode_label_str = MODE_DESCRIPTIONS[mode]

    # ── Offer fingerprint matching if enabled in config and fpcalc is available ──
    use_fingerprints = False
    if ACOUSTID_OFFER:
        if check_fpcalc(FPCALC_PATH):
            use_fingerprints = prompt_fingerprint_matching()
        else:
            print(f"\n  NOTE: Fingerprint matching is enabled in config but fpcalc was not found.")
            print(f"        Download from https://acoustid.org/chromaprint and add to PATH.")
            print(f"        Skipping fingerprint matching for this run.\n")

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    run_label = "DRY RUN" if DRY_RUN else "LIVE RUN"

    print(f"  Threads          : {MAX_THREADS}")
    print(f"  Fuzzy match      : {'enabled (threshold: ' + str(FUZZY_THRESHOLD) + ')' if FUZZY_ENABLED else 'disabled'}")
    print(f"  Duration         : {'enabled (tolerance: ±' + str(DURATION_TOLERANCE) + 's)' if USE_DURATION else 'disabled'}")
    print(f"  Size tolerance   : {EXACT_SIZE_TOLERANCE * 100:.1f}%")
    print(f"  Fingerprinting   : {'enabled' if use_fingerprints else 'disabled'}")
    print(f"  Cache            : {'enabled' if CACHE_ENABLED else 'disabled'}")
    print()

    # ── Load metadata cache ──
    org_cache = {}
    if CACHE_ENABLED:
        if not CLEAR_CACHE:
            org_cache = load_cache(CACHE_FILE)
        print(f"  Cache loaded: {len(org_cache)} entries.")

    # ── Scan organized folder ──
    print()
    organized_files = scan_folder_threaded(organized, "Scanning organized", cache=org_cache if CACHE_ENABLED else None)
    print(f"  Organized: {len(organized_files)} files found.")

    if CACHE_ENABLED:
        save_cache(org_cache, CACHE_FILE)
        print(f"  Cache saved: {len(org_cache)} entries.")

    # ── Scan unsorted folder (no cache — changes frequently) ──
    print()
    unsorted_files = scan_folder_threaded(unsorted, "Scanning unsorted")
    print(f"  Unsorted:  {len(unsorted_files)} files found.")

    # ── Build organized index ──
    organized_index: dict[str, list[dict]] = {}
    for f in organized_files:
        organized_index.setdefault(f["filename"], []).append(f)

    # ── Initialise resume state ──
    if resume_state is None and RESUME_ENABLED:
        resume_state = {"processed": set(), "mode": mode}
    elif resume_state:
        resume_state["mode"] = mode

    # ── Categorise by filename / metadata ──
    print()
    cats = categorise(unsorted_files, organized_index, mode, already_processed)

    # ── Optional fingerprint pass on no-match files ──
    fp_cache    = {}
    fp_log_info = ""
    if use_fingerprints and cats["no_match"]:
        fp_cache = load_cache(FP_CACHE_FILE)
        print(f"\n  Fingerprint cache loaded: {len(fp_cache)} entries.")
        print()

        org_fp_index = build_fp_index(organized_files, FPCALC_PATH, fp_cache)
        print(f"  Library fingerprints ready: {len(org_fp_index)} files.")
        print()

        fp_dups, fp_better, fp_no_match = fingerprint_pass(
            cats["no_match"], org_fp_index, FPCALC_PATH, fp_cache, FP_SIMILARITY_THRESHOLD
        )

        save_cache(fp_cache, FP_CACHE_FILE)

        cats["duplicate"].extend(fp_dups)
        cats["better"].extend(fp_better)
        cats["no_match"] = fp_no_match

        if fp_dups or fp_better:
            fp_log_info = (
                f"Fingerprint pass: {len(fp_dups)} duplicate(s), "
                f"{len(fp_better)} better quality match(es) found"
            )
        else:
            fp_log_info = "Fingerprint pass: no additional matches found"
        print(f"\n  {fp_log_info}")

    print(f"\n  Exact matches (auto-move)          : {len(cats['exact'])}")
    print(f"  Standard duplicates (for review)   : {len(cats['duplicate'])}")
    print(f"  Higher quality matches (for review): {len(cats['better'])}")
    print(f"  No match (untouched)               : {len(cats['no_match'])}")

    # ── Auto-move exact matches ──
    if not DRY_RUN and cats["exact"]:
        duplicates.mkdir(parents=True, exist_ok=True)

    auto_csv_rows  = []
    auto_log_lines = []

    pbar_exact = make_pbar(len(cats["exact"]), "Auto-moving exact matches")
    for item in cats["exact"]:
        u = item["unsorted"]
        if DRY_RUN:
            try:
                rel = u["path"].relative_to(unsorted)
            except ValueError:
                rel = u["path"].name
            dest_path = str(duplicates / rel)
            auto_log_lines.append(f"  [EXACT - WOULD AUTO-MOVE] {u['path']}")
            auto_log_lines.append(f"    → {dest_path}")
            action = "Would auto-move to Duplicates"
        else:
            dest = move_file(u["path"], duplicates, unsorted)
            dest_path = str(dest)
            auto_log_lines.append(f"  [EXACT - AUTO-MOVED] {u['path']}")
            auto_log_lines.append(f"    → {dest_path}")
            action = "Auto-moved to Duplicates"
            if resume_state:
                resume_state["processed"].add(str(u["path"]))

        auto_csv_rows.append({
            "category":      "Exact Match",
            "action":        action,
            "unsorted":      u,
            "match":         item["match"],
            "dest_path":     dest_path,
            "why_not_exact": "",
            "match_method":  "filename/metadata",
        })
        pbar_exact.update(1)
    pbar_exact.close()

    if cats["exact"]:
        verb = "would be auto-moved" if DRY_RUN else "auto-moved"
        print(f"\n  {len(cats['exact'])} exact match(es) {verb} to Duplicates folder.")

    # ── Step 1: Standard duplicates ──
    dup_selected = set()
    if cats["duplicate"]:
        content = build_notepad_list(
            "STANDARD DUPLICATES", cats["duplicate"],
            f"Match mode: {mode_label_str}", DRY_RUN,
        )
        tmp1 = open_in_notepad(content)
        print("\n  Standard duplicates list opened in Notepad.")
        dup_selected = prompt_selection(
            "Select standard duplicates to move to Duplicates folder:",
            cats["duplicate"],
        )
        try:
            os.remove(tmp1)
        except Exception:
            pass
    else:
        print("\n  No standard duplicates to review.")

    # ── Step 2: Higher quality matches ──
    better_selected = set()
    if cats["better"]:
        content2 = build_notepad_list(
            "HIGHER QUALITY MATCHES (unsorted is higher bitrate)",
            cats["better"],
            f"These will be moved to: {BETTER_QUALITY_FOLDER}", DRY_RUN,
        )
        tmp2 = open_in_notepad(content2)
        print("\n  Higher quality matches list opened in Notepad.")
        better_selected = prompt_selection(
            "Select higher quality files to move to Better Quality folder:",
            cats["better"],
        )
        try:
            os.remove(tmp2)
        except Exception:
            pass
    else:
        print("\n  No higher quality matches to review.")

    # ── Set up logging ──
    if not DRY_RUN:
        log_folder.mkdir(parents=True, exist_ok=True)
        if dup_selected:
            duplicates.mkdir(parents=True, exist_ok=True)
        if better_selected:
            better_folder.mkdir(parents=True, exist_ok=True)

    log_path = str(log_folder / f"music_log_{timestamp}.txt")
    csv_path = str(log_folder / f"music_report_{timestamp}.csv")

    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    log = logging.getLogger()

    log.info("\n" + "=" * 60)
    log.info(f"Music Duplicate Finder  [{run_label}]")
    if DRY_RUN:
        log.info("*** DRY RUN: No files will be moved ***")
    log.info(f"Started                : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"Match mode             : {mode_label_str}")
    log.info(f"Fuzzy matching         : {'enabled (threshold: ' + str(FUZZY_THRESHOLD) + ')' if FUZZY_ENABLED else 'disabled'}")
    log.info(f"Duration matching      : {'enabled (±' + str(DURATION_TOLERANCE) + 's)' if USE_DURATION else 'disabled'}")
    log.info(f"Exact size tolerance   : {EXACT_SIZE_TOLERANCE * 100:.1f}%")
    log.info(f"Fingerprint matching   : {'enabled (threshold: ' + str(FP_SIMILARITY_THRESHOLD) + '%)' if use_fingerprints else 'disabled'}")
    log.info(f"Threads                : {MAX_THREADS}")
    log.info(f"Organized folder       : {organized}")
    log.info(f"Unsorted folder        : {unsorted}")
    log.info(f"Duplicates folder      : {duplicates}")
    log.info(f"Better quality folder  : {better_folder}")
    log.info("=" * 60 + "\n")

    if fp_log_info:
        log.info(f"  {fp_log_info}\n")

    # Log exact matches
    if auto_log_lines:
        log.info("── EXACT MATCHES (auto-moved) ──\n")
        for line in auto_log_lines:
            log.info(line)
        log.info("")

    # Move and log standard duplicates
    moved_dups, dup_csv_rows = 0, []
    if dup_selected:
        log.info("── STANDARD DUPLICATES ──\n")
        moved_dups, dup_csv_rows = process_batch(
            cats["duplicate"], dup_selected, duplicates, unsorted, log, "DUPLICATE", DRY_RUN, resume_state
        )

    # Move and log better quality
    moved_better, better_csv_rows = 0, []
    if better_selected:
        log.info("── HIGHER QUALITY MATCHES ──\n")
        moved_better, better_csv_rows = process_batch(
            cats["better"], better_selected, better_folder, unsorted, log, "BETTER QUALITY", DRY_RUN, resume_state
        )

    # No-match rows for CSV
    no_match_csv_rows = [
        {"category": "No Match", "action": "Kept", "unsorted": item["unsorted"],
         "match": None, "dest_path": "", "why_not_exact": "", "match_method": ""}
        for item in cats["no_match"]
    ]

    # Unselected duplicate rows
    unsel_dup_rows = [
        {"category": "Standard Duplicate", "action": "Kept (not selected)",
         "unsorted": cats["duplicate"][i]["unsorted"], "match": cats["duplicate"][i]["match"],
         "dest_path": "", "why_not_exact": cats["duplicate"][i].get("why_not_exact", ""),
         "match_method": cats["duplicate"][i].get("match_method", "")}
        for i in range(len(cats["duplicate"])) if i not in dup_selected
    ]
    unsel_better_rows = [
        {"category": "Higher Quality Match", "action": "Kept (not selected)",
         "unsorted": cats["better"][i]["unsorted"], "match": cats["better"][i]["match"],
         "dest_path": "", "why_not_exact": cats["better"][i].get("why_not_exact", ""),
         "match_method": cats["better"][i].get("match_method", "")}
        for i in range(len(cats["better"])) if i not in better_selected
    ]

    all_csv_rows = (auto_csv_rows + dup_csv_rows + better_csv_rows +
                    unsel_dup_rows + unsel_better_rows + no_match_csv_rows)

    # Write CSV
    try:
        if not DRY_RUN:
            os.makedirs(str(log_folder), exist_ok=True)
        write_csv_report(csv_path, all_csv_rows, mode_label_str, run_label, timestamp)
        log.info(f"\nCSV report saved to: {csv_path}")
    except Exception as e:
        log.info(f"\nWARNING: Could not write CSV report: {e}")

    # Clear resume state on successful completion
    if not DRY_RUN:
        clear_resume(RESUME_FILE)

    # ── Summary ──
    log.info("\n" + "=" * 60)
    log.info(f"SUMMARY  [{run_label}]")
    log.info(f"  Match mode                        : {mode_label_str}")
    log.info(f"  Total unsorted files scanned      : {len(unsorted_files)}")
    log.info(f"  Exact matches (auto-moved)        : {len(cats['exact'])}")
    log.info(f"  Standard duplicates found         : {len(cats['duplicate'])}")
    log.info(f"  Higher quality matches found      : {len(cats['better'])}")
    log.info(f"  No match (untouched)              : {len(cats['no_match'])}")
    log.info(f"  ---")
    if DRY_RUN:
        log.info(f"  Standard duplicates would move    : {moved_dups}")
        log.info(f"  Higher quality files would move   : {moved_better}")
    else:
        log.info(f"  Standard duplicates moved         : {moved_dups}")
        log.info(f"  Higher quality files moved        : {moved_better}")
    log.info(f"\n  Log saved to : {log_path}")
    log.info(f"  CSV saved to : {csv_path}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
