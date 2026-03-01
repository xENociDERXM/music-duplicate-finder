# Music Duplicate Finder & Mover

Got music scattered across old phones, hard drives, and download folders? This tool compares your unsorted collection against your organized library and helps you clean up duplicates safely — with a review step before anything moves, and full undo support if you change your mind.

A Python script that compares an unsorted music folder against an organized library, identifies duplicates, and helps you clean them up — without ever blindly deleting anything.

Built for people who have accumulated music across multiple devices, downloads, and backups over the years and need a safe, reviewable way to deduplicate without losing anything important.

---

## How it works

Every file in your unsorted folder is compared against your organized library and placed into one of four categories:

| Category | Description | What happens |
|---|---|---|
| **Exact Match** | Same filename, metadata, bitrate, and size (within tolerance) | Auto-moved to Duplicates folder — no review needed |
| **Standard Duplicate** | Matches on chosen criteria, organized copy is equal or better quality | Shown for review, you choose which to move |
| **Higher Quality** | Matches but unsorted copy has a higher bitrate | Shown for review — you may want to replace your organized copy |
| **No Match** | Nothing found in organized library | Left completely untouched |

Exact matches are the only thing that moves automatically. Everything else goes through a Notepad review list where you make the final call.

---

## Features

- **Multi-threaded scanning** — reads metadata in parallel, 3–4x faster than single-threaded
- **Organized folder caching** — skips re-scanning unchanged files on subsequent runs
- **Fuzzy metadata matching** — catches slight tag inconsistencies (`"The All-American Rejects"` vs `"All-American Rejects"`)
- **Duration matching** — reduces false positives from songs that share a title but are different lengths
- **Configurable size tolerance** — files at the same bitrate often differ slightly in size due to embedded artwork and tags; tolerance handles this without false negatives
- **"Why not auto" explanations** — Standard Duplicates show exactly why they didn't qualify for auto-move (e.g. `size diff: 2.5%`)
- **AcoustID fingerprint matching** *(optional)* — compares actual audio content to catch renamed or retagged duplicates
- **Resume capability** — picks up where it left off if interrupted
- **Full CSV report** — every file, every decision, every run, all logged
- **Dry run mode** — preview everything before moving a single file

---

## Requirements

```
pip install mutagen tqdm rapidfuzz
```

| Package | Purpose |
|---|---|
| `mutagen` | Reading audio metadata and bitrate |
| `tqdm` | Progress bars |
| `rapidfuzz` | Fast fuzzy string matching |

**Optional — for audio fingerprint matching:**

Download `fpcalc` from [acoustid.org/chromaprint](https://acoustid.org/chromaprint) and add it to your PATH (or set the full path in the config). No API key required — all comparisons are done locally.

---

## Setup

1. Install dependencies:
   ```
   pip install mutagen tqdm rapidfuzz
   ```

2. Edit `music_config.json` and set your folder paths:
   ```json
   "folders": {
     "organized":      "C:\\Music\\Organized",
     "unsorted":       "C:\\Music\\Unsorted",
     "duplicates":     "C:\\Music\\Duplicates",
     "better_quality": "C:\\Music\\BetterQuality"
   }
   ```

3. Run:
   ```
   python find_music_duplicates.py
   ```

---

## Usage

```
python find_music_duplicates.py              # normal run
python find_music_duplicates.py --dry-run   # preview only, nothing moves
python find_music_duplicates.py --clear-cache   # delete cache and start fresh
python find_music_duplicates.py --clear-resume  # discard saved resume state
python find_music_duplicates.py --config my.json  # use a custom config file
```

On each run you'll be asked:
1. Which **match mode** to use (filename, metadata, either, or both)
2. Whether to run **audio fingerprint matching** (if enabled in config)
3. Which **Standard Duplicates** to move (via a Notepad review list)
4. Which **Higher Quality** files to move

---

## Match modes

| Mode | How it works |
|---|---|
| `1` — Filename only | Filenames must match |
| `2` — Metadata only | Title, artist, and album must match |
| `3` — Either | Filename OR metadata matches |
| `4` — Both *(recommended)* | Filename AND metadata must both match on the same file |

Mode 4 is the safest — it prevents false positives where two different songs happen to share a filename or have similar tags.

---

## Configuration

All settings live in `music_config.json`. Key options:

| Setting | Default | Description |
|---|---|---|
| `mode` | `"4"` | Default match mode |
| `fuzzy_enabled` | `true` | Use fuzzy matching for tags |
| `fuzzy_threshold` | `88` | 0–100, how similar tags must be |
| `use_duration` | `true` | Include track length in matching |
| `duration_tolerance_seconds` | `2` | Max duration difference to still match |
| `exact_match_size_tolerance_percent` | `3.0` | Max size difference (%) for auto-move |
| `max_threads` | `0` (auto) | Parallel threads for scanning |
| `cache_enabled` | `true` | Cache organized folder metadata |
| `acoustid.enabled` | `false` | Offer fingerprint matching each run |
| `acoustid.similarity_threshold` | `85` | How similar fingerprints must be |

---

## Audio fingerprint matching

When enabled, the script runs a second pass on any files that had no filename/metadata match. It uses [Chromaprint](https://acoustid.org/chromaprint) (`fpcalc`) to generate an audio fingerprint for each file and compares it against fingerprints of your organized library.

This catches duplicates that have been renamed or retagged — cases that filename and metadata matching can't find.

Fingerprints are cached in `music_fp_cache.json` so subsequent runs are fast. The first run will be slower depending on library size.

To enable:
1. Download `fpcalc` from [acoustid.org/chromaprint](https://acoustid.org/chromaprint)
2. Add it to your PATH or set `"fpcalc_path"` in the config
3. Set `"acoustid" → "enabled": true` in the config

---

## Undoing a run

`undo_duplicates.py` reads the CSV report from any previous run and moves files back to their original locations.

```
python undo_duplicates.py "C:\Music\Duplicates\music_report_20260228_120000.csv"
python undo_duplicates.py "C:\Music\Duplicates\music_report_20260228_120000.csv" --dry-run
python undo_duplicates.py "C:\Music\Duplicates\music_report_20260228_120000.csv" --filter-category "Exact Match"
```

---

## Supported formats

MP3, FLAC, AAC, M4A

---

## Files

| File | Description |
|---|---|
| `find_music_duplicates.py` | Main script |
| `undo_duplicates.py` | Restore files from a previous run |
| `music_config.json` | All configuration settings |
| `music_cache.json` | Auto-generated metadata cache (organized folder) |
| `music_fp_cache.json` | Auto-generated fingerprint cache (if fingerprinting used) |
| `music_resume.json` | Auto-generated resume state (deleted on completion) |
