#!/usr/bin/env python3
"""
Export songs from a Metrolist .backup (or song.db) into 3 CSV files:
  1. liked_songs.csv   – Songs marked as liked
  2. most_played.csv   – Songs ordered by total play time (descending)
  3. cached_songs.csv  – Every song in the database

Usage:
  python3 export_songs.py Metrolist_20260224032843.backup
  python3 export_songs.py song.db
"""

import csv
import sqlite3
import sys
import os
import glob
import shutil
import tempfile
import zipfile
from datetime import datetime


def extract_db(backup_path: str) -> tuple[str, str]:
    """Extract song.db from a .backup ZIP file.

    Returns (db_path, temp_dir) — caller must clean up temp_dir.
    """
    if not zipfile.is_zipfile(backup_path):
        print(f"Error: '{backup_path}' is not a valid ZIP / .backup file")
        sys.exit(1)

    tmp = tempfile.mkdtemp(prefix="metrolist_")
    with zipfile.ZipFile(backup_path, "r") as zf:
        names = zf.namelist()
        if "song.db" not in names:
            print(f"Error: 'song.db' not found inside '{backup_path}'")
            print(f"  Archive contains: {names}")
            shutil.rmtree(tmp)
            sys.exit(1)
        zf.extract("song.db", tmp)
        print(f"Extracted song.db from: {backup_path}")

    return os.path.join(tmp, "song.db"), tmp

# ── SQL fragments ────────────────────────────────────────────────
BASE_SELECT = """
    SELECT
        s.title,
        COALESCE(a.name, 'Unknown Artist') AS artist,
        s.albumName                        AS album,
        s.duration,
        s.totalPlayTime,
        s.liked,
        s.likedDate
    FROM song s
    LEFT JOIN song_artist_map sam ON s.id = sam.songId AND sam.position = 0
    LEFT JOIN artist a           ON a.id = sam.artistId
"""

LIKED_QUERY = BASE_SELECT + " WHERE s.liked = 1 ORDER BY s.likedDate DESC"
MOST_PLAYED_QUERY = BASE_SELECT + " WHERE s.totalPlayTime > 0 ORDER BY s.totalPlayTime DESC"
ALL_QUERY = BASE_SELECT + " ORDER BY s.title COLLATE NOCASE"


def ms_to_mins(ms: int) -> str:
    """Convert milliseconds to a human-readable 'Xm Ys' string."""
    if ms <= 0:
        return "0m 0s"
    total_secs = ms // 1000
    m, s = divmod(total_secs, 60)
    return f"{m}m {s}s"


def epoch_to_date(epoch_ms: int | None) -> str:
    """Convert epoch milliseconds to YYYY-MM-DD, or '' if missing."""
    if not epoch_ms:
        return ""
    try:
        return datetime.fromtimestamp(epoch_ms / 1000).strftime("%Y-%m-%d")
    except (OSError, ValueError):
        return ""


def duration_secs(ms: int) -> str:
    """Convert duration in seconds (stored as -1 if unknown)."""
    if ms is None or ms < 0:
        return ""
    m, s = divmod(ms, 60)
    return f"{m}:{s:02d}"


def write_csv(path: str, header: list[str], rows: list[list], label: str):
    """Write rows to a CSV file and print a summary."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"  ✔ {label:20s} → {path}  ({len(rows)} songs)")


def export(db_path: str):
    if not os.path.isfile(db_path):
        print(f"Error: database not found at '{db_path}'")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    print(f"Connected to: {db_path}\n")

    # ── 1. Liked songs ───────────────────────────────────────────
    cur.execute(LIKED_QUERY)
    liked_rows = [
        [title, artist, epoch_to_date(likedDate)]
        for title, artist, _album, _dur, _pt, _liked, likedDate in cur.fetchall()
    ]
    write_csv("liked_songs.csv", ["title", "artist", "liked_date"], liked_rows, "Liked Songs")

    # ── 2. Most played ───────────────────────────────────────────
    cur.execute(MOST_PLAYED_QUERY)
    played_rows = [
        [title, artist, ms_to_mins(totalPlayTime)]
        for title, artist, _album, _dur, totalPlayTime, _liked, _ld in cur.fetchall()
    ]
    write_csv("most_played.csv", ["title", "artist", "total_play_time"], played_rows, "Most Played")

    # ── 3. All cached songs ──────────────────────────────────────
    cur.execute(ALL_QUERY)
    all_rows = [
        [title, artist, duration_secs(dur), album or ""]
        for title, artist, album, dur, _pt, _liked, _ld in cur.fetchall()
    ]
    write_csv("cached_songs.csv", ["title", "artist", "duration", "album"], all_rows, "Cached Songs")

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    # ── Resolve input path ───────────────────────────────────────
    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        # Auto-detect a Metrolist .backup file in the current directory
        backups = sorted(glob.glob("Metrolist*.backup"))
        if backups:
            path = backups[-1]  # newest by filename timestamp
            print(f"Auto-detected backup: {path}")
        else:
            print("Usage: python3 export_songs.py <Metrolist.backup | song.db>")
            sys.exit(1)

    # ── Extract if needed, then export ───────────────────────────
    tmp_dir = None
    if path.endswith(".backup") or zipfile.is_zipfile(path):
        db_path, tmp_dir = extract_db(path)
    else:
        db_path = path

    try:
        export(db_path)
    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir)
