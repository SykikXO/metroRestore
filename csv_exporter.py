"""
Reusable export logic for Metrolist song.db → CSV.
Returns CSV data as in-memory strings or writes to a directory.
"""

import csv
import io
import os
import sqlite3
import shutil
import tempfile
import zipfile
from datetime import datetime

# ── SQL ──────────────────────────────────────────────────────────
_BASE = """
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

LIKED_Q = _BASE + " WHERE s.liked = 1 ORDER BY s.likedDate DESC"
MOST_PLAYED_Q = _BASE + " WHERE s.totalPlayTime > 0 ORDER BY s.totalPlayTime DESC"
ALL_Q = _BASE + " ORDER BY s.title COLLATE NOCASE"


# ── Helpers ──────────────────────────────────────────────────────
def _ms_to_mins(ms):
    if not ms or ms <= 0:
        return "0m 0s"
    t = ms // 1000
    m, s = divmod(t, 60)
    return f"{m}m {s}s"


def _epoch_to_date(epoch_ms):
    if not epoch_ms:
        return ""
    try:
        return datetime.fromtimestamp(epoch_ms / 1000).strftime("%Y-%m-%d")
    except (OSError, ValueError):
        return ""


def _duration_fmt(secs):
    if secs is None or secs < 0:
        return ""
    m, s = divmod(secs, 60)
    return f"{m}:{s:02d}"


def _rows_to_csv(header, rows):
    """Return a CSV string from header + rows."""
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    return buf.getvalue()


# ── Core export ──────────────────────────────────────────────────
def extract_db_from_backup(backup_path):
    """Extract song.db from a .backup ZIP into a temp dir.

    Returns (db_path, temp_dir).  Caller must clean up temp_dir.
    """
    if not zipfile.is_zipfile(backup_path):
        raise ValueError("Not a valid .backup / ZIP file")

    tmp = tempfile.mkdtemp(prefix="metrolist_")
    with zipfile.ZipFile(backup_path, "r") as zf:
        if "song.db" not in zf.namelist():
            shutil.rmtree(tmp)
            raise ValueError("song.db not found inside backup")
        zf.extract("song.db", tmp)
    return os.path.join(tmp, "song.db"), tmp


def generate_csvs(db_path):
    """Run all 3 queries and return a dict of {filename: csv_string}.

    Also returns a stats dict with row counts.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    result = {}
    stats = {}

    # 1 ── Liked
    cur.execute(LIKED_Q)
    rows = [
        [t, a, _epoch_to_date(ld)]
        for t, a, _al, _d, _pt, _lk, ld in cur.fetchall()
    ]
    result["liked_songs.csv"] = _rows_to_csv(["title", "artist", "liked_date"], rows)
    stats["liked_songs"] = len(rows)

    # 2 ── Most played
    cur.execute(MOST_PLAYED_Q)
    rows = [
        [t, a, _ms_to_mins(pt)]
        for t, a, _al, _d, pt, _lk, _ld in cur.fetchall()
    ]
    result["most_played.csv"] = _rows_to_csv(["title", "artist", "total_play_time"], rows)
    stats["most_played"] = len(rows)

    # 3 ── All cached
    cur.execute(ALL_Q)
    rows = [
        [t, a, _duration_fmt(d), al or ""]
        for t, a, al, d, _pt, _lk, _ld in cur.fetchall()
    ]
    result["cached_songs.csv"] = _rows_to_csv(["title", "artist", "duration", "album"], rows)
    stats["cached_songs"] = len(rows)

    conn.close()
    return result, stats
