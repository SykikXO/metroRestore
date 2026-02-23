"""
Metrolist CSV Export — Flask Web App
"""

import os
import shutil
import threading
import time
import uuid
import tempfile

from flask import Flask, render_template, request, jsonify, send_file, abort

from csv_exporter import extract_db_from_backup, generate_csvs

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB upload limit

# ── In-memory session store ──────────────────────────────────────
# { session_id: { "dir": tmpdir, "files": {name: path}, "created": time } }
_sessions = {}
_SESSION_TTL = 600  # 10 minutes


def _cleanup_loop():
    """Background thread that removes expired sessions."""
    while True:
        time.sleep(60)
        now = time.time()
        expired = [
            sid for sid, s in list(_sessions.items())
            if now - s["created"] > _SESSION_TTL
        ]
        for sid in expired:
            d = _sessions.pop(sid, {}).get("dir")
            if d and os.path.isdir(d):
                shutil.rmtree(d, ignore_errors=True)


threading.Thread(target=_cleanup_loop, daemon=True).start()


# ── Routes ───────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    f = request.files.get("backup")
    if not f or f.filename == "":
        return jsonify({"error": "No file uploaded"}), 400

    # Save uploaded file to temp
    tmp_upload = tempfile.NamedTemporaryFile(delete=False, suffix=".backup")
    f.save(tmp_upload)
    tmp_upload.close()

    try:
        db_path, tmp_dir = extract_db_from_backup(tmp_upload.name)
    except ValueError as e:
        os.unlink(tmp_upload.name)
        return jsonify({"error": str(e)}), 400
    finally:
        if os.path.exists(tmp_upload.name):
            os.unlink(tmp_upload.name)

    try:
        csvs, stats = generate_csvs(db_path)
    except Exception as e:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return jsonify({"error": f"Processing failed: {e}"}), 500

    # Write CSVs to a session directory
    session_id = uuid.uuid4().hex[:12]
    out_dir = tempfile.mkdtemp(prefix=f"metro_{session_id}_")
    file_map = {}
    for name, content in csvs.items():
        path = os.path.join(out_dir, name)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(content)
        file_map[name] = path

    # Clean up the extracted db dir
    shutil.rmtree(tmp_dir, ignore_errors=True)

    _sessions[session_id] = {
        "dir": out_dir,
        "files": file_map,
        "created": time.time(),
    }

    return jsonify({
        "session_id": session_id,
        "stats": stats,
        "files": list(file_map.keys()),
    })


@app.route("/download/<session_id>/<filename>")
def download(session_id, filename):
    session = _sessions.get(session_id)
    if not session:
        abort(404, description="Session expired or not found")
    path = session["files"].get(filename)
    if not path or not os.path.isfile(path):
        abort(404, description="File not found")
    return send_file(path, as_attachment=True, download_name=filename)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
