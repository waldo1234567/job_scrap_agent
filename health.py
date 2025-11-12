from flask import Flask, request, jsonify
import os, json
import sqlite3, os, requests, threading,time

app = Flask(__name__)


API_KEY = os.getenv("INGEST_API_KEY", "")

DATA_DIR = os.getenv("DATA_DIR", "/tmp/data")
os.makedirs(DATA_DIR, exist_ok=True)

@app.route("/health")
def health():
    ok = True
    details={}
    
    try:
        conn = sqlite3.connect('/jobs.db',timeout=2)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*)")
        details['jobs'] = cur.fetchone()[0]
        conn.close()
    
    except Exception as e:
        ok =False
        details['db_error'] = str(e)
    
    chrome = os.getenv("CHROME_REMOTE_URL")
    
    try:
        r = requests.get(chrome.rstrip('/') + "/status", timeout=3) # type: ignore
        details['chrome_status'] = r.status_code
        if r.status_code != 200:
            ok = False
    except Exception as e:
        ok = False
        details['chrome_error'] = str(e)
    return jsonify({"ok": ok, "details": details})


@app.route("/ingest/jobs", methods=["POST"])
def ingest_jobs():
    key= request.headers.get("X-API-KEY", "")
    if API_KEY and key != API_KEY:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    payload = request.get_json(force=True)
    if not payload:
        return jsonify({"ok": False, "error": "no json"}), 400

    
    jobs = payload.get("jobs") or []
    if not isinstance(jobs, list):
        return jsonify({"ok": False, "error": "jobs must be list"}), 400
    
    from datetime import datetime
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(DATA_DIR, f"ingest_{ts}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)
    
    return jsonify({"ok": True, "received": len(jobs)}), 200


def _run_health_server():
    app.run(host="0.0.0.0", port=5001, debug=False, threaded=True)
    
def start_health_background():
    t = threading.Thread(target=_run_health_server, daemon=True)
    t.start()
    time.sleep(0.2)
    