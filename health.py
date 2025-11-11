from flask import Flask,jsonify
import sqlite3, os, requests

app = Flask(__name__)

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
    