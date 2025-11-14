import gradio as gr
import threading, time, json, io
import pandas as pd
import threading
from typing import List
from jobdb import JobDatabase
from datetime import datetime
from gr_helper.render_jobs import render_job_cards_clickable
import os
from flask import request, jsonify
from starlette.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

_logs: List[str] = []
_lock = threading.Lock()
API_KEY = os.getenv("INGEST_API_KEY", "")

DATA_DIR = os.getenv("DATA_DIR", "/tmp/data")
os.makedirs(DATA_DIR, exist_ok=True)

GRADIO_USER = os.getenv("GRADIO_AUTH_USER", "admin")
GRADIO_PASS = os.getenv("GRADIO_AUTH_PASS", "changeme")
username = os.environ.get("DB_USERNAME")
password = os.environ.get("PASSWORD")
host = os.environ.get("HOST")
port = os.environ.get("DB_PORT")
dbname = os.environ.get("DBNAME")

db = JobDatabase()
print("Connected to database")

def _append_log(msg: str):
    with _lock:
        _logs.append(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def get_logs() -> str:
    with _lock :
        return "\n".join(_logs[-500:]) or "No logs yet."
    
    
        
def fetch_stats() -> str:
    all_jobs =db.get_all_jobs(status='new', limit=1000)
    scored = [j for j in all_jobs if (j.get('ai_score') or 0) > 0]
    total = len(all_jobs)
    scored_count = len(scored)
    avg = round(sum(j.get('ai_score', 0) for j in scored)/ scored_count, 1) if scored_count else 0
    high = len([j for j in scored if j['ai_score'] >= 70])
    mid = len([j for j in scored if j['ai_score'] < 70])
    low = len([j for j in scored if j['ai_score'] < 45])
    
    md = (
        f"### Database Stats\n\n"
        f"- Total jobs: **{total}**\n"
        f"- Scored: **{scored_count}**\n"
        f"- Avg score: **{avg}**\n"
        f"- High(70+): **{high}**, Medium(40-69): **{mid}**, Low(<40): **{low}**\n"
    )
    _append_log(f"Stats refreshed: {total} jobs, {scored_count} scored")
    
    return md

def parse_date_posted(s: str):
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    for fmt in ("%m/%d/%Y", "%m/%d"):
        try:
            dt = datetime.strptime(s, fmt)
            if fmt == "%m/%d":
                dt =dt.replace(year=datetime.now().year)
            return dt.date()
        except Exception:
            continue
    
    return None
                
                
def top_jobs_table(min_score: int = 70, limit: int = 10, sort_by: str = "score_desc", source_filter: str = "All"):
    conn = db.get_connection()
    query = """
            SELECT id, title, company, location, ai_score, ai_analysis, url, date_posted, source 
            FROM jobs 
            WHERE ai_score >= %s
        """
    params = [min_score]
        
    if source_filter != "All":
        query += " AND source = %s"
        params.append(source_filter) # type: ignore
        
    df = pd.read_sql_query(query, conn, params=params) # type: ignore
    db.return_connection(conn)
    
    if df.empty:
        return pd.DataFrame([{"message":"No jobs match the criteria"}])
    
    df['ai_score'] = pd.to_numeric(df['ai_score'], errors='coerce').fillna(0).astype(int)
    df['parsed_date'] = df.get('date_posted', '').apply(parse_date_posted) # type: ignore
    
    if sort_by == "score_desc":
        df = df.sort_values(by=['ai_score', 'parsed_date'], ascending=[False, False])
    elif sort_by == "score_asc":
        df = df.sort_values(by=['ai_score', 'parsed_date'], ascending=[True, False])
    elif sort_by == "newest":
        df = df.assign(parsed_date_null = df['parsed_date'].isna())
        df = df.sort_values(by=['parsed_date_null', 'parsed_date', 'ai_score'], ascending=[True, False, False])
        df = df.drop(columns=['parsed_date_null'])
    elif sort_by == "oldest":
        df = df.assign(parsed_date_null = df['parsed_date'].isna())
        df = df.sort_values(by=['parsed_date_null', 'parsed_date', 'ai_score'], ascending=[True, True, False])
        df = df.drop(columns=['parsed_date_null'])
    else:
        df = df.sort_values(by=['ai_score', 'parsed_date'], ascending=[False, False])
        
    out =df.head(int(limit)).copy()
    out = out[['id','title','company','location','ai_score','ai_analysis','url','date_posted']]
    
    
    _append_log(f"Displayed {len(out)} jobs (min_score={min_score}, sort={sort_by})")
    return out

def export_csv():
    conn = db.get_connection()
    df = pd.read_sql_query("SELECT * FROM jobs", conn)
    conn.close()
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    return("all_jobs.csv", buffer.getvalue())

    
    
def show_job_detail(job_id:int):
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
    row = cursor.fetchone()
        
    db.return_connection(conn)
    if not row:
        return f"Job {job_id} not found"

    d= dict(row)
    raw_posted = d.get("date_posted")
    try:
        parsed = parse_date_posted(raw_posted)   # type: ignore
    except Exception:
        parsed = None

    if parsed:
        date_str = parsed.isoformat() 
    else:
        date_str = raw_posted if raw_posted else "Unknown"
        
    pretty = json.dumps({
        "id": d.get("id"),
        "title": d.get("title"),
        "company": d.get("company"),
        "description" : d.get('description'),
        "date posted" : date_str, # type: ignore
        "location": d.get("location"),
        "score": d.get("ai_score"),
        "analysis": d.get("ai_analysis"),
        "url": d.get("url"),
        "source": d.get("source")
    }, ensure_ascii=False, indent=2)
    return pretty

with gr.Blocks(title="Job Info Dashboard") as demo:
    gr.Markdown("# Job Matcher — Dashboard")
    with gr.Row():
        with gr.Column(scale=2):
            stats_md = gr.Markdown(fetch_stats)
            with gr.Row():
                batch_slider = gr.Slider(minimum=1, maximum=20, value=6, step=1, label="Batch size")
                max_batches= gr.Number(value=20, precision=0, label="Max Batches")
            with gr.Row():
                run_btn = gr.Button("Run Now (background)")
                refresh_btn = gr.Button("Refresh stats")
                export_btn = gr.Button("Export CSV") 
            logs_area = gr.Textbox(label="Logs (latest)", value=get_logs(), lines=12)
        with gr.Column(scale=3):
            gr.Markdown("### Top Matches")
            top_min = gr.Slider(minimum=0, maximum=100, value=70, step=5, label="Min score")
            top_limit = gr.Number(value=10, precision=0, label="Limit")
            sort_dropdown = gr.Dropdown(choices=[
                ("Score (high → low)", "score_desc"),
                ("Score (low → high)", "score_asc"),
                ("Newest", "newest"),
                ("Oldest", "oldest")
            ], value="score_desc", label="Sort by")
            top_cards = gr.HTML(render_job_cards_clickable(70, 100, int(top_limit.value))) # type: ignore
            with gr.Row():
                detail_id = gr.Number(value=0, precision=0, label="Job ID (show details)")
                show_btn = gr.Button("Show Job")
                detail_out = gr.Textbox(label="Job Detail", lines= 10)
                
    def refresh_top_table(min_score, limit, sort_by):
        return top_jobs_table(min_score, limit, sort_by)
                
    refresh_btn.click(fn=lambda: (fetch_stats(), get_logs(), render_job_cards_clickable( int(top_min.value), 100, int(top_limit.value))), inputs=None, outputs=[stats_md, logs_area, top_cards])
    export_btn.click(fn=export_csv, inputs=None, outputs=None)
    
    top_min.change(fn=lambda s, l, so: render_job_cards_clickable(int(s), 100, int(l)), inputs=[top_min, top_limit, sort_dropdown], outputs=top_cards)
    top_limit.change(fn=lambda s, l, so: render_job_cards_clickable( int(s), 100, int(l)), inputs=[top_min, top_limit, sort_dropdown], outputs=top_cards)
    sort_dropdown.change(fn=lambda s, l, so: render_job_cards_clickable( int(top_min.value), 100, int(top_limit.value)), inputs=[top_min, top_limit, sort_dropdown], outputs=top_cards)
    
    show_btn.click(fn=show_job_detail, inputs=detail_id, outputs=detail_out)

def refresh_cards(min_score, limit, sort_by):
    return render_job_cards_clickable( int(min_score), 100, int(limit))

app = demo.app
db = JobDatabase()

@app.route("/health")
async def health(request):
    details = {
        "env": {
            "DB_HOST": os.getenv("HOST"),
            "DB_PORT": os.getenv("DB_PORT"),
            "DBNAME": os.getenv("DBNAME"),
        }
    }
   
    try:
        db_ok, db_details = await run_in_threadpool(db.get_stats)
        details["database"] = db_details
        status = 200 if db_ok else 500
        return JSONResponse({"ok": db_ok, "details": details}, status_code=status)
    
    except Exception as e:
        details["error"] = str(e) # type: ignore
        return JSONResponse({"ok": False, "details": details}, status_code=500)
        


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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    print(f"Starting Gradio on port {port}")
    demo.launch(server_name="0.0.0.0", server_port=port, auth=(GRADIO_USER, GRADIO_PASS), show_error=True)