import gradio as gr
import threading, time, json, sqlite3, io
import pandas as pd
import threading
from typing import List, Dict
from job_agent import JobMatcherAgent
from datetime import datetime, timedelta
from scheduler import SchedulerManager, run_scrape_and_score, JOB_ID, scheduler_log
from gr_helper.render_jobs import render_job_cards_clickable, render_job_cards,get_job_by_id

SCHEDULE_KEYWORDS = [
    "AI工程師 實習", "前端工程師 實習", "後端工程師 實習", "機器學習 實習"
]

user_profile = {
    "skills": [
        "Frontend Development","Backend Development","AI Agents",
        "AI Engineering","Python","JavaScript","React","Machine Learning"
    ],
    "preferences": {"job_type":"internship","location":["Remote","Taiwan"],"min_relevance":40}
}
agent = JobMatcherAgent(user_profile=user_profile)
mgr = SchedulerManager(agent, SCHEDULE_KEYWORDS)

_logs: List[str] = []
_running_thread = None
_lock = threading.Lock()
db_path = agent.db.db_name

def _append_log(msg: str):
    with _lock:
        _logs.append(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")
        
def fetch_stats() -> str:
    all_jobs = agent.db.get_all_jobs(status='new', limit=1000)
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
                
                
def top_jobs_table(min_score: int = 70, limit: int = 10, sort_by: str = "score_desc"):
    conn = sqlite3.connect(agent.db.db_name)
    conn.row_factory = sqlite3.Row
    
    df = pd.read_sql_query(
        "SELECT id, title, company, location, ai_score, ai_analysis, url, date_posted FROM jobs WHERE ai_score >= ?",
        conn, params=(min_score,)
    )
    
    conn.close()
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
    
    return out

def export_csv():
    conn = sqlite3.connect(agent.db.db_name)
    df = pd.read_sql_query("SELECT * FROM jobs", conn)
    conn.close()
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    return("all_jobs.csv", buffer.getvalue())


def _process_run(batch_size: int, max_batches: int):
    try:
        _append_log(f"Started run : batch_size={batch_size}, max_batches={max_batches}")
        total = agent.process_all_jobs(batch_size, max_batches)
        _append_log(f"Finished run: scored {total} jobs")
    except Exception as e:
        _append_log(f"Run failed: {e}")
        
def run_now(batch_size: int = 6 , max_batches:int = 20):
    global _running_thread
    if _running_thread and _running_thread.is_alive():
        return "Already Running"

    _append_log("Scheduling run (background)...")
    _running_thread = threading.Thread(target=_process_run, args=(batch_size, max_batches), daemon=True)
    _running_thread.start()
    return "Started"

def get_logs() -> str:
    with _lock :
        return "\n".join(_logs[-500:]) or "No logs yet."
    
def show_job_detail(job_id:int):
    conn = sqlite3.connect(agent.db.db_name)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
    row = cur.fetchone()
    conn.close()
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

def start_scheduler_ui():
    started = mgr.start()
    if started:
        return f"Scheduler started. Next run: {mgr._job.next_run_time}" # type: ignore
    else:
        return "Scheduler already running."

def stop_scheduler_ui():
    stopped = mgr.stop()
    return "Scheduler stopped." if stopped else "Scheduler not running."

def scheduler_status_ui():
    st = mgr.status()
    return json.dumps(st, indent=2, default=str)

def schedule_one_off_now_ui(headless:bool = True):
    def _run():
        scheduler_log("One-off run (UI) started")
        res = run_scrape_and_score(keywords=SCHEDULE_KEYWORDS, user_profile=agent.user_profile, headless=headless)
        scheduler_log(f"One-off run (UI) finished: {res}")

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return "One-off run scheduled (background). Check logs for progress."


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
            with gr.Row():
                start_sched_btn = gr.Button("Start Scheduler")
                stop_sched_btn = gr.Button("Stop Scheduler")
                sched_status_btn = gr.Button("Scheduler Status")
                oneoff_btn = gr.Button("Run Scheduler Once")
                sched_status_out = gr.Textbox(label="Scheduler status", lines=4, value=scheduler_status_ui())
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
            top_cards = gr.HTML(render_job_cards_clickable(db_path, 70, 100, int(top_limit.value))) # type: ignore
            with gr.Row():
                detail_id = gr.Number(value=0, precision=0, label="Job ID (show details)")
                show_btn = gr.Button("Show Job")
                detail_out = gr.Textbox(label="Job Detail", lines= 10)
                
    def refresh_top_table(min_score, limit, sort_by):
        return top_jobs_table(min_score, limit, sort_by)
                
    run_btn.click(fn=run_now, inputs=[batch_slider, max_batches], outputs=logs_area)
    refresh_btn.click(fn=lambda: (fetch_stats(), get_logs(), render_job_cards_clickable(db_path, int(top_min.value), 100, int(top_limit.value))), inputs=None, outputs=[stats_md, logs_area, top_cards])
    export_btn.click(fn=export_csv, inputs=None, outputs=None)
    
    top_min.change(fn=lambda s, l, so: render_job_cards_clickable(db_path, int(s), 100, int(l)), inputs=[top_min, top_limit, sort_dropdown], outputs=top_cards)
    top_limit.change(fn=lambda s, l, so: render_job_cards_clickable(db_path, int(s), 100, int(l)), inputs=[top_min, top_limit, sort_dropdown], outputs=top_cards)
    sort_dropdown.change(fn=lambda s, l, so: render_job_cards_clickable(db_path, int(top_min.value), 100, int(top_limit.value)), inputs=[top_min, top_limit, sort_dropdown], outputs=top_cards)
    
    start_sched_btn.click(fn=start_scheduler_ui, inputs=None, outputs=sched_status_out)
    stop_sched_btn.click(fn=stop_scheduler_ui, inputs=None, outputs=sched_status_out)
    sched_status_btn.click(fn=scheduler_status_ui, inputs=None, outputs=sched_status_out)
    oneoff_btn.click(fn=schedule_one_off_now_ui, inputs=None, outputs=logs_area)
    show_btn.click(fn=show_job_detail, inputs=detail_id, outputs=detail_out)

def refresh_cards(min_score, limit, sort_by):
    return render_job_cards_clickable(db_path, int(min_score), 100, int(limit))



if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860, share=False)