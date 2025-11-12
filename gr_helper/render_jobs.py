import sqlite3
import json
from typing import List
from jobdb import JobDatabase

db = JobDatabase()

def _fetch_jobs( min_score: int, max_score: int, limit: int):
    conn = db.get_connection()
    cur = conn.cursor()
    query = """
        SELECT id, title, company, location, ai_score, ai_analysis, url, date_posted, source
        FROM jobs
        WHERE ai_score BETWEEN %s AND %s
    """
    params = [min_score, max_score]
    
    query += " ORDER BY ai_score DESC, created_at DESC LIMIT %s"
    params.append(int(limit))
    cur.execute(query, params)
  
    columns = [desc[0] for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    
    db.return_connection(conn)
    return rows

def get_job_by_id( job_id: int):
    conn = db.get_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT * FROM jobs WHERE id = %s", (int(job_id),))
    row = cur.fetchone()
    
    if not row:
        db.return_connection(conn)
        return {}
 
    columns = [desc[0] for desc in cur.description]
    job_dict = dict(zip(columns, row))
    
    db.return_connection(conn)
    return job_dict

def render_job_cards_clickable( min_score: int, max_score: int, limit: int = 8) -> str:
    rows = _fetch_jobs(min_score, max_score, limit)
    if not rows:
        return "<div>No jobs found for this range.</div>"
    
    css = """
    <style>
      .job-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; }
      .job-card { border: 1px solid #e6e6e6; border-radius: 8px; padding: 12px; box-shadow: 0 2px 6px rgba(0,0,0,0.03); cursor: pointer; }
      .job-card:hover { transform: translateY(-3px); transition: .12s ease; }
      .job-title { font-size: 16px; font-weight: 600; margin-bottom: 6px; }
      .job-meta { font-size: 13px; color: #555; margin-bottom: 8px; }
      .score-badge { display:inline-block; padding:4px 8px; border-radius:12px; background:#0b7285; color:white; font-weight:600; font-size:13px; }
      .score-badge.med { background:#f59f00; }
      .job-analysis { font-size:13px; color:#222; margin-top:8px; }
      .job-footer { margin-top:10px; font-size:12px; color:#666; display:flex; justify-content:space-between; align-items:center; }
      .job-url { font-size:12px; color:#0b7285; text-decoration:none; }
      .job-id { font-size:12px; color:#888; }
    </style>
    """

    payload_map = {}
    cards_html: List[str] = ['<div class="job-grid">']

    for j in rows:
        jid = j['id']
        payload_map[jid] = {
            "id": j.get("id"),
            "title": j.get("title"),
            "company": j.get("company"),
            "description": j.get("description"),
            "location": j.get("location"),
            "score": j.get("ai_score"),
            "analysis": j.get("ai_analysis"),
            "url": j.get("url"),
            "date_posted": j.get("date_posted")
        }
        badge_class = "score-badge" if (j.get('ai_score') or 0) >= 70 else "score-badge med"
        analysis_short = (j.get('ai_analysis') or "")[:180]
        url_html = f'<a href="{j.get("url","")}" target="_blank" class="job-url">Apply</a>' if j.get("url") else ""
        loc = j.get("location", "") or ""
        cards_html.append(f"""
        <div class="job-card" data-jobid="{jid}" onclick="window._showJob({jid})">
          <div class="job-title">{(j.get('title') or '')}</div>
          <div class="job-meta">{(j.get('company') or '')} • {loc}</div>
          <div><span class="{badge_class}">{(j.get('ai_score') or 0)}</span></div>
          <div class="job-analysis">{analysis_short}</div>
          <div class="job-footer">
            <div class="job-id">ID: {jid}</div>
            <div>{url_html}</div>
          </div>
        </div>
        """)

    cards_html.append('</div>')

    js = f"""
    <script>
    (function() {{
      window._jobPayloads = {json.dumps(payload_map, ensure_ascii=False)};

      function _showJobImpl(id) {{
          try {{
              const payload = window._jobPayloads[id];
              const pretty = JSON.stringify(payload, null, 2);
              // find best textarea: placeholder "Job detail", fallback to first textarea
              const tb = document.querySelector('textarea[placeholder="Job detail"]') || document.querySelector('textarea');
              if (tb) {{
                  tb.value = pretty;
                  tb.dispatchEvent(new Event('input', {{ bubbles: true }}));
              }} else {{
                  // If Gradio changed structure, attempt to find a gradio output container
                  const pre = document.querySelector('#job-detail-pre') || null;
                  if (pre) {{
                      pre.innerText = pretty;
                  }} else {{
                      alert(pretty);
                  }}
              }}
          }} catch (e) {{
              console.warn("showJob error:", e);
          }}
      }}
      window._showJob = _showJobImpl;

      function attachCardHandlers(scope) {{
          const root = scope || document;
          const cards = root.querySelectorAll('.job-card');
          cards.forEach(card => {{
              // avoid re-attaching
              if (card.__jobHandlerAttached) return;
              card.__jobHandlerAttached = true;
              card.addEventListener('click', function (ev) {{
                  const id = this.dataset.jobid || this.getAttribute('data-jobid');
                  if (id !== null && id !== undefined) {{
                      _showJobImpl(id);
                  }}
              }});
          }});
      }}
      attachCardHandlers();

      const observer = new MutationObserver((mutations) => {{
          attachCardHandlers();
      }});
      observer.observe(document.body, {{ childList: true, subtree: true }});

      setTimeout(attachCardHandlers, 250);
      setTimeout(attachCardHandlers, 1500);

    }})();
    </script>
    """

    return css + "\n".join(cards_html) + js

def render_job_cards( min_score: int, max_score: int, limit: int = 8) -> str:
    rows = _fetch_jobs( min_score, max_score, limit)
    if not rows:
        return "<div>No jobs found for this range.</div>"

    css = """
    <style>
      .job-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; }
      .job-card { border: 1px solid #e6e6e6; border-radius: 8px; padding: 12px; box-shadow: 0 2px 6px rgba(0,0,0,0.03); }
      .job-title { font-size: 16px; font-weight: 600; margin-bottom: 6px; }
      .job-meta { font-size: 13px; color: #555; margin-bottom: 8px; }
      .score-badge { display:inline-block; padding:4px 8px; border-radius:12px; background:#0b7285; color:white; font-weight:600; font-size:13px; }
      .score-badge.med { background:#f59f00; }
      .job-analysis { font-size:13px; color:#222; margin-top:8px; }
    </style>
    """

    cards = ['<div class="job-grid">']
    for j in rows:
        badge_class = "score-badge" if (j.get('ai_score') or 0) >= 70 else "score-badge med"
        analysis_short = (j.get('ai_analysis') or "")[:180]
        url_html = f'<a href="{j.get("url","")}" target="_blank">Apply</a>' if j.get("url") else ""
        loc = j.get("location", "") or ""
        cards.append(f"""
        <div class="job-card">
          <div class="job-title">{(j.get('title') or '')}</div>
          <div class="job-meta">{(j.get('company') or '')} • {loc}</div>
          <div><span class="{badge_class}">{(j.get('ai_score') or 0)}</span></div>
          <div class="job-analysis">{analysis_short}</div>
          <div style="margin-top:10px;font-size:12px;color:#666">{url_html} <span style="float:right">ID: {j.get('id')}</span></div>
        </div>
        """)
    cards.append('</div>')
    return css + "\n".join(cards)