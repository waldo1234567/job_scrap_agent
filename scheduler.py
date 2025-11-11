import os
import time
import json
import traceback
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import threading
from yilingsi_scraper import Job104Scraper
from job_agent import JobMatcherAgent, JobDatabase

SCHEDULE_CRON = {"hour" : 2, "minute" : 30}
JOB_ID = "daily_scrape_and_score"
LOCKFILE = "/tmp/job_scheduler.lock"
LOG_PATH = "scheduler.log"

JOBSTORE_DB = "sqlite:///apscheduler_jobs.sqlite"

SCORING_BATCH_SIZE=6
SCORING_MAX_BATCHES=10
SCRAPE_HEADLESS=True

CLEANER_MIN_SCORE = 40    
CLEANER_MAX_AGE_DAYS = 30      
CLEANER_ACTION = "archive"

if os.getenv("SCRAPE_HEADLESS") == "0":
    SCRAPE_HEADLESS = False
    
_log_lock = threading.Lock()

def scheduler_log(msg:str):
    with _log_lock:
        ts = datetime.now().isoformat(timespec="seconds")
        line = f"[{ts}] {msg}"
        print(line)
        try:
            with open(LOG_PATH, "a" , encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

def _acquire_lock() -> bool:
    try:
        if os.path.exists(LOCKFILE):
            age = time.time() - os.path.getmtime(LOCKFILE)
            
            if age > 60 * 60 * 6:
                scheduler_log("Stale lock detected, removing")
                os.remove(LOCKFILE)
            else:
                return False
            
        with open(LOCKFILE, "w") as f:
            f.write(str(os.getpid()))
        return True
    
    except Exception as e:
        scheduler_log(f"Lock acquire error: {e}")
        return False

def _release_lock():
    try:
        if os.path.exists(LOCKFILE):
            os.remove(LOCKFILE)
    except Exception as e:
        scheduler_log(f"Lock release error: {e}")
        
        
def upsert_jobs_into_db(db: 'JobDatabase', jobs: List[Dict]) -> Dict:
    conn = sqlite3.connect(db.db_name)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    inserted = 0
    skipped = 0
    inserted_ids=[]
    
    for job in jobs:
        url = job.get("url", "") or ""
        title = (job.get("title") or "").strip()
        company = (job.get("company") or "").strip()
        if url:
            cur.execute("SELECT id FROM jobs WHERE url = ? LIMIT 1", (url,))
            if cur.fetchone():
                skipped += 1
                continue
        cur.execute("SELECT id FROM jobs WHERE title = ? AND company = ? LIMIT 1", (title, company))
        if cur.fetchone():
            skipped += 1
            continue
        
        try:
            cur.execute("""
                INSERT INTO jobs (title, company, location, url, salary, description, date_posted, search_keyword, source, scraped_at, ai_score, ai_analysis, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, '', 'new')
            """, (
                job.get("title"), job.get("company"), job.get("location"),
                job.get("url"), job.get("salary"), job.get("description"),
                job.get("date_posted"), job.get("search_keyword"), job.get("source"),
                job.get("scraped_at")
            ))
            inserted += 1
            inserted_ids.append(cur.lastrowid)
        except Exception as e:
            scheduler_log(f"DB insert error for job {title[:40]}: {e}")
            continue

    conn.commit()
    conn.close()
    return {"inserted": inserted, "skipped": skipped, "inserted_ids": inserted_ids}


def run_scrape_and_score(keywords: List[str], user_profile: Dict,headless:bool = SCRAPE_HEADLESS):
    scheduler_log("Starting scheduled run")
    if not _acquire_lock():
        scheduler_log("Another run is in progress; exiting this invocation.")
        return {"status": "skipped", "reason": "already_running"}

    
    start_ts = time.time()
    scraper = None
    agent = None
    
    try:
        agent = JobMatcherAgent(user_profile=user_profile)
        
        scraper = Job104Scraper(headless=headless)
        scheduler_log(f"Scraper started (headless={headless})")

        scraped = scraper.scrape_jobs(keywords)
        scheduler_log(f"Scraped {len(scraped)} raw jobs")

        upsert_stats = upsert_jobs_into_db(agent.db, scraped)
        scheduler_log(f"DB upsert: inserted {upsert_stats['inserted']}, skipped {upsert_stats['skipped']}")

        scored_total = agent.process_all_jobs(batch_size=SCORING_BATCH_SIZE, max_batches=SCORING_MAX_BATCHES)
        scheduler_log(f"Scoring completed; total scored in this run: {scored_total}")

        duration = time.time() - start_ts
        scheduler_log(f"Scheduled run completed in {duration:.1f}s")
        
        return {"status": "ok", "inserted": upsert_stats['inserted'], "skipped": upsert_stats['skipped'], "scored": scored_total}
    
    except Exception as e:
        scheduler_log(f"Run failed with exception: {e}")
        scheduler_log(traceback.format_exc())
        
        return {"status": "error", "error": str(e)}
    
    finally:
        try:
            if scraper:
                scraper.close()
        except Exception:
            pass
        _release_lock()

def _parse_date_posted_to_date(s: str):
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    for fmt in ("%m/%d/%Y", "%m/%d"):
        try:
            dt = datetime.strptime(s, fmt)
            if fmt == "%m/%d":
                dt = dt.replace(year=datetime.now().year)
            return dt.date()
        except Exception:
            continue
    return None


def clean_database(db:'JobDatabase', min_score:int =CLEANER_MIN_SCORE, max_age_days:int = CLEANER_MAX_AGE_DAYS, action: str = CLEANER_ACTION) -> Dict[str,Any]:
    stats = {"checked": 0, "archived": 0, "deleted": 0, "skipped": 0}
    cutoff_date = datetime.now().date() - timedelta(days=int(max_age_days))
    conn  =sqlite3.connect(db.db_name)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    cur.execute("SELECT id, ai_score, date_posted, scraped_at, status FROM jobs WHERE status IS NULL OR status != 'archived'")
    rows = cur.fetchall()
    
    to_archive: List[int] = []
    to_delete: List[int] = []
    
    for r in rows:
        stats["checked"] += 1
        jid = r["id"]
        score = r["ai_score"] if r["ai_score"] is not None else 0
        date_posted = r.get("date_posted") or ""
        scraped_at = r.get("scraped_at") or ""
        
        low_score = (score < min_score)
        
        old = False
        
        parsed = _parse_date_posted_to_date(date_posted)
        if parsed:
            old = (parsed <= cutoff_date)
        else:
            try:
                sa = None
                if scraped_at:
                    sa = datetime.fromisoformat(scraped_at).date()
                if sa:
                    old = (sa <= cutoff_date)
            except Exception:
                old = False
        
        if low_score or old:
            if action == "archive":
                to_archive.append(jid)
            elif action == "delete":
                to_delete.append(jid)
            else:
                stats["skipped"] += 1
    try:
        if to_archive:
            now_iso = datetime.now().isoformat()
            try:
                cur.execute("ALTER TABLE jobs ADD COLUMN archived_at TEXT")
            except Exception:
                pass
            cur.executemany("INSERT INTO archived_jobs SELECT * FROM jobs WHERE id IN (?,?,...)", [(now_iso, i) for i in to_archive])
            stats["archived"] = len(to_archive)
        if to_delete:
            cur.executemany("DELETE FROM jobs WHERE id IN (?,?,...)", [(i,) for i in to_delete])
            stats["deleted"] = len(to_delete)
        
        conn.commit()
    finally:
        conn.close()
        
    return stats


def run_clean_database(db_name, min_score, max_age_days, action):
    db = JobDatabase(db_name)
    return clean_database(db, min_score, max_age_days, action)


class SchedulerManager:
    def __init__(self, agent:'JobMatcherAgent' , keywords:List[str], jobstore_db: str =JOBSTORE_DB):
        executors = {"default": ThreadPoolExecutor(5)}
        jobstores = {"default" : SQLAlchemyJobStore(jobstore_db)}
        self.scheduler = BackgroundScheduler(executors=executors, jobstores=jobstores)
        self.agent = agent
        self.user_profile=agent.user_profile
        self.keywords= keywords
        self._job = None
        self.scheduler.add_listener(self._event_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        
    def _event_listener(self, event):
        if event.exception:
            scheduler_log(f"Job error: {event.exception}")
        else:
            scheduler_log("Job executed successfully.")
            
    def start(self):
        if not self.scheduler.running:
            self.scheduler.start()
            
            self._job = self.scheduler.add_job(
                func=run_scrape_and_score,
                trigger='cron',
                id=JOB_ID,
                kwargs={"keywords": self.keywords, "user_profile": self.user_profile, "headless": SCRAPE_HEADLESS},
                replace_existing=True,
                max_instances=1,
                hour=SCHEDULE_CRON["hour"],
                minute=SCHEDULE_CRON["minute"],
            )
            scheduler_log("Scheduler started and job scheduled.")
            
            self._cleaner_job = self.scheduler.add_job(
                func=run_clean_database,
                trigger='cron',
                id=f"{JOB_ID}_db_cleaner",
                replace_existing=True,
                kwargs={"db_name": self.agent.db.db_name,"min_score": CLEANER_MIN_SCORE,"max_age_days": CLEANER_MAX_AGE_DAYS,"action": CLEANER_ACTION},
                max_instances=1,
                hour=3,
                minute=30
            )
            scheduler_log("DB cleaner scheduled daily at 03:30")
            return True
        return False

    def stop(self):
        if self.scheduler.running:
            try:
                self.scheduler.remove_job(JOB_ID)
            except Exception:
                pass
            self.scheduler.shutdown(wait=False)
            scheduler_log("Scheduler stopped.")
            return True
        return False
    
    def status(self):
        return {
            "running": self.scheduler.running,
            "job": (self._job.id if self._job else None),
            "next_run_time": str(self._job.next_run_time) if self._job else None
        }
        
if __name__ == "__main__":    
    user_profile = {
        "skills": ["Frontend Development","Backend Development","AI Agents","AI Engineering","Python","JavaScript","React","Machine Learning"],
        "preferences": {"job_type":"internship","location":["Remote","Taiwan"],"min_relevance":40}
    }
    
    agent = JobMatcherAgent(user_profile=user_profile)
    keywords =  ["AI工程師 實習", "前端工程師 實習", "後端工程師 實習", "機器學習 實習"]
    mgr = SchedulerManager(agent=agent, keywords=keywords)
    mgr.start()
    scheduler_log("SchedulerManager is running. Press Ctrl-C to exit.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        mgr.stop()
        scheduler_log("Exiting.")
        