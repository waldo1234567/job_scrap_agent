import os
import json
import hashlib
from typing import List, Dict, Optional, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv

load_dotenv()

username = os.environ.get("DB_USERNAME")
password = os.environ.get("PASSWORD")
host = os.environ.get("HOST")
port = os.environ.get("DB_PORT")
dbname = os.environ.get("DBNAME")

class JobDatabase:
    def __init__(self, database_url:Optional[str] = None):
        self.pool=SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            dbname=dbname,
            user=username,
            password=password,
            host=host,
            port=port,
            connect_timeout= 3,
            sslmode="require"
        )
        
        print("Connected to Supabase")
    
    def get_connection(self):
        if self.pool:
            return self.pool.getconn()
        return psycopg2.connect(
            dbname=dbname,
            user=username,
            password=password,
            host=host,
            port=port,
            connect_timeout=3,
            sslmode="require"
        )
    
    def return_connection(self,conn):
        if not conn:
            return
        if self.pool:
            try:
                self.pool.putconn(conn)
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
        else:
            try:
                conn.close()
            except Exception:
                pass
    
    def generate_job_hash(self, job:Dict):
        unique_string = f"{job['title']}{job['company']}{job['url']}"
        return hashlib.md5(unique_string.encode()).hexdigest()
    
    def save_jobs(self, jobs:List[Dict]) -> tuple:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        new_jobs = 0
        duplicate_jobs = 0
        
        for i, job in enumerate(jobs, 1):
            job_hash = self.generate_job_hash(job)
            
            try:
                tags_json = json.dumps(job.get('tags', []))

                cursor.execute('''
                    INSERT INTO jobs (
                        job_hash, title, company, location, url, 
                        salary, description, date_posted, tags, 
                        source, search_keyword, scraped_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
                    ON CONFLICT (job_hash) DO NOTHING
                ''', (
                    job_hash,
                    job['title'],
                    job.get('company', 'Unknown'),
                    job.get('location', 'Remote'),
                    job['url'],
                    job.get('salary', 'Not specified'),
                    job.get('description', ''),
                    job.get('date_posted', 'Unknown'),
                    tags_json,
                    job['source'],
                    job.get('search_keyword', ''),
                    job['scraped_at']
                ))
                if cursor.rowcount > 0:
                    new_jobs += 1
                    print(f"Job {i}/{len(jobs)}: {job['title'][:50]}...")
                else:
                    duplicate_jobs += 1
                    print(f"Job {i}/{len(jobs)}: Duplicate - {job['title'][:50]}...")
                    
            except Exception as e:
                print(f"Job {i}/{len(jobs)}: Error - {job['title'][:50]}... - {e}")
        
        try:
            conn.commit()
            print(f"Saved {new_jobs} new jobs, skipped {duplicate_jobs} duplicates")
        except Exception as e:
            print(f"Commit failed: {e}")
            conn.rollback()
            new_jobs = 0
    
        self.return_connection(conn)
        return new_jobs, duplicate_jobs

    def get_all_jobs(self, status: str = 'new', limit: int = 100) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute('''
            SELECT * FROM jobs 
            WHERE status = %s 
            ORDER BY created_at DESC 
            LIMIT %s
        ''', (status, limit))
        
        jobs = [dict(row) for row in cursor.fetchall()]
        self.return_connection(conn)
        
        return jobs
    
    def update_job_score(self, job_id: int, score: int, analysis: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE jobs 
            SET ai_score = %s, ai_analysis = %s
            WHERE id = %s
        ''', (score, analysis, job_id))
        
        conn.commit()
        self.return_connection(conn)
    
    def update_job_status(self, job_id: int, status: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE jobs SET status = %s WHERE id = %s
        ''', (status, job_id))
        
        conn.commit()
        self.return_connection(conn)
    
    def get_jobs_by_score(self, min_score: int = 70, limit: int = 50) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute('''
            SELECT * FROM jobs 
            WHERE ai_score >= %s
            ORDER BY ai_score DESC, created_at DESC
            LIMIT %s
        ''', (min_score, limit))
        
        jobs = [dict(row) for row in cursor.fetchall()]
        self.return_connection(conn)
        
        return jobs
    
    def get_stats(self) -> Dict:
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM jobs')
        total = cursor.fetchone()[0] # type: ignore
        
        cursor.execute('SELECT COUNT(*) FROM jobs WHERE status = %s', ('new',))
        new = cursor.fetchone()[0] # type: ignore
        
        cursor.execute('SELECT COUNT(*) FROM jobs WHERE status = %s', ('interested',))
        interested = cursor.fetchone()[0] # type: ignore
        
        cursor.execute('SELECT COUNT(*) FROM jobs WHERE status = %s', ('applied',))
        applied = cursor.fetchone()[0] # type: ignore
        
        self.return_connection(conn)
        
        return {
            'total': total,
            'new': new,
            'interested': interested,
            'applied': applied
        }
    
    def check(self) -> Tuple[bool, Dict]:
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
            self.pool.putconn(conn)
            return True, {"db": "ok"}
        except Exception as e:
            return False, {"db_error": str(e)}
            
    def close(self):
        if getattr(self, "pool", None):
            try:
                self.pool.closeall()
            except Exception as e:
                print("Error closing pool:", e)
        
def get_database(database_url: Optional[str] = None):
    db_url = database_url or os.getenv('DATABASE_URL')
    
    if db_url and db_url.startswith('postgresql://'):
        print("Using PostgreSQL (Supabase)")
        return JobDatabase(database_url=db_url)
    else:
        print("Using SQLite (local)")
        from unified_run import JobDatabase as SQLiteDB
        return SQLiteDB()
    
if __name__ == "__main__":
    db = JobDatabase()
    
    print("\nTesting database connection...")
    stats = db.get_stats()
    print(f"Connection successful!")
    print(f"Total jobs: {stats['total']}")
    
    db.close()