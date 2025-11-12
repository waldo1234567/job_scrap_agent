import json
from jobdb import JobDatabase
from yilingsi_scraper import Job104Scraper

# class JobDatabase:
#     def __init__(self, db_name="jobs.db"):
#         self.db_name=db_name
#         self.setup_database()
        
#     def setup_database(self):
#         conn = sqlite3.connect(self.db_name)
#         cursor = conn.cursor()
        
#         cursor.execute(
#             '''
#                 CREATE TABLE IF NOT EXISTS jobs (
#                 id INTEGER PRIMARY KEY AUTOINCREMENT,
#                 job_hash TEXT UNIQUE,
#                 title TEXT NOT NULL,
#                 company TEXT,
#                 location TEXT,
#                 url TEXT,
#                 salary TEXT,
#                 description TEXT,
#                 date_posted TEXT,
#                 tags TEXT,
#                 source TEXT,
#                 search_keyword TEXT,
#                 scraped_at TEXT,
#                 status TEXT DEFAULT 'new',
#                 ai_score INTEGER DEFAULT 0,
#                 ai_analysis TEXT,
#                 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#                 )
#             '''
#         )
#         conn.commit()
#         conn.close()
#         print("DB Initialized")
    
#     def generate_job_hash(self, job):
#         unique_string = f"{job['title']}{job['company']}{job['url']}"
#         return hashlib.md5(unique_string.encode()).hexdigest()
    
#     def save_jobs(self, jobs):
#         conn = sqlite3.connect(self.db_name)
#         cursor = conn.cursor()
        
#         new_jobs = 0
#         duplicate_jobs = 0
        
#         for job in jobs:
#             job_hash = self.generate_job_hash(job)
            
#             try:
#                 cursor.execute(
#                     '''
#                         INSERT INTO jobs (
#                         job_hash, title, company, location, url, 
#                         salary, description, date_posted, tags, 
#                         source, search_keyword, scraped_at
#                     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#                     ''',(
#                         job_hash,
#                         job['title'],
#                         job.get('company', 'Unknown'),
#                         job.get('location', 'Remote'),
#                         job['url'],
#                         job.get('salary', 'Not specified'),
#                         job.get('description', ''),
#                         job.get('date_posted', 'Unknown'),
#                         json.dumps(job.get('tags', [])),
#                         job['source'],
#                         job.get('search_keyword', ''),
#                         job['scraped_at']
#                     )
#                 )
#                 new_jobs += 1
#             except sqlite3.IntegrityError:
#                 duplicate_jobs += 1
#                 continue
            
#         conn.commit()
#         conn.close()
        
#         print(f"Saved {new_jobs} new jobs, skipped {duplicate_jobs} duplicates")
#         return new_jobs, duplicate_jobs
    
#     def get_all_jobs(self, status='new', limit = 100):
#         conn = sqlite3.connect(self.db_name)
#         conn.row_factory = sqlite3.Row
#         cursor = conn.cursor()
        
#         cursor.execute('''
#            SELECT * FROM jobs 
#             WHERE status = ? 
#             ORDER BY created_at DESC 
#             LIMIT ?            
#         ''',(status, limit))
        
#         jobs = [dict(row) for row in cursor.fetchall()]
#         conn.close()
        
#         return jobs
    
#     def update_job_status(self, job_id, status):
#         conn = sqlite3.connect(self.db_name)
#         cursor = conn.cursor()
        
#         cursor.execute('''
#             UPDATE jobs SET status = ? WHERE id = ?
#         ''', (status, job_id))
        
#         conn.commit()
#         conn.close()
    
#     def get_stats(self):
#         conn = sqlite3.connect(self.db_name)
#         cursor = conn.cursor()
        
#         cursor.execute('SELECT COUNT(*) FROM jobs')
#         total = cursor.fetchone()[0]
        
#         cursor.execute('SELECT COUNT(*) FROM jobs WHERE status = "new"')
#         new = cursor.fetchone()[0]
        
#         cursor.execute('SELECT COUNT(*) FROM jobs WHERE status = "interested"')
#         interested = cursor.fetchone()[0]
        
#         cursor.execute('SELECT COUNT(*) FROM jobs WHERE status = "applied"')
#         applied = cursor.fetchone()[0]
        
#         conn.close()
        
#         return {
#             'total': total,
#             'new': new,
#             'interested': interested,
#             'applied': applied
#         }

class UnifiedJobScrapper:
    def __init__(self):
        self.db = JobDatabase()
        self.job104 = Job104Scraper()
        
    def scrape_all(self, include_104=True):
        all_jobs = []
        
        if include_104:
            print("\n" + "="*50)
            print("Scraping from 104...")
            print("="*50)
            
            try:
                self.job104 = Job104Scraper(headless=True)
                
                job104_keywords = [
                    "AI工程師 實習",
                    "前端工程師 實習", 
                    "後端工程師 實習",
                    "機器學習 實習",
                    "軟體工程師 實習"
                ]
                
                job104_jobs = self.job104.scrape_jobs(job104_keywords, max_pages=4)
                all_jobs.extend(job104_jobs)
            except Exception as e:
                print(f"error scraping 104 : {e}")
            
            finally:
                if self.job104:
                    self.job104.close()
        
        print("\n" + "="*50)
        print("Saving to database...")
        print("="*50)
        
        new_jobs, duplicates = self.db.save_jobs(all_jobs)
        
        stats = self.db.get_stats()
        print(f"\n Database Statistics:")
        print(f"Total jobs: {stats['total']}")
        print(f"New: {stats['new']}")
        print(f"Interested: {stats['interested']}")
        print(f"Applied: {stats['applied']}")
        
        return all_jobs
    
    
    def export_to_json(self, filename="all_jobs.json"):
        jobs = self.db.get_all_jobs(status='new', limit=1000)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(jobs, f, indent=2, ensure_ascii=False)
        
        print(f"\n Exported {len(jobs)} jobs to {filename}")
        

if __name__ == "__main__":
    print("Job Finder Agent - Starting...")
    print("="*50)
    
    scraper = UnifiedJobScrapper()
    
    jobs = scraper.scrape_all(include_104=True)
    
    scraper.export_to_json()
    
    print("\n" + "="*50)
    print("Preview of latest jobs:")
    print("="*50)
    
    latest_jobs = scraper.db.get_all_jobs(status='new', limit=5)
    
    for i, job in enumerate(latest_jobs, 1):
        print(f"\n{i}. {job['title']}")
        print(f"{job['company']}")
        print(f"{job['location']}")
        print(f"{job['salary']}")
        print(f"{job['url']}")
        print(f"Source: {job['source']}")
    
    print("\n✅ Scraping complete!")
    print(f"💡 Check 'jobs.db' for full database")
    print(f"💡 Check 'all_jobs.json' for JSON export")