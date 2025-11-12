import os
from yilingsi_scraper import Job104Scraper
from jobdb import JobDatabase

def main():
    print("Starting job scraper...")
    print("="*60)
    
    db = JobDatabase()
    
    all_jobs=[]
    
    print("\nScraping 104.com.tw...")
    try:
        scraper_104 = Job104Scraper(headless=True)
        job104_keywords = [
            "AI工程師 實習",
            "前端工程師 實習", 
            "後端工程師 實習",
            "機器學習 實習",
            "軟體工程師 實習"
        ]
        job104_jobs = scraper_104.scrape_jobs(job104_keywords, max_pages=2)
        all_jobs.extend(job104_jobs)
        scraper_104.close()
        print(f"104.com.tw: {len(job104_jobs)} jobs")
    except Exception as e:
        print(f"104 scraping failed: {e}")
        
    print(f"\nSaving {len(all_jobs)} jobs to Supabase...")
    new_jobs, duplicates = db.save_jobs(all_jobs)
    
    print(f"\n{'='*60}")
    print(f"Scraping complete!")
    print(f"New jobs: {new_jobs}")
    print(f"Duplicates: {duplicates}")
    print(f"{'='*60}")
    
    db.close()
    
if __name__ == "__main__":
    main()