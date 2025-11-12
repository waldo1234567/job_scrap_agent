import os
from job_agent import JobMatcherAgent
from jobdb import JobDatabase

def main():
    print("Starting AI job scorer...")
    print("="*60)
    
    api_key = os.getenv('GOOGLE_API_KEY')
    if not api_key:
        print("GOOGLE_API_KEY not set!")
        exit(1)
        
    user_profile = {
        "skills": [
            "Frontend Development",
            "Backend Development", 
            "AI Agents",
            "AI Engineering",
            "Python",
            "JavaScript",
            "React",
            "Machine Learning"
        ],
        "preferences": {
            "job_type": "internship",
            "location": ["Remote", "Taiwan"],
            "min_relevance": 40
        }
    }
    
    db = JobDatabase()
    scorer = JobMatcherAgent(user_profile=user_profile)
    scorer.db = db

    total_scored = scorer.process_all_jobs(batch_size=10, max_batches=20)
    
    print(f"\nScoring complete! Scored {total_scored} jobs")
    
    db.close()
    
if __name__ == "__main__":
    main()