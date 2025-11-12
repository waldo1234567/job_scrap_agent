import os
import json
from typing import List, Dict
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.agents import create_agent
from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
from unified_run import JobDatabase
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()

class JobMatcherAgent:
    def __init__(self, user_profile : Dict):
        self.user_profile = user_profile
        self.db = JobDatabase()
        
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.0-flash",
            temperature = 0.2
        )
        
        self.tools=[]
        
        
        self.agent = create_agent(self.llm, tools=self.tools)
        
    def get_unscored_jobs(self, limit: int = 10) -> List[Dict]:
        """Get unscored jobs directly from DB"""
        conn = self.db.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('''
            SELECT * FROM jobs 
            WHERE status = %s AND ai_score = 0
            ORDER BY created_at DESC 
            LIMIT %s
        ''', ('new', limit))
        
        jobs = [dict(row) for row in cursor.fetchall()]
        self.db.return_connection(conn)
        
        print(f"Found {len(jobs)} unscored jobs")
        return jobs
    
    def score_jobs_batch(self, jobs: List[Dict]) -> List[Dict]:
        if not jobs:
            return []
        
        jobs_for_llm = []
        for job in jobs:
            tags = job.get('tags', [])
            if isinstance(tags, str):
                try:
                    tags = json.loads(tags)
                except json.JSONDecodeError:
                    tags = []
            
            jobs_for_llm.append({
                "id": job['id'],
                "title": job['title'],
                "company": job['company'],
                "description": job.get('description', '')[:250],  
                "tags": tags[:5] if tags else []
            })
        
        prompt = f"""Score these {len(jobs)} jobs for this candidate:

CANDIDATE SKILLS: {', '.join(self.user_profile['skills'])}
JOB TYPE: {self.user_profile['preferences']['job_type']}
LOCATION: {', '.join(self.user_profile['preferences']['location'])}

JOBS TO SCORE:
{json.dumps(jobs_for_llm, ensure_ascii=False, indent=2)}

SCORING SCALE:
- 90-100: Perfect match (key skills match, relevant tech role)
- 70-89: Strong match (most skills match)
- 50-69: Decent match (some skills match)
- 30-49: Weak match (few skills match)
- 0-29: Not relevant (retail, sales, non-tech)

RETURN ONLY THIS JSON (no markdown, no explanation):
[
  {{"id": 1, "score": 85, "analysis": "Strong Python/ML match"}},
  {{"id": 2, "score": 30, "analysis": "Retail job, not relevant"}}
]

Keep each analysis under 50 words. Return JSON for ALL {len(jobs)} jobs."""

        messages = [
            SystemMessage(content="You are a job scoring assistant. Return only valid JSON."),
            HumanMessage(content=prompt)
        ]
        response = self.llm.invoke(messages)
        response_text = response.content.strip() # type: ignore
        try:
            if response_text.startswith('```json'):
                response_text = response_text.split('```json')[1].split('```')[0].strip()
            elif response_text.startswith('```'):
                response_text = response_text.split('```')[1].split('```')[0].strip()
            
            scores = json.loads(response_text)
            
            print(f"LLM scored {len(scores)} jobs")
            return scores
            
        except json.JSONDecodeError as e:
            print(f"JSON parse error: {e}")
            print(f"   Response was: {response_text[:200]}") # type: ignore
            return []
        except Exception as e:
            print(f"LLM error: {e}")
            return []
    
    def save_scores_to_db(self, scores: List[Dict]) -> int:
        """Save scores directly to database"""
        saved = 0
        for score_data in scores:
            try:
                self.db.update_job_score(
                    job_id=score_data['id'],
                    score=score_data['score'],
                    analysis=score_data['analysis']
                )
                saved += 1
            except Exception as e:
                print(f"Error saving job {score_data.get('id')}: {e}")
        
        print(f"Saved {saved} scores to database")
        return saved
    
    def save_job_scores(self, scores: List[Dict]) -> int:
        return self.save_scores_to_db(scores)
    
    def get_top_jobs(self, min_score: int = 70, limit: int = 10) -> List[Dict]:
        """Get and display top jobs using PostgreSQL"""
        jobs = self.db.get_jobs_by_score(min_score=min_score, limit=limit)
        
        if not jobs:
            print(f"No jobs found with score >= {min_score}")
            return []
        
        print(f"\n{'='*60}")
        print(f"TOP {len(jobs)} JOBS (score >= {min_score})")
        print(f"{'='*60}\n")
        
        for i, job in enumerate(jobs, 1):
            print(f"{i}. {job['title']}")
            print(f"   {job['company']}")
            print(f"   {job['location']}")
            print(f"   Score: {job['ai_score']}/100")
            print(f"   {job['ai_analysis']}")
            print(f"   {job['url']}")
            print(f"   Source: {job['source']}\n")
        
        return jobs
    
    def get_database_stats(self) -> Dict:
        """Get database statistics"""
        return self.db.get_stats()
    
    
    def process_all_jobs(self, batch_size: int = 10, max_batches: int = 20):
        print(f"\n{'='*60}")
        print(f" Starting batch processing")
        print(f"   Batch size: {batch_size}")
        print(f"   Max batches: {max_batches}")
        print(f"{'='*60}\n")
        
        total_scored = 0
        
        for batch_num in range(1, max_batches + 1):
            print(f"Batch {batch_num}/{max_batches}")
            
            jobs = self.get_unscored_jobs(limit=batch_size)
            
            if not jobs:
                print(f"No more unscored jobs")
                break
            
            print(f"Found {len(jobs)} unscored jobs")
    
            scores = self.score_jobs_batch(jobs)
            
            if not scores:
                print(f" Failed to get scores, skipping batch")
                continue
        
            saved = self.save_scores_to_db(scores)
            total_scored += saved
            
            print(f"Batch complete\n")
        
        print(f"{'='*60}")
        print(f"Processing complete!")
        print(f"Total jobs scored: {total_scored}")
        print(f"{'='*60}\n")
    
        self.show_statistics()
        
        return total_scored
    
    def show_statistics(self):
        """Show database statistics"""
        all_jobs = self.db.get_all_jobs(status='new', limit=10000)
        scored = [j for j in all_jobs if j['ai_score'] > 0]
        
        if not scored:
            print("No scored jobs yet")
            return
        
        high = [j for j in scored if j['ai_score'] >= 70]
        medium = [j for j in scored if 40 <= j['ai_score'] < 70]
        low = [j for j in scored if j['ai_score'] < 40]
        
        print(" STATISTICS:")
        print(f"   Total scored: {len(scored)}")
        print(f"   High quality (70+): {len(high)} jobs")
        print(f"   Medium quality (40-69): {len(medium)} jobs")
        print(f"   Low quality (<40): {len(low)} jobs")
        
        if scored:
            avg = sum(j['ai_score'] for j in scored) / len(scored)
            print(f"   Average score: {avg:.1f}/100")
    

if __name__ == "__main__":
    print("Job Matcher Agent - Starting...")
    print("="*60)
   
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
    
    print("Initializing agent...")
    agent = JobMatcherAgent(user_profile=user_profile)
    
    print(" Agent ready!\n")
    print("="*60)
    print(" Analyzing jobs...")
    print("="*60 + "\n")
    
    result = agent.process_all_jobs(batch_size=10, max_batches=15)
    
    print("\n" + "="*60)
    print("ANALYSIS COMPLETE")
    print("="*60)
    print(result)
    
    print("\n" + "="*60)
    print("TOP RECOMMENDATIONS")
    print("="*60 + "\n")
    
    recommendations = agent.get_top_jobs(min_score=70, limit=10)
    print(recommendations)
    
    print("\nDone! Check jobs.db for all scored jobs.")