import os
import json
from typing import List, Dict,Optional
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.agents import create_agent
from langchain.tools import tool
from langchain.agents.middleware import wrap_tool_call
from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
from unified_run import JobDatabase
from dotenv import load_dotenv
import sqlite3

load_dotenv()


@wrap_tool_call
def handle_tool_error(request, handler):
    """handle tool call errors gracefully."""
    try:
        return handler(request)
    except Exception as e:
        return ToolMessage(content=f"An error occurred while executing the tool: {str(e)}")


@tool
def get_unscored_jobs(limit: int = 10) -> str:
    """
    Run this function below to Get jobs from database that haven't been scored by AI yet.
    Returns a list of jobs with id, title, description (truncated), and tags.
    
    Args:
        limit: Maximum number of jobs to retrieve (default 10)
    """
    db = JobDatabase()
    all_jobs = db.get_all_jobs(status='new', limit=1000)
    unscored = [job for job in all_jobs if job['ai_score'] == 0][:limit]
    
    if not unscored:
        return json.dumps({"message": "No unscored jobs found", "jobs": []})
    
    formatted_jobs = []
    for job in unscored:
        tags = json.loads(job.get('tags', '[]')) if isinstance(job.get('tags'), str) else job.get('tags', '[]')
        formatted_jobs.append({
            "id": job['id'],
            "title": job['title'],
            "company": job['company'],
            "description": job.get('description', '')[:300], 
            "tags": tags[:5],
            "source": job['source']
        })
    return json.dumps({"count": len(formatted_jobs), "jobs": formatted_jobs}, ensure_ascii=False)   


@tool
def save_job_scores(job_scores: str) -> str:
    """
    Run this function to save AI scores and analysis for jobs to the database.
    
    Args:
        job_scores: JSON string containing list of objects with:
                   - id (int): job ID
                   - score (int): relevance score 0-100
                   - analysis (str): brief explanation (max 50 words)
    
    Returns:
        Confirmation message
    """
    db = JobDatabase()
    
    try:
        scores = json.loads(job_scores)
        
        conn = sqlite3.connect(db.db_name)
        cursor = conn.cursor()
        print("SAVING TO DB .............................")
        saved_count = 0
        for score_data in scores:
            print(score_data["id"], "===> score_data ids")
            cursor.execute('''
                UPDATE jobs 
                SET ai_score = ?, ai_analysis = ?
                WHERE id = ?
            ''', (score_data['score'], score_data['analysis'], score_data['id']))
            saved_count += 1
        
        conn.commit()
        conn.close()
        
        return json.dumps({
            "success": True,
            "message": f"Successfully saved scores for {saved_count} jobs",
            "count": saved_count
        })
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e)
        })
        

@tool
def get_top_jobs(min_score: int = 70, limit:int = 15) -> str:
    """
    Run this function to get top-scoring jobs from the database.
    
    Args:
        min_score: Minimum AI score (0-100), default 70
        limit: Maximum number of jobs to return, default 10
    
    Returns:
        JSON string with top jobs
    
    """
    
    db = JobDatabase()
    
    conn = sqlite3.connect(db.db_name)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM jobs 
        WHERE ai_score >= ?
        ORDER BY ai_score DESC, created_at DESC
        LIMIT ?
    ''', (min_score, limit))
    
    jobs = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    if not jobs:
        return json.dumps({"message": "No jobs found with the specified criteria", "jobs": []})
    
    formatted = []
    for job in jobs:
        formatted.append({
            "id": job['id'],
            "title": job['title'],
            "company": job['company'],
            "location": job['location'],
            "score": job['ai_score'],
            "analysis": job['ai_analysis'],
            "url": job['url'],
            "source": job['source']
        })
    
    return json.dumps({"count": len(formatted), "jobs": formatted}, ensure_ascii=False)
    
@tool
def get_database_stats()-> str:
    """
    Run this function to get statistics about jobs in the database.
    
    Returns:
        JSON with total jobs, scored jobs, average score, etc.
    """
    db = JobDatabase()
    
    all_jobs = db.get_all_jobs(status='new', limit=10000)
    scored_jobs = [j for j in all_jobs if j['ai_score'] > 0]
    
    stats = {
        "total_jobs": len(all_jobs),
        "scored_jobs": len(scored_jobs),
        "unscored_jobs": len(all_jobs) - len(scored_jobs),
        "average_score": round(sum(j['ai_score'] for j in scored_jobs) / len(scored_jobs), 2) if scored_jobs else 0,
        "high_quality_jobs": len([j for j in scored_jobs if j['ai_score'] >= 70]),
        "medium_quality_jobs": len([j for j in scored_jobs if 40 <= j['ai_score'] < 70]),
        "low_quality_jobs": len([j for j in scored_jobs if j['ai_score'] < 40])
    }
    
    return json.dumps(stats, indent=2)


class JobMatcherAgent:
    def __init__(self, user_profile : Dict):
        self.user_profile = user_profile
        self.db = JobDatabase()
        
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.0-flash",
            temperature = 0.2
        )
        
        self.tools=[get_unscored_jobs, save_job_scores, get_top_jobs, get_database_stats]
        
        
        self.agent = create_agent(self.llm, tools=self.tools, middleware=[handle_tool_error])
        
    
    def get_unscored_jobs(self, limit: int = 10) -> List[Dict]:
        """Get unscored jobs directly from DB"""
        all_jobs = self.db.get_all_jobs(status='new', limit=1000)
        unscored = [job for job in all_jobs if job['ai_score'] == 0][:limit]
        return unscored
    
    def score_jobs_batch(self, jobs: List[Dict]) -> List[Dict]:
        """
        Score a batch of jobs using LLM
        Returns: [{"id": 1, "score": 85, "analysis": "reason"}, ...]
        """
        
        if not jobs:
            return []
        
        jobs_for_llm = []
        for job in jobs:
            tags = json.loads(job.get('tags', '[]')) if isinstance(job.get('tags'), str) else job.get('tags', [])
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
            
            print(f"   ✅ LLM scored {len(scores)} jobs")
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
        conn = sqlite3.connect(self.db.db_name)
        cursor = conn.cursor()
        
        saved = 0
        for score_data in scores:
            try:
                cursor.execute('''
                    UPDATE jobs 
                    SET ai_score = ?, ai_analysis = ?
                    WHERE id = ?
                ''', (score_data['score'], score_data['analysis'], score_data['id']))
                saved += 1
            except Exception as e:
                print(f"Error saving job {score_data.get('id')}: {e}")
        
        conn.commit()
        conn.close()
        
        print(f"Saved {saved} scores to database")
        return saved
    
    def process_all_jobs(self, batch_size: int = 10, max_batches: int = 20):
        """
        Process all unscored jobs in batches
        This is the main method you'll use
        """
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
    
    def get_top_jobs(self, min_score: int = 70, limit: int = 10):
        """Get and display top jobs"""
        conn = sqlite3.connect(self.db.db_name)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM jobs 
            WHERE ai_score >= ?
            ORDER BY ai_score DESC, created_at DESC
            LIMIT ?
        ''', (min_score, limit))
        
        jobs = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        if not jobs:
            print(f"No jobs found with score >= {min_score}")
            return
        
        print(f"\n{'='*60}")
        print(f"TOP {len(jobs)} JOBS (score >= {min_score})")
        print(f"{'='*60}\n")
        
        for i, job in enumerate(jobs, 1):
            print(f"{i}. {job['title']}")
            print(f"{job['company']}")
            print(f"{job['location']}")
            print(f"Score: {job['ai_score']}/100")
            print(f"{job['ai_analysis']}")
            print(f"{job['url']}")
            print(f"Source: {job['source']}\n")
    

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