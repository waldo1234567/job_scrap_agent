from datetime import datetime, timedelta
from jobdb import JobDatabase


def main():
    print("cleaning db ....")
    
    db = JobDatabase()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cutoff_date =  datetime.now() - timedelta(days=30)
    
    cursor.execute('''
        DELETE FROM jobs 
        WHERE created_at < %s 
        OR ai_score < 40
        RETURNING id
    ''', (cutoff_date,))
    

    deleted = cursor.fetchall()
    conn.commit()
    
    print(f"Deleted {len(deleted)} old low-quality jobs")
    
    db.return_connection(conn)
    db.close()
    
if __name__ == "__main__":
    main()