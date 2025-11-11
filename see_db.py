from unified_run import JobDatabase

db = JobDatabase()

# Get all new jobs
jobs = db.get_all_jobs(status='new', limit=50)

# Print job titles
for job in jobs:
    print(f"{job['title']} at {job['company']}")

# Get statistics
stats = db.get_stats()
print(stats)