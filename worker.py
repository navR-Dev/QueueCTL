import subprocess # Lets the program execute shell commands
import time # Sleep and delay functionalities for retry backoff
from storage import update_job, move_to_dlq 
from job import Job

def process_job(job, base=2): # base is used as the base for the exponential backoff (base^exponent)
    while job.attempts < job.max_retries: # Use loop instead of recursion
        job.state = "processing" 
        update_job(job)
        print(f"Processing job {job.id}: {job.command}") # Simple console log

        result = subprocess.run(job.command, shell=True) # Run the shell command with full shell syntax
        if result.returncode == 0:
            job.state = "completed"
            update_job(job)
            print(f"Job {job.id} completed successfully")
            return

        # If the job fails
        job.attempts += 1
        job.state = "failed"
        update_job(job)
        delay = base ** job.attempts # Calculate backoff delay
        print(f"Job {job.id} failed. Retrying in {delay} seconds (attempt {job.attempts}/{job.max_retries})")
        time.sleep(delay)

    # Move job to DLQ after exceeding max retries
    job.state = "dead"
    move_to_dlq(job)
    print(f"Job {job.id} moved to DLQ after {job.max_retries} failed attempts")
