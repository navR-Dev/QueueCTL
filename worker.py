import subprocess # Lets the program execute shell commands
import time # Sleep and delay functionalities for retry backoff
from storage import update_job, move_to_dlq # Storage operations
from job import Job
import json # For reading config values

# Load backoff base from config file
with open("config.json") as f:
    CONFIG = json.load(f)

def process_job(job, base=None): # base controls exponential backoff
    base = base or CONFIG["default_backoff_base"] # Use CLI or config default

    while job.attempts < job.max_retries: # Retry loop
        if job.state != "pending":
            job.state = "pending" # Reset state so this worker continues retrying
            update_job(job)

        job.state = "processing" # Claim job
        update_job(job)
        print(f"Processing job {job.id}: {job.command}")

        try:
            # Run command safely with output captured
            result = subprocess.run(
                job.command,
                shell=True,
                capture_output=True,
                text=True
            )

            if result.returncode == 0:
                job.state = "completed" # Success
                update_job(job)
                print(f"Job {job.id} completed successfully")
                return
            else:
                raise subprocess.SubprocessError(
                    f"Command returned exit code {result.returncode}"
                )

        except Exception as e:
            job.attempts += 1
            job.state = "failed" # Mark failed attempt
            update_job(job)
            print(f"Job {job.id} failed: {str(e)}")

            if job.attempts >= job.max_retries:
                job.state = "dead" # Move to DLQ after exceeding retries
                move_to_dlq(job)
                print(f"Job {job.id} moved to DLQ after {job.max_retries} failed attempts")
                return

            delay = base ** job.attempts # Exponential backoff
            print(f"Retrying job {job.id} in {delay} seconds (attempt {job.attempts}/{job.max_retries})")
            time.sleep(delay)

    # Safety fallback (should never reach here)
    job.state = "dead"
    move_to_dlq(job)
    print(f"Job {job.id} moved to DLQ after retry loop ended unexpectedly")
