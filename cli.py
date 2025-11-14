import click # CLI library
from job import Job # Job model
from storage import add_job, get_jobs_by_state # Job storage operations
from worker import process_job # Worker execution logic
from dlq import list_dlq, retry_from_dlq # DLQ operations
import json # To load configuration
from multiprocessing import Pool # For running multiple workers

# Load config
with open("config.json") as f:
    CONFIG = json.load(f)

@click.group() # Top-level CLI group: queuectl
def cli():
    pass

# ----------------------------
# ENQUEUE
# ----------------------------
@cli.command() # queuectl enqueue
@click.argument("input_str")
def enqueue(input_str):
    # Detect JSON input if it starts with '{'
    if input_str.strip().startswith("{"):
        try:
            data = json.loads(input_str) # Parse JSON
        except json.JSONDecodeError:
            click.echo("Invalid JSON format for enqueue.")
            return

        # Extract fields with fallbacks
        custom_id = data.get("id", None) # Optional manual ID
        command = data.get("command", None) # Command required
        max_retries = data.get("max_retries", CONFIG["default_max_retries"])

        if not command:
            click.echo("JSON must include a 'command' field.")
            return

        # Create job
        job = Job(command, max_retries=max_retries)

        # Apply custom ID if provided
        if custom_id:
            job.id = custom_id

        add_job(job)
        click.echo(f"Enqueued job {job.id} ({job.command}) [JSON mode]")
        return

    # Otherwise treat as normal shell command
    job = Job(input_str, CONFIG["default_max_retries"]) # Use config default retries
    add_job(job)
    click.echo(f"Enqueued job {job.id} ({input_str}) [shell mode]")

# ----------------------------
# WORKER GROUP
# ----------------------------
@cli.group() # queuectl worker
def worker():
    pass

@worker.command(name="start") # queuectl worker start
@click.option("--count", default=1, help="Number of workers")
@click.option("--base", default=None, help="Base for exponential backoff")
def worker_start(count, base):
    jobs = get_jobs_by_state("pending")
    if not jobs:
        click.echo("No pending jobs to process.")
        return
    click.echo(f"Starting {count} worker(s) to process {len(jobs)} job(s)...")
    jobs = [Job.from_dict(j) for j in jobs]

    if count == 1:
        for job in jobs:
            process_job(job, base)
    else:
        with Pool(count) as pool: # Parallel workers
            pool.starmap(process_job, [(job, base) for job in jobs])

@worker.command(name="stop") # queuectl worker stop
def worker_stop():
    click.echo("Graceful worker shutdown not implemented in this simplified version.")

# ----------------------------
# STATUS
# ----------------------------
@cli.command() # queuectl status
def status():
    pending = len(get_jobs_by_state("pending"))
    processing = len(get_jobs_by_state("processing"))
    failed = len(get_jobs_by_state("failed"))
    completed = len(get_jobs_by_state("completed"))
    dead = len(get_jobs_by_state("dead"))
    click.echo("Queue Status:")
    click.echo(f"  Pending: {pending}")
    click.echo(f"  Processing: {processing}")
    click.echo(f"  Failed: {failed}")
    click.echo(f"  Completed: {completed}")
    click.echo(f"  Dead (DLQ): {dead}")

# ----------------------------
# LIST JOBS
# ----------------------------
@cli.command() # queuectl list
@click.option("--state", default="pending", help="Filter by state")
def list_jobs(state):
    jobs = get_jobs_by_state(state)
    if not jobs:
        click.echo(f"No jobs found with state '{state}'")
        return
    for j in jobs:
        click.echo(f"{j['id']} | {j['command']} | {j['state']}")

# ----------------------------
# DLQ GROUP
# ----------------------------
@cli.group() # queuectl dlq
def dlq():
    pass

@dlq.command(name="list") # queuectl dlq list
def dlq_list():
    jobs = list_dlq()
    if not jobs:
        click.echo("DLQ is empty.")
        return
    for j in jobs:
        click.echo(f"{j['id']} | {j['command']} | {j['state']}")

@dlq.command(name="retry") # queuectl dlq retry <job_id>
@click.argument("job_id")
def dlq_retry(job_id):
    success = retry_from_dlq(job_id)
    if success:
        click.echo(f"Moved job {job_id} back to pending queue.")
    else:
        click.echo(f"Job {job_id} not found in DLQ.")

# ----------------------------
# CONFIG GROUP
# ----------------------------
@cli.group() # queuectl config
def config():
    pass

@config.command(name="set") # queuectl config set max-retries 3
@click.argument("key")
@click.argument("value")
def config_set(key, value):
    with open("config.json", "r") as f:
        data = json.load(f)

    if key not in data:
        click.echo(f"Invalid config key: {key}")
        return

    # convert to int where appropriate
    try:
        value = int(value)
    except:
        pass

    data[key] = value

    with open("config.json", "w") as f:
        json.dump(data, f, indent=2)

    click.echo(f"Updated config '{key}' to '{value}'")
