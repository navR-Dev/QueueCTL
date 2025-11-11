import click # Building the CLI
from multiprocessing import Pool # Enables parallel job execution
from job import Job
from storage import add_job, get_jobs_by_state 
from worker import process_job 
from dlq import list_dlq, retry_from_dlq 

@click.group() # Declares CLI entry point group
def cli(): # Main command container
    pass

@cli.command() # Registers as a subcommand
@click.argument('command') 
@click.option('--max-retries', default=3, help='Maximum retry attempts for this job') # Configurable max retries
def enqueue(command, max_retries):
    job = Job(command, max_retries=max_retries)
    add_job(job)
    click.echo(f"Enqueued job {job.id} ({command})")

@cli.command() 
@click.option('--state', default="pending", help='Filter jobs by state (pending, failed, completed)') # Filter option
def list(state):
    jobs = get_jobs_by_state(state) 
    if not jobs: 
        click.echo(f"No jobs found with state '{state}'") 
        return
    for j in jobs: 
        click.echo(f"{j['id']} | {j['command']} | {j['state']}") 

@cli.command() 
@click.option('--workers', default=1, help='Number of parallel worker processes') # Parallel workers
@click.option('--base', default=2, help='Base value for exponential backoff (delay = base^attempts)') # Backoff base
def run(workers, base):
    jobs = get_jobs_by_state("pending")
    if not jobs:
        click.echo("No pending jobs found.")
        return

    click.echo(f"Starting {workers} worker(s) to process {len(jobs)} job(s)...")
    jobs = [Job.from_dict(j) for j in jobs]

    if workers == 1: # Single worker execution
        for job in jobs:
            process_job(job, base)
    else: # Parallel execution
        with Pool(workers) as pool: # Create worker pool
            pool.starmap(process_job, [(job, base) for job in jobs]) # Map jobs to workers

@cli.command(name="dlq-list")
def dlq_list():
    dlq_jobs = list_dlq() # Fetch jobs in DLQ
    if not dlq_jobs:
        click.echo("No jobs found in the Dead Letter Queue.")
        return
    for j in dlq_jobs:
        click.echo(f"{j['id']} | {j['command']} | {j['state']}")

@cli.command(name="dlq-retry")
@click.argument('job_id')
def dlq_retry(job_id):
    success = retry_from_dlq(job_id)
    if success:
        click.echo(f"Moved job {job_id} back to pending queue.")
    else: 
        click.echo(f"Job {job_id} not found in DLQ.") 
