import json # JSON operations (reading and writing)
import os # Check if JSON file exists
from threading import Lock # Implements locking to prevent duplicate processing

# Load configuration file
with open("config.json") as f: 
    CONFIG = json.load(f) # Read configuration values

FILE_PATH = CONFIG["queue_file"] # Use dynamic file path from config
LOCK = Lock() # Thread-safe lock object

# Helper function - Load job queue data
def _load():
    if not os.path.exists(FILE_PATH): 
        return {"jobs": [], "dlq": []} # If the file doesn't exist, return empty structure
    with open(FILE_PATH, "r") as f:
        return json.load(f) # Load data from JSON file

# Helper function - Safely save updated data to the disk
def _save(data):
    with LOCK, open(FILE_PATH, "w") as f:
        json.dump(data, f, indent=2) # Write updated data with indentation

# Add a new job to the queue
def add_job(job):
    data = _load()
    data["jobs"].append(job.to_dict()) # Add job as dictionary
    _save(data)

# Modify a job's record
def update_job(job):
    data = _load()
    for i, j in enumerate(data["jobs"]): # Find job by ID
        if j["id"] == job.id: 
            data["jobs"][i] = job.to_dict() # Replace job data
            break
    _save(data)

# Move job to DLQ
def move_to_dlq(job):
    data = _load()
    data["jobs"] = [j for j in data["jobs"] if j["id"] != job.id] # Remove from jobs
    data["dlq"].append(job.to_dict()) # Add to DLQ
    _save(data)

# Retrieve all jobs in a specific state
def get_jobs_by_state(state):
    return [j for j in _load()["jobs"] if j["state"] == state] # Filter by state
