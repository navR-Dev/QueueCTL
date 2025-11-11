import json # JSON operations (reading and writing)
import os # Check if JSON file exists
from threading import Lock # Implements locking to prevent duplicate processing

FILE_PATH = "queue.json" # File that stores the job and DLQ data
LOCK = Lock() # Thread - safe lock object

# Helper function - Load job queue data
def _load():
    if not os.path.exists(FILE_PATH): 
        return {"jobs": [], "dlq": []} # If the file doesn't exist, handle gracefully instad of throwing an error
    with open(FILE_PATH, "r") as f:
        return json.load(f) # Open the file in read - only mode, and parse into a dictionary

# Helper function - Safely save updated data to the disk
def _save(data):
    with LOCK, open(FILE_PATH, "w") as f:
        json.dump(data, f, indent=2) # Open file in write mode, with a lock, then convert the dictionary to json and write it back neatly (2 space indent)

# Add a new job to the queue
def add_job(job):
    data = _load()
    data["jobs"].append(job.to_dict()) # Add the job object to the queue after converting it to a dictionary
    _save(data) 

# Modify a job's record
def update_job(job):
    data = _load()
    for i, j in enumerate(data["jobs"]): # Uses enumerate to get the index and job
        if j["id"] == job.id: 
            data["jobs"][i] = job.to_dict()
            break
    _save(data)

# Move job to DLQ
def move_to_dlq(job):
    data = _load()
    data["jobs"] = [j for j in data["jobs"] if j["id"] != job.id] # List comprehension to remove failed job(s)
    data["dlq"].append(job.to_dict()) # Add job (in dictionary form) to DLQ
    _save(data)

# Retrieve all jobs in a specific state
def get_jobs_by_state(state):
    return [j for j in _load()["jobs"] if j["state"] == state] # List comprehension to get all jobs in the specified state (passed to the function)
