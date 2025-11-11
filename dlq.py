from storage import _load, _save # To access and modify stored queue data
from job import Job # To reconstruct Job objects when needed

# Retrieve all jobs currently present in the DLQ
def list_dlq():
    data = _load() 
    return data["dlq"] 

# Move a specific job from DLQ back to the pending queue for reprocessing
def retry_from_dlq(job_id):
    data = _load() 
    job = next((j for j in data["dlq"] if j["id"] == job_id), None)
    if not job:
        return False 
    data["dlq"].remove(job) # Remove from DLQ
    job["state"] = "pending" # Reset job state to pending
    job["attempts"] = 0 # Reset attempts count
    data["jobs"].append(job) # Move job back to the main job queue
    _save(data) 
    return True 
