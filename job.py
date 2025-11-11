import uuid # For universally unique identifiers
from datetime import datetime, timezone # For timestamps

class Job:
    # Constructor - Initialising all the job parameters, as specified in the job description:
    def __init__(self, command, max_retries=3): # max_retries - configurable, default value is 3
        
        self.id = str(uuid.uuid4()) # Unique to each job
        
        self.command = command # Passed during creation - Specifies the command to be executed
        
        self.state = "pending" # State of the job - Initialised as pending as the job is not assigned to a worker
        
        self.attempts = 0 # How many attempts have been made to execute the job
        
        self.max_retries = max_retries # Maximum times a job can be executed before it is moved to the DLQ
        
        now = datetime.now(timezone.utc).isoformat() # Gets current time (UTC)
        self.created_at = now # Timestamp for job creation - initialised as current time
        self.updated_at = now # Timestamp for job updation - initialised as current time

    def to_dict(self):
        return self.__dict__ # Built - in attribute to store all instance variables as a dictionary

    @classmethod # Method is called on the class, not an instance
    def from_dict(cls, data):
        job = cls(data["command"], data.get("max_retries", 3)) # Job creation
        job.__dict__.update(data) # Setting the field values
        return job
