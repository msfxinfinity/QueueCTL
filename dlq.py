# dlq.py
from storage import Storage
import json

storage = Storage()

def list_dlq(limit: int = 100):
    return [dict(r) for r in storage.list_dead(limit)]

def retry(job_id: int):
    storage.retry_dead_job(job_id)
