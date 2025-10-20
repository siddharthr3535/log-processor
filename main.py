from fastapi import FastAPI
from datetime import datetime
from typing import Optional
from queue import Queue
from threading import Thread
import time

from database import LogDatabase

app = FastAPI(title="Log Processor")


db = LogDatabase()


log_queue = Queue()


batch = []
BATCH_SIZE = 100
batch_lock = Thread()

def process_worker():

    local_batch = []
    
    while True:
        try:

            log = log_queue.get(timeout=1)
            

            log["processed_at"] = datetime.now().isoformat()
            log["word_count"] = len(log["message"].split())
            

            local_batch.append(log)
            

            if len(local_batch) >= BATCH_SIZE:
                count = db.insert_logs_batch(local_batch)
                local_batch = []
            
            log_queue.task_done()
            
        except Exception as e:
            if local_batch:
                count = db.insert_logs_batch(local_batch)
                local_batch = []
                print("inserted " , count, "logs from timeout")


NUM_WORKERS = 4
for i in range(NUM_WORKERS):
    worker = Thread(target=process_worker, daemon=True)
    worker.start()

@app.post("/logs")
async def ingest_logs(level: str, message: str, service: str):
    log_entry = {
        "level": level.upper(),
        "message": message,
        "service": service,
        "received_at": datetime.now().isoformat()
    }
    
    log_queue.put(log_entry)
    
    return {
        "status": "queued",
        "queue_size": log_queue.qsize()
    }

@app.get("/stats")
async def get_stats():
    stats = db.get_stats()
    stats["queued"] = log_queue.qsize()
    return stats

@app.get("/logs")
async def get_logs(level: Optional[str] = None, limit: int = 100):
    logs = db.get_logs(level=level, limit=limit)
    return {
        "count": len(logs),
        "logs": logs
    }

@app.get("/search")
async def search_logs(q: str, limit: int = 100):
    logs = db.search_logs(search_term=q, limit=limit)
    return {
        "count": len(logs),
        "query": q,
        "logs": logs
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)