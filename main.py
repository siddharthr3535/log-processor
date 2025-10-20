from fastapi import FastAPI
from datetime import datetime
from typing import Optional
from queue import Queue
from threading import Thread
import time

app = FastAPI(title="Log Processor")

#for incoming logs
log_queue = Queue()  # Incoming logs
processed_logs = []  

def process_worker():
    #process logs from queuees
    print("starting worker")
    while True:

        log = log_queue.get()
        
        log["processed_at"] = datetime.now().isoformat()
        log["word_count"] = len(log["message"].split())
        

        processed_logs.append(log)
        
        print(f"Processed: [{log['level']}] {log['service']} - Total: {len(processed_logs)}")
        
        log_queue.task_done()

NUM_WORKERS = 4
for i in range(NUM_WORKERS):
    worker = Thread(target=process_worker, daemon=True)
    worker.start()
    print("worker " , i + 1 , " starting")

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
    by_level = {}
    by_service = {}
    
    for log in processed_logs:
        level = log["level"]
        service = log["service"]
        by_level[level] = by_level.get(level, 0) + 1
        by_service[service] = by_service.get(service, 0) + 1
    
    return {
        "queued": log_queue.qsize(),
        "processed": len(processed_logs),
        "by_level": by_level,
        "by_service": by_service
    }

@app.get("/logs")
async def get_logs(level: Optional[str] = None, limit: int = 50):
    filtered = processed_logs
    
    if level:
        filtered = [log for log in processed_logs if log["level"] == level.upper()]
    
    return {
        "count": len(filtered),
        "logs": filtered[-limit:][::-1]  
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)