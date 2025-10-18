from fastapi from FastAPI

app = FastAPI()
logs = []
@app.get("/logs")
async def ingest_logs(levlel: str, message: str, service: str):
    print("inside the ingest method /logs endpoint")
    log_entry = {
        "level": levlel,
        "message": message,
        "service": service
    }
    logs.append(log_entry)
    return {"status": "success", "data": logs}

@app.get("/getLevels")
async def get_levels():
    levels = ["INFO", "DEBUG", "ERROR", "WARNING", "CRITICAL"]
    return {"status": "success", "levels": levels}

@app.get("/stats")
async def get_stats():
    by_level = {}
    total = 0
    for log in logs:
        level = log("level")
        if level not in by_level:
            by_level[level] = 0
        else:
            by_level[level] += 1
        total += 1
    return {
        "status": "success",
        "total": total,
        "by_level": by_level
    }


