
# /pb/py/chunker/main.py
# ./main.py

import os
import asyncio
import json
from fastapi import FastAPI, UploadFile, File, BackgroundTask
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import shutil

# Import chunking logic from the existing combined script
# Note: Ensure script functions are wrap-able or callable
from phase0102_chunker_aggregator_2 import run_chunking_process 

app = FastAPI()

# Global store to keep track of progress for the UI
progress_queue = asyncio.Queue()

@app.get("/", response_class=HTMLResponse)
async def get_ui():
    with open("index.html", "r") as f:
        return f.read()

@app.post("/upload")
async def handle_upload(file: UploadFile = File(...)):
    # Save the uploaded PDF to a local temp file
    temp_path = f"temp_{file.filename}"
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Start the chunking in the background so the UI doesn't freeze
    # We pass the queue so the script can "push" updates to it
    asyncio.create_task(run_chunking_process(temp_path, progress_queue))
    
    return {"status": "Processing started", "filename": file.filename}

@app.get("/stream")
async def stream_updates():
    """
    This is the SSE endpoint. The browser listens here to get 
    real-time updates as chunks are created.
    """
    async def event_generator():
        while True:
            # Wait for a new chunk/summary from the background task
            data = await progress_queue.get()
            if data == "DONE":
                yield "data: {\"type\": \"done\"}\n\n"
                break
            yield f"data: {json.dumps(data)}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7860)
