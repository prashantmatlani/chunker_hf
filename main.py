
# /pb/py/chunker/hf/main.py
# ./main.py

import zipfile
import io

import os
import asyncio
import json
import uvicorn
from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import HTMLResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import shutil
import glob

# Import chunking logic from the existing combined script
# Note: Ensure script functions are wrap-able or callable
from chunker_2 import run_chunking_process 

app = FastAPI()

# Global store to keep track of progress for the UI
progress_queue = asyncio.Queue()

@app.get("/", response_class=HTMLResponse)
async def get_ui():
    with open("index.html", "r") as f:
        return f.read()

"""
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
"""

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


@app.post("/upload")
async def handle_upload(
    file: UploadFile = File(...),
    whole: str = Form("false"),
    start: str = Form("20"),
    end: str = Form("30")
):
    temp_path = f"temp_{file.filename}"
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # Fix: Convert strings to proper types
    is_whole = whole.lower() == "true"
    s_page = int(start)
    #s_page = s_page-1 if s_page != 1 else 0
    e_page = int(end)

    #Debugging the values received from the UI
    print(f"📡 UI SIGNAL RECEIVED: whole={is_whole}, start={s_page}, end={e_page}")

    # Start the task with explicit parameters; pass everything to the aggregator
    asyncio.create_task(run_chunking_process(
        temp_path, 
        progress_queue, 
        whole=is_whole, 
        start_p=s_page, 
        end_p=e_page
    ))
    return {"status": "Processing started"}

#"""
@app.get("/download-latest")
async def download_latest():
    # Look for files matching our pattern
    files = glob.glob("knowledge_tree_*.json")
    if not files: 
        return {"error": "No JSON files found yet. Finish an extraction first."}
    # Sort by creation time to get the newest one
    latest_file = max(files, key=os.path.getctime)
    return FileResponse(path=latest_file, filename=os.path.basename(latest_file))
#"""

@app.get("/download-markdown")
async def download_md(type: str = "nested"):
    pattern = "nested_knowledge_*.md" if type == "nested" else "table_knowledge_*.md"
    files = glob.glob(pattern)
    if not files: return {"error": "No markdown found"}
    latest = max(files, key=os.path.getctime)
    return FileResponse(path=latest, filename=os.path.basename(latest))

@app.get("/download-all")
async def download_all():
    # Find the latest files for each type
    json_files = glob.glob("knowledge_tree_*.json")
    nested_files = glob.glob("nested_knowledge_*.md")
    table_files = glob.glob("table_knowledge_*.md")
    
    if not json_files:
        return {"error": "No files found. Please complete a run first."}

    # Identify the newest ones
    latest_json = max(json_files, key=os.path.getctime)
    # Match the timestamp from the JSON to get the corresponding MDs
    timestamp = os.path.basename(latest_json).replace("knowledge_tree_", "").replace(".json", "")
    
    files_to_zip = [
        latest_json,
        f"nested_knowledge_{timestamp}.md",
        f"table_knowledge_{timestamp}.md"
    ]

    # Create an in-memory ZIP file
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for file_path in files_to_zip:
            if os.path.exists(file_path):
                zip_file.write(file_path, os.path.basename(file_path))
    
    zip_buffer.seek(0)
    return StreamingResponse(
        zip_buffer, 
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": f"attachment; filename=jung_knowledge_base_{timestamp}.zip"}
    )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=7860)
