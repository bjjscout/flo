import os
import tempfile
import uuid
import time
import asyncio
import aiohttp
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel
import logging

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class M3U8Request(BaseModel):
    url: str

# Dictionary to store file creation times and task information
tasks = {}

# Semaphore to limit concurrent conversions
MAX_CONCURRENT_CONVERSIONS = 3
conversion_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CONVERSIONS)

async def delete_file_after_delay(filepath: str, delay: int):
    await asyncio.sleep(delay)
    try:
        os.remove(filepath)
        logger.info(f"Deleted file: {filepath}")
    except OSError as e:
        logger.error(f"Error deleting file {filepath}: {e}")

async def convert_m3u8_to_mp3(input_url: str, output_path: str, task_id: str):
    async with conversion_semaphore:
        try:
            # Direct conversion from HLS to MP3
            cmd = [
                'ffmpeg',
                '-i', input_url,
                '-vn',  # Disable video
                '-acodec', 'libmp3lame',
                '-ac', '1',  # Convert to mono
                '-ar', '16000',  # Resample to 16 kHz
                '-ab', '32k',  # Set bitrate to 32 kbps
                '-f', 'mp3',  # Force MP3 format
                output_path
            ]
            
            logger.info(f"Converting HLS to MP3: {' '.join(cmd)}")
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                error_message = stderr.decode()
                logger.error(f"Error converting to MP3: {error_message}")
                tasks[task_id]['status'] = 'failed'
                tasks[task_id]['error'] = error_message
            else:
                logger.info(f"Conversion completed: {output_path}")
                tasks[task_id]['status'] = 'completed'
                tasks[task_id]['file_path'] = output_path

        except Exception as e:
            logger.error(f"Conversion error: {str(e)}")
            tasks[task_id]['status'] = 'failed'
            tasks[task_id]['error'] = str(e)

@app.post("/convert")
async def start_conversion(request: Request, m3u8_request: M3U8Request, background_tasks: BackgroundTasks):
    try:
        logger.info(f"Received conversion request for URL: {m3u8_request.url}")
        temp_dir = tempfile.mkdtemp()
        filename = f"{uuid.uuid4()}.mp3"
        output_path = os.path.join(temp_dir, filename)
        task_id = str(uuid.uuid4())

        tasks[task_id] = {
            'status': 'processing',
            'filename': filename,
            'created_at': time.time()
        }

        background_tasks.add_task(convert_m3u8_to_mp3, m3u8_request.url, output_path, task_id)
        background_tasks.add_task(delete_file_after_delay, output_path, 30 * 60)

        # Construct the full status URL
        base_url = str(request.base_url).rstrip('/')
        status_url = f"{base_url}/status/{task_id}"

        return {"task_id": task_id, "status_url": status_url}
    except Exception as e:
        logger.error(f"Error in conversion process: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/status/{task_id}")
async def get_conversion_status(request: Request, task_id: str):
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task_info = tasks[task_id]
    if task_info['status'] == 'completed':
        base_url = str(request.base_url).rstrip('/')
        download_url = f"{base_url}/download/{task_info['filename']}"
        return {"status": "completed", "download_url": download_url}
    elif task_info['status'] == 'failed':
        return {"status": "failed", "error": task_info.get('error', 'Unknown error')}
    else:
        return {"status": "processing"}

@app.get("/download/{filename}")
async def download_file(filename: str):
    logger.info(f"Download request received for filename: {filename}")
    try:
        for task_id, task_info in tasks.items():
            if task_info['filename'] == filename:
                filepath = task_info.get('file_path')
                if filepath and os.path.exists(filepath):
                    logger.info(f"File exists, attempting to serve: {filepath}")
                    return FileResponse(filepath, filename=filename, media_type='audio/mpeg')
        
        logger.warning(f"No matching file found for filename: {filename}")
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        logger.error(f"Unexpected error in download_file: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(cleanup_old_tasks())
    # Test ffmpeg installation
    try:
        process = await asyncio.create_subprocess_exec(
            'ffmpeg', '-version',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        if process.returncode == 0:
            ffmpeg_version = stdout.decode().split('\n')[0]
            logger.info(f"FFmpeg version: {ffmpeg_version}")
        else:
            logger.error(f"FFmpeg not found or error: {stderr.decode()}")
    except Exception as e:
        logger.error(f"Error checking FFmpeg: {str(e)}")

async def cleanup_old_tasks():
    while True:
        current_time = time.time()
        tasks_to_delete = [task_id for task_id, info in tasks.items() 
                           if current_time - info['created_at'] > 30 * 60]
        for task_id in tasks_to_delete:
            task_info = tasks[task_id]
            if 'file_path' in task_info:
                try:
                    os.remove(task_info['file_path'])
                    logger.info(f"Cleaned up old file: {task_info['file_path']}")
                except OSError as e:
                    logger.error(f"Error deleting old file {task_info['file_path']}: {e}")
            del tasks[task_id]
        await asyncio.sleep(60)  # Check every minute

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv('PORT', 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)