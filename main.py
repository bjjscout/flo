import os
import tempfile
import uuid
import time
import asyncio
import aiohttp
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import logging

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class M3U8Request(BaseModel):
    url: str

# Dictionary to store file creation times and conversion progress
file_info = {}

# Semaphore to limit concurrent conversions
MAX_CONCURRENT_CONVERSIONS = 3
conversion_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CONVERSIONS)

async def delete_file_after_delay(filepath: str, delay: int):
    await asyncio.sleep(delay)
    try:
        os.remove(filepath)
        del file_info[filepath]
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
                '-ab', '128k',
                output_path
            ]
            
            logger.info(f"Converting HLS to MP3: {' '.join(cmd)}")
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            while True:
                if process.stdout.at_eof() and process.stderr.at_eof():
                    break
                for fd in (process.stderr, process.stdout):
                    line = await fd.readline()
                    if line:
                        if b"time=" in line:
                            # Extract time information and update progress
                            time_info = line.decode().split("time=")[1].split()[0]
                            file_info[task_id]['progress'] = time_info
            
            await process.wait()
            
            if process.returncode != 0:
                error_message = await process.stderr.read()
                logger.error(f"Error converting to MP3: {error_message.decode()}")
                raise RuntimeError(f"Failed to convert HLS to MP3: {error_message.decode()}")

            logger.info(f"Conversion completed: {output_path}")
            file_info[task_id]['status'] = 'completed'

        except Exception as e:
            logger.error(f"Conversion error: {str(e)}")
            file_info[task_id]['status'] = 'failed'
            file_info[task_id]['error'] = str(e)
            raise

@app.post("/convert")
async def convert_m3u8_to_mp3_endpoint(request: Request, m3u8_request: M3U8Request, background_tasks: BackgroundTasks):
    try:
        logger.info(f"Received conversion request for URL: {m3u8_request.url}")
        temp_dir = tempfile.mkdtemp()
        filename = f"{uuid.uuid4()}.mp3"
        output_path = os.path.join(temp_dir, filename)
        task_id = str(uuid.uuid4())

        file_info[task_id] = {
            'status': 'processing',
            'progress': '0:00:00',
            'output_path': output_path,
            'filename': filename
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
async def get_conversion_status(task_id: str):
    if task_id in file_info:
        status_info = file_info[task_id]
        if status_info['status'] == 'completed':
            # Construct the full download URL
            base_url = str(request.base_url).rstrip('/')
            download_url = f"{base_url}/download/{status_info['filename']}"
            return {"status": status_info['status'], "download_url": download_url}
        elif status_info['status'] == 'failed':
            return {"status": status_info['status'], "error": status_info.get('error', 'Unknown error')}
        else:
            return {"status": status_info['status'], "progress": status_info['progress']}
    else:
        raise HTTPException(status_code=404, detail="Task not found")

@app.get("/download/{filename}")
async def download_file(filename: str):
    logger.info(f"Download request received for filename: {filename}")
    try:
        for task_id, info in file_info.items():
            if info['filename'] == filename:
                filepath = info['output_path']
                logger.info(f"Found matching file: {filepath}")
                if os.path.exists(filepath):
                    logger.info(f"File exists, attempting to serve: {filepath}")
                    try:
                        return FileResponse(filepath, filename=filename, media_type='audio/mpeg')
                    except Exception as e:
                        logger.error(f"Error serving file {filepath}: {str(e)}", exc_info=True)
                        raise HTTPException(status_code=500, detail=f"Error serving file: {str(e)}")
                else:
                    logger.warning(f"File not found or expired: {filepath}")
                    raise HTTPException(status_code=404, detail="File not found or expired")
        
        logger.warning(f"No matching file found for filename: {filename}")
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        logger.error(f"Unexpected error in download_file: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(cleanup_old_files())
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

async def cleanup_old_files():
    while True:
        current_time = time.time()
        tasks_to_delete = [task_id for task_id, info in file_info.items() 
                           if current_time - info.get('creation_time', 0) > 30 * 60]
        for task_id in tasks_to_delete:
            try:
                os.remove(file_info[task_id]['output_path'])
                del file_info[task_id]
                logger.info(f"Cleaned up old file for task: {task_id}")
            except OSError as e:
                logger.error(f"Error deleting old file for task {task_id}: {e}")
        await asyncio.sleep(60)  # Check every minute

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv('PORT', 8080))
    uvicorn.run(app, host="0.0.0.0", port=port, timeout_keep_alive=120)