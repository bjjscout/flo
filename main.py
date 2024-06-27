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

# Dictionary to store file creation times
file_creation_times = {}

# Semaphore to limit concurrent conversions
MAX_CONCURRENT_CONVERSIONS = 3
conversion_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CONVERSIONS)

async def delete_file_after_delay(filepath: str, delay: int):
    await asyncio.sleep(delay)
    try:
        os.remove(filepath)
        del file_creation_times[filepath]
        logger.info(f"Deleted file: {filepath}")
    except OSError as e:
        logger.error(f"Error deleting file {filepath}: {e}")

async def convert_m3u8_to_mp3(input_url: str, output_path: str):
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
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                error_message = stderr.decode()
                logger.error(f"Error converting to MP3: {error_message}")
                raise RuntimeError(f"Failed to convert HLS to MP3: {error_message}")

            logger.info(f"Conversion completed: {output_path}")

        except Exception as e:
            logger.error(f"Conversion error: {str(e)}")
            raise

@app.post("/convert")
async def convert_m3u8_to_mp3_endpoint(request: Request, m3u8_request: M3U8Request, background_tasks: BackgroundTasks):
    try:
        logger.info(f"Received conversion request for URL: {m3u8_request.url}")
        temp_dir = tempfile.mkdtemp()
        filename = f"{uuid.uuid4()}.mp3"
        output_path = os.path.join(temp_dir, filename)

        logger.info(f"Starting conversion")
        await convert_m3u8_to_mp3(m3u8_request.url, output_path)
        logger.info(f"Conversion completed")

        background_tasks.add_task(delete_file_after_delay, output_path, 30 * 60)
        file_creation_times[output_path] = time.time()

        # Construct the full download URL
        base_url = str(request.base_url).rstrip('/')
        download_url = f"{base_url}/download/{filename}"

        return {"download_url": download_url, "file_path": output_path}
    except Exception as e:
        logger.error(f"Error in conversion process: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/download/{filename}")
async def download_file(filename: str):
    for filepath, creation_time in file_creation_times.items():
        if filepath.endswith(filename):
            if os.path.exists(filepath):
                return FileResponse(filepath, filename=filename, media_type='audio/mpeg')
            else:
                raise HTTPException(status_code=404, detail="File not found or expired")
    raise HTTPException(status_code=404, detail="File not found")

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
            logger.info("FFmpeg version: {}".format(ffmpeg_version))
        else:
            logger.error("FFmpeg not found or error: {}".format(stderr.decode()))
    except Exception as e:
        logger.error("Error checking FFmpeg: {}".format(str(e)))

async def cleanup_old_files():
    while True:
        current_time = time.time()
        files_to_delete = [file for file, creation_time in file_creation_times.items() 
                           if current_time - creation_time > 30 * 60]
        for file in files_to_delete:
            try:
                os.remove(file)
                del file_creation_times[file]
                logger.info(f"Cleaned up old file: {file}")
            except OSError as e:
                logger.error(f"Error deleting old file {file}: {e}")
        await asyncio.sleep(60)  # Check every minute

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv('PORT', 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)