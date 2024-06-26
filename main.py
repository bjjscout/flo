import os
import tempfile
import uuid
import time
import asyncio
import aiohttp
import m3u8
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import ffmpeg
from typing import List
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

async def fetch_m3u8_content(url: str) -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()

def parse_m3u8(content: str) -> List[str]:
    playlist = m3u8.loads(content)
    return [variant.uri for variant in playlist.playlists]

async def get_audio_stream_url(m3u8_url: str) -> str:
    content = await fetch_m3u8_content(m3u8_url)
    streams = parse_m3u8(content)
    
    if streams:
        return streams[0]
    else:
        raise ValueError("No audio streams found in the M3U8 playlist")

async def convert_m3u8_to_mp3(input_url: str, output_path: str):
    async with conversion_semaphore:
        try:
            stream = ffmpeg.input(input_url)
            stream = ffmpeg.output(stream, output_path, acodec='libmp3lame', ab='128k')
            await asyncio.to_thread(ffmpeg.run, stream, overwrite_output=True)
        except ffmpeg.Error as e:
            logger.error(f"FFmpeg error: {e.stderr.decode() if e.stderr else str(e)}")
            raise

@app.post("/convert")
async def convert_m3u8_to_mp3_endpoint(request: M3U8Request, background_tasks: BackgroundTasks):
    try:
        temp_dir = tempfile.mkdtemp()
        filename = f"{uuid.uuid4()}.mp3"
        output_path = os.path.join(temp_dir, filename)

        audio_stream_url = await get_audio_stream_url(request.url)
        await convert_m3u8_to_mp3(audio_stream_url, output_path)

        background_tasks.add_task(delete_file_after_delay, output_path, 30 * 60)
        file_creation_times[output_path] = time.time()

        return {"download_url": f"/download/{filename}", "file_path": output_path}
    except Exception as e:
        logger.error(f"Error in conversion process: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/download/{filename}")
async def download_file(filename: str):
    for filepath, creation_time in file_creation_times.items():
        if filepath.endswith(filename):
            if os.path.exists(filepath):
                return {"file_path": filepath}
            else:
                raise HTTPException(status_code=404, detail="File not found or expired")
    raise HTTPException(status_code=404, detail="File not found")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(cleanup_old_files())

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