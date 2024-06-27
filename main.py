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

async def get_audio_stream_url(m3u8_url: str) -> str:
    content = await fetch_m3u8_content(m3u8_url)
    playlist = m3u8.loads(content)
    
    # First, look for audio-only streams
    audio_streams = [variant for variant in playlist.playlists if 'AUDIO' in variant.stream_info.keys()]
    
    if audio_streams:
        stream_url = audio_streams[0].uri
    elif playlist.playlists:
        # If no audio-only stream, use the first available stream
        stream_url = playlist.playlists[0].uri
    else:
        # If no playlists found, use the main m3u8 URL
        return m3u8_url

    # If the stream URL is relative, make it absolute
    if not stream_url.startswith('http'):
        base_url = m3u8_url.rsplit('/', 1)[0]
        stream_url = f"{base_url}/{stream_url}"
    
    return stream_url

async def convert_m3u8_to_mp3(input_url: str, output_path: str):
    async with conversion_semaphore:
        try:
            # First, probe the input to check for audio streams
            probe = await asyncio.to_thread(ffmpeg.probe, input_url)
            audio_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'audio'), None)
            
            if audio_stream:
                logger.info("Audio stream found in input")
                stream = ffmpeg.input(input_url, protocol_whitelist='file,http,https,tcp,tls,crypto')
                stream = ffmpeg.output(stream, output_path, acodec='libmp3lame', ab='128k', map='a')
            else:
                logger.info("No audio stream found, attempting to extract audio from video")
                stream = ffmpeg.input(input_url, protocol_whitelist='file,http,https,tcp,tls,crypto')
                stream = ffmpeg.output(stream, output_path, acodec='libmp3lame', ab='128k', vn=None)
            
            cmd = ffmpeg.compile(stream)
            logger.info(f"FFmpeg command: {' '.join(cmd)}")
            await asyncio.to_thread(ffmpeg.run, stream, capture_stderr=True, overwrite_output=True)
        except ffmpeg.Error as e:
            logger.error(f"FFmpeg error: {e.stderr.decode() if e.stderr else str(e)}")
            raise

@app.post("/convert")
async def convert_m3u8_to_mp3_endpoint(request: M3U8Request, background_tasks: BackgroundTasks):
    try:
        logger.info(f"Received conversion request for URL: {request.url}")
        temp_dir = tempfile.mkdtemp()
        filename = f"{uuid.uuid4()}.mp3"
        output_path = os.path.join(temp_dir, filename)

        logger.info(f"Getting audio stream URL")
        audio_stream_url = await get_audio_stream_url(request.url)
        logger.info(f"Audio stream URL: {audio_stream_url}")

        logger.info(f"Starting conversion")
        await convert_m3u8_to_mp3(audio_stream_url, output_path)
        logger.info(f"Conversion completed")

        background_tasks.add_task(delete_file_after_delay, output_path, 30 * 60)
        file_creation_times[output_path] = time.time()

        return {"download_url": f"/download/{filename}", "file_path": output_path}
    except Exception as e:
        logger.error(f"Error in conversion process: {str(e)}", exc_info=True)
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