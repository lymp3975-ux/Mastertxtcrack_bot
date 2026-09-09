import asyncio
import os
import mmap
import concurrent.futures
import aiohttp
import aiofiles
import tempfile
from datetime import datetime
from pathlib import Path
from pyrogram import Client, filters
from pyrogram.types import Message
import multiprocessing as mp
from typing import List, Tuple
import re

# Get variables directly from Railway environment
API_ID = os.environ.get("API_ID")  # Railway sets this
API_HASH = os.environ.get("API_HASH")  # Railway sets this
BOT_TOKEN = os.environ.get("BOT_TOKEN")  # Railway sets this
ALLOWED_USER_ID = int(os.environ.get("ALLOWED_USER_ID", "0"))  # Railway sets this

# Performance settings
CHUNK_SIZE = 64 * 1024 * 1024  # 64MB chunks
MAX_WORKERS = mp.cpu_count()
READ_BUFFER = 1024 * 1024

app = Client(
    "file_search_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=MAX_WORKERS * 2
)

TEMP_DIR = Path("/tmp/file_search")
TEMP_DIR.mkdir(exist_ok=True)

user_states = {}

class FastFileSearcher:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.file_size = os.path.getsize(file_path)
        self.num_chunks = max(1, (self.file_size + CHUNK_SIZE - 1) // CHUNK_SIZE)
    
    def search_chunk(self, chunk_info: Tuple[int, int, str]) -> Tuple[int, List[bytes]]:
        chunk_start, chunk_size, search_term = chunk_info
        matches = []
        search_bytes = search_term.encode('utf-8', errors='ignore')
        
        with open(self.file_path, 'rb') as f:
            mm = mmap.mmap(f.fileno(), chunk_size, offset=chunk_start, access=mmap.ACCESS_READ)
            try:
                pos = 0
                while True:
                    pos = mm.find(search_bytes, pos)
                    if pos == -1:
                        break
                    
                    line_start = mm.rfind(b'\n', 0, pos) + 1
                    line_end = mm.find(b'\n', pos)
                    if line_end == -1:
                        line_end = chunk_size
                    
                    line = mm[line_start:line_end]
                    matches.append(line)
                    pos = line_end + 1
                    
                    if len(matches) >= 10000:
                        break
            finally:
                mm.close()
        
        return len(matches), matches
    
    async def search_parallel(self, search_term: str) -> Tuple[int, List[bytes]]:
        all_matches = []
        total_matches = 0
        
        chunk_tasks = []
        for i in range(self.num_chunks):
            chunk_start = i * CHUNK_SIZE
            chunk_size = min(CHUNK_SIZE, self.file_size - chunk_start)
            chunk_tasks.append((chunk_start, chunk_size, search_term))
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self.search_chunk, task) for task in chunk_tasks]
            
            for future in concurrent.futures.as_completed(futures):
                matches_count, matches = future.result()
                total_matches += matches_count
                all_matches.extend(matches)
        
        return total_matches, all_matches

class FastDownloader:
    @staticmethod
    async def download_with_progress(url: str, dest_path: Path, progress_callback=None):
        connector = aiohttp.TCPConnector(
            limit=10,
            limit_per_host=5,
            ttl_dns_cache=300,
            use_dns_cache=True
        )
        
        timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=60)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async with session.get(url, allow_redirects=True) as response:
                if response.status != 200:
                    raise Exception(f"Download failed: HTTP {response.status}")
                
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                last_update = 0
                
                async with aiofiles.open(dest_path, 'wb', buffering=READ_BUFFER) as f:
                    async for chunk in response.content.iter_chunked(READ_BUFFER):
                        await f.write(chunk)
                        downloaded += len(chunk)
                        
                        if progress_callback and total_size > 0:
                            percent = (downloaded / total_size) * 100
                            if percent - last_update >= 2:
                                await progress_callback(downloaded, total_size)
                                last_update = percent

@app.on_message(filters.command("start"))
async def start_command(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        await message.reply("❌ Unauthorized")
        return
    
    await message.reply(
        "⚡ **Lightning-Fast File Search Bot**\n\n"
        "**Commands:**\n"
        "/search - Search in a file\n"
        "/cancel - Cancel operation\n"
        "/status - System status\n\n"
        "Just send /search to begin!"
    )

@app.on_message(filters.command("search"))
async def search_command(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        await message.reply("❌ Unauthorized")
        return
    
    user_states[message.from_user.id] = {"state": "awaiting_url", "mode": "plain"}
    await message.reply("📎 Send me the direct download link:")

@app.on_message(filters.command("cancel"))
async def cancel_command(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        await message.reply("❌ Unauthorized")
        return
    
    user_states.pop(message.from_user.id, None)
    await message.reply("✅ Operation cancelled")

@app.on_message(filters.command("status"))
async def status_command(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        await message.reply("❌ Unauthorized")
        return
    
    cpu_count = mp.cpu_count()
    stat = os.statvfs(TEMP_DIR)
    free_space = (stat.f_bavail * stat.f_frsize) / (1024**3)
    
    await message.reply(
        f"📊 **System Status**\n\n"
        f"🖥 **CPU Cores:** {cpu_count}\n"
        f"💿 **Free Disk:** {free_space:.1f}GB\n"
        f"⚡ **Search Engine:** Memory-mapped parallel"
    )

@app.on_message(filters.text & filters.private)
async def handle_messages(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        await message.reply("❌ Unauthorized")
        return
    
    user_id = message.from_user.id
    state_data = user_states.get(user_id, {})
    state = state_data.get("state")
    
    if state == "awaiting_url":
        user_states[user_id] = {
            "state": "awaiting_search_term",
            "url": message.text.strip(),
            "mode": state_data.get("mode", "plain")
        }
        await message.reply("🔍 Now send the text to search for:")
    
    elif state == "awaiting_search_term":
        search_term = message.text.strip()
        url = state_data["url"]
        
        user_states.pop(user_id, None)
        
        status_msg = await message.reply("⚡ Initializing high-speed search...")
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            download_path = TEMP_DIR / f"download_{user_id}_{timestamp}.txt"
            output_path = TEMP_DIR / f"results_{user_id}_{timestamp}.txt"
            
            downloader = FastDownloader()
            async def download_progress(downloaded, total):
                percent = (downloaded / total) * 100
                await status_msg.edit_text(
                    f"⬇️ **Downloading:** {percent:.1f}%\n"
                    f"📦 Size: {downloaded / (1024*1024):.1f}MB / {total / (1024*1024):.1f}MB"
                )
            
            await downloader.download_with_progress(url, download_path, download_progress)
            
            await status_msg.edit_text("🔍 Searching with parallel memory-mapped engine...")
            
            searcher = FastFileSearcher(str(download_path))
            
            start_time = datetime.now()
            matches_count, matches = await searcher.search_parallel(search_term)
            search_time = (datetime.now() - start_time).total_seconds()
            
            if matches_count > 0:
                async with aiofiles.open(output_path, 'wb') as f:
                    for match in matches[:100000]:
                        await f.write(match + b'\n')
                
                file_size = os.path.getsize(output_path)
                
                if file_size > 49 * 1024 * 1024:
                    # Split file
                    parts = split_file(output_path, 49 * 1024 * 1024)
                    
                    await status_msg.edit_text(
                        f"✅ **Found:** {matches_count:,} matches\n"
                        f"⚡ **Search time:** {search_time:.2f}s\n"
                        f"📁 Sending {len(parts)} parts..."
                    )
                    
                    for i, part_path in enumerate(parts, 1):
                        await message.reply_document(
                            part_path,
                            caption=f"📄 Part {i}/{len(parts)} - Found {matches_count:,} matches for '{search_term}'"
                        )
                        part_path.unlink(missing_ok=True)
                else:
                    await message.reply_document(
                        output_path,
                        caption=f"📄 Found {matches_count:,} matches\n⚡ Search time: {search_time:.2f}s"
                    )
                
                await status_msg.delete()
            else:
                await status_msg.edit_text(
                    f"❌ No matches found for '{search_term}'\n"
                    f"⚡ Search time: {search_time:.2f}s"
                )
            
            download_path.unlink(missing_ok=True)
            output_path.unlink(missing_ok=True)
            
        except Exception as e:
            await status_msg.edit_text(f"❌ Error: {str(e)}")
        finally:
            for file in TEMP_DIR.glob(f"*_{user_id}_{timestamp}*"):
                file.unlink(missing_ok=True)

def split_file(file_path: Path, max_size: int) -> List[Path]:
    parts = []
    file_size = os.path.getsize(file_path)
    num_parts = (file_size + max_size - 1) // max_size
    
    with open(file_path, 'rb') as f:
        for i in range(num_parts):
            part_path = file_path.parent / f"{file_path.stem}_part{i+1}.txt"
            with open(part_path, 'wb') as part_file:
                remaining = min(max_size, file_size - i * max_size)
                while remaining > 0:
                    chunk = f.read(min(READ_BUFFER, remaining))
                    if not chunk:
                        break
                    part_file.write(chunk)
                    remaining -= len(chunk)
            parts.append(part_path)
    
    return parts

if __name__ == "__main__":
    print("⚡ High-Performance File Search Bot starting...")
    print(f"CPU Cores: {mp.cpu_count()}")
    app.run()
