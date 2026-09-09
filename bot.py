import asyncio
import os
import mmap
import concurrent.futures
import aiohttp
import aiofiles
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
import multiprocessing as mp
from typing import List, Tuple, Dict
import re
import shutil

# Configuration from Railway
API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ALLOWED_USER_ID = int(os.environ.get("ALLOWED_USER_ID", "0"))

# Performance settings
CHUNK_SIZE = 64 * 1024 * 1024  # 64MB chunks
MAX_WORKERS = mp.cpu_count()
READ_BUFFER = 1024 * 1024
MAX_FILE_AGE = 3600  # 1 hour in seconds

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
active_tasks = {}

class FileCleaner:
    """Auto-delete old files"""
    
    @staticmethod
    async def cleanup_old_files():
        """Delete files older than MAX_FILE_AGE"""
        while True:
            try:
                current_time = datetime.now().timestamp()
                for file in TEMP_DIR.iterdir():
                    if file.is_file():
                        file_age = current_time - file.stat().st_mtime
                        if file_age > MAX_FILE_AGE:
                            file.unlink(missing_ok=True)
                            print(f"🗑 Deleted old file: {file.name} (age: {file_age/60:.1f} min)")
                
                # Also clean empty directories
                for dir in TEMP_DIR.iterdir():
                    if dir.is_dir() and not any(dir.iterdir()):
                        dir.rmdir()
                        
            except Exception as e:
                print(f"Cleanup error: {e}")
            
            await asyncio.sleep(300)  # Check every 5 minutes
    
    @staticmethod
    async def schedule_deletion(file_path: Path, delay_seconds: int = 300):
        """Schedule file deletion after delay"""
        await asyncio.sleep(delay_seconds)
        file_path.unlink(missing_ok=True)
        print(f"🗑 Scheduled deletion: {file_path.name}")

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

class MultiFileSearch:
    """Handle multiple files and search terms"""
    
    @staticmethod
    async def search_multiple_files(
        files: List[Path],
        search_terms: List[str],
        progress_callback=None
    ) -> Dict[str, Dict]:
        """Search multiple terms in multiple files"""
        results = {}
        
        for file_idx, file_path in enumerate(files, 1):
            file_name = file_path.name
            results[file_name] = {}
            
            searcher = FastFileSearcher(str(file_path))
            
            for term_idx, term in enumerate(search_terms, 1):
                if progress_callback:
                    await progress_callback(f"Searching '{term}' in {file_name}...")
                
                matches_count, matches = await searcher.search_parallel(term)
                results[file_name][term] = {
                    'count': matches_count,
                    'matches': matches[:50000]  # Limit per term
                }
        
        return results

@app.on_message(filters.command("start"))
async def start_command(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        await message.reply("❌ Unauthorized")
        return
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 Single Search", callback_data="single_search")],
        [InlineKeyboardButton("📁 Multiple Files", callback_data="multi_files")],
        [InlineKeyboardButton("🔎 Multi Terms", callback_data="multi_terms")],
        [InlineKeyboardButton("🧹 Clean Files", callback_data="clean_files")],
        [InlineKeyboardButton("📊 Status", callback_data="status")]
    ])
    
    await message.reply(
        "⚡ **Lightning-Fast File Search Bot**\n\n"
        "**Features:**\n"
        "• Memory-mapped parallel search\n"
        "• Multi-file support\n"
        "• Auto-delete old files\n"
        "• Handles 4GB+ files\n\n"
        "**Commands:**\n"
        "/search - Single file search\n"
        "/multi - Multiple files search\n"
        "/multiterms - Multiple terms search\n"
        "/clean - Delete all temp files\n"
        "/status - System status\n\n"
        "Choose an option below:",
        reply_markup=keyboard
    )

@app.on_message(filters.command("search"))
async def search_command(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        await message.reply("❌ Unauthorized")
        return
    
    user_states[message.from_user.id] = {"state": "awaiting_url", "mode": "single"}
    await message.reply("📎 Send me the direct download link:")

@app.on_message(filters.command("multi"))
async def multi_search_command(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        await message.reply("❌ Unauthorized")
        return
    
    user_states[message.from_user.id] = {"state": "awaiting_urls", "urls": [], "mode": "multi"}
    await message.reply(
        "📎 Send multiple download links (one per line):\n\n"
        "Example:\n"
        "https://example.com/file1.txt\n"
        "https://example.com/file2.txt\n"
        "https://example.com/file3.txt\n\n"
        "Type /done when finished"
    )

@app.on_message(filters.command("multiterms"))
async def multi_terms_command(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        await message.reply("❌ Unauthorized")
        return
    
    user_states[message.from_user.id] = {"state": "awaiting_url", "mode": "multi_terms"}
    await message.reply("📎 Send me the direct download link:")

@app.on_message(filters.command("clean"))
async def clean_command(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        await message.reply("❌ Unauthorized")
        return
    
    deleted = 0
    freed_space = 0
    
    for file in TEMP_DIR.iterdir():
        if file.is_file():
            size = file.stat().st_size
            file.unlink(missing_ok=True)
            deleted += 1
            freed_space += size
    
    await message.reply(
        f"🧹 **Cleanup Complete**\n\n"
        f"🗑 Deleted: {deleted} files\n"
        f"💾 Freed: {freed_space / (1024*1024):.1f}MB"
    )

@app.on_message(filters.command("done"))
async def done_command(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        await message.reply("❌ Unauthorized")
        return
    
    user_id = message.from_user.id
    state_data = user_states.get(user_id, {})
    
    if state_data.get("state") == "awaiting_urls" and state_data.get("urls"):
        urls = state_data["urls"]
        user_states[user_id] = {
            "state": "awaiting_search_term",
            "urls": urls,
            "mode": "multi"
        }
        await message.reply(f"✅ Received {len(urls)} URLs. Now send search term:")
    else:
        await message.reply("No URLs pending. Send links first.")

@app.on_message(filters.command("status"))
async def status_command(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        await message.reply("❌ Unauthorized")
        return
    
    cpu_count = mp.cpu_count()
    stat = os.statvfs(TEMP_DIR)
    free_space = (stat.f_bavail * stat.f_frsize) / (1024**3)
    
    # Count temp files
    temp_files = list(TEMP_DIR.iterdir())
    total_size = sum(f.stat().st_size for f in temp_files if f.is_file())
    
    await message.reply(
        f"📊 **System Status**\n\n"
        f"🖥 **CPU Cores:** {cpu_count}\n"
        f"💿 **Free Disk:** {free_space:.1f}GB\n"
        f"📁 **Temp Files:** {len(temp_files)}\n"
        f"💾 **Temp Size:** {total_size / (1024*1024):.1f}MB\n"
        f"⚡ **Search Engine:** Memory-mapped parallel\n"
        f"🗑 **Auto-delete:** Files older than 1 hour"
    )

@app.on_message(filters.text & filters.private)
async def handle_messages(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        await message.reply("❌ Unauthorized")
        return
    
    user_id = message.from_user.id
    state_data = user_states.get(user_id, {})
    state = state_data.get("state")
    mode = state_data.get("mode", "single")
    
    if state == "awaiting_url":
        user_states[user_id] = {
            "state": "awaiting_search_term",
            "url": message.text.strip(),
            "mode": mode
        }
        
        if mode == "multi_terms":
            await message.reply("🔍 Send multiple search terms (one per line):")
        else:
            await message.reply("🔍 Now send the text to search for:")
    
    elif state == "awaiting_urls":
        # Add URLs from message
        urls = state_data.get("urls", [])
        new_urls = [url.strip() for url in message.text.split('\n') if url.strip()]
        urls.extend(new_urls)
        
        user_states[user_id] = {
            "state": "awaiting_urls",
            "urls": urls,
            "mode": "multi"
        }
        
        await message.reply(
            f"✅ Added {len(new_urls)} URLs (Total: {len(urls)})\n"
            "Send more or type /done to continue"
        )
    
    elif state == "awaiting_search_term":
        search_text = message.text.strip()
        mode = state_data.get("mode", "single")
        
        if mode == "multi":
            # Multiple files, single term
            urls = state_data.get("urls", [])
            user_states.pop(user_id, None)
            await process_multiple_files(client, message, urls, [search_text])
        
        elif mode == "multi_terms":
            # Single file, multiple terms
            url = state_data.get("url")
            search_terms = [term.strip() for term in search_text.split('\n') if term.strip()]
            user_states.pop(user_id, None)
            await process_multiple_terms(client, message, url, search_terms)
        
        else:
            # Single file, single term
            url = state_data.get("url")
            user_states.pop(user_id, None)
            await process_single_search(client, message, url, search_text)

async def process_single_search(client, message, url, search_term):
    """Process single file search"""
    status_msg = await message.reply("⚡ Initializing high-speed search...")
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        download_path = TEMP_DIR / f"download_{message.from_user.id}_{timestamp}.txt"
        output_path = TEMP_DIR / f"results_{message.from_user.id}_{timestamp}.txt"
        
        # Download
        downloader = FastDownloader()
        async def download_progress(downloaded, total):
            percent = (downloaded / total) * 100
            await status_msg.edit_text(
                f"⬇️ **Downloading:** {percent:.1f}%\n"
                f"📦 Size: {downloaded / (1024*1024):.1f}MB / {total / (1024*1024):.1f}MB"
            )
        
        await downloader.download_with_progress(url, download_path, download_progress)
        
        # Search
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
                    # Schedule deletion
                    asyncio.create_task(FileCleaner.schedule_deletion(part_path, 300))
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
        
        # Schedule cleanup
        asyncio.create_task(FileCleaner.schedule_deletion(download_path, 300))
        asyncio.create_task(FileCleaner.schedule_deletion(output_path, 300))
        
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")

async def process_multiple_files(client, message, urls, search_terms):
    """Process multiple files with same search term"""
    status_msg = await message.reply(f"⚡ Processing {len(urls)} files...")
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        downloaded_files = []
        downloader = FastDownloader()
        
        # Download all files
        for i, url in enumerate(urls, 1):
            download_path = TEMP_DIR / f"download_{message.from_user.id}_{timestamp}_{i}.txt"
            
            await status_msg.edit_text(f"⬇️ Downloading file {i}/{len(urls)}...")
            await downloader.download_with_progress(url, download_path)
            downloaded_files.append(download_path)
        
        # Search in all files
        for i, file_path in enumerate(downloaded_files, 1):
            await status_msg.edit_text(f"🔍 Searching file {i}/{len(downloaded_files)}...")
            
            searcher = FastFileSearcher(str(file_path))
            matches_count, matches = await searcher.search_parallel(search_terms[0])
            
            if matches_count > 0:
                output_path = TEMP_DIR / f"results_{message.from_user.id}_{timestamp}_{i}.txt"
                async with aiofiles.open(output_path, 'wb') as f:
                    for match in matches[:50000]:
                        await f.write(match + b'\n')
                
                await message.reply_document(
                    output_path,
                    caption=f"📄 File {i}: Found {matches_count:,} matches for '{search_terms[0]}'"
                )
                
                # Schedule deletion
                asyncio.create_task(FileCleaner.schedule_deletion(output_path, 300))
        
        await status_msg.delete()
        
        # Cleanup downloads
        for file_path in downloaded_files:
            asyncio.create_task(FileCleaner.schedule_deletion(file_path, 300))
        
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")

async def process_multiple_terms(client, message, url, search_terms):
    """Process single file with multiple search terms"""
    status_msg = await message.reply(f"⚡ Processing {len(search_terms)} search terms...")
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        download_path = TEMP_DIR / f"download_{message.from_user.id}_{timestamp}.txt"
        
        # Download file once
        downloader = FastDownloader()
        await status_msg.edit_text("⬇️ Downloading file...")
        await downloader.download_with_progress(url, download_path)
        
        # Search for each term
        searcher = FastFileSearcher(str(download_path))
        
        for i, term in enumerate(search_terms, 1):
            await status_msg.edit_text(f"🔍 Searching term {i}/{len(search_terms)}: '{term}'...")
            
            matches_count, matches = await searcher.search_parallel(term)
            
            if matches_count > 0:
                output_path = TEMP_DIR / f"results_{message.from_user.id}_{timestamp}_term{i}.txt"
                async with aiofiles.open(output_path, 'wb') as f:
                    for match in matches[:50000]:
                        await f.write(match + b'\n')
                
                await message.reply_document(
                    output_path,
                    caption=f"📄 Term '{term}': Found {matches_count:,} matches"
                )
                
                # Schedule deletion
                asyncio.create_task(FileCleaner.schedule_deletion(output_path, 300))
            else:
                await message.reply(f"❌ No matches for '{term}'")
        
        await status_msg.delete()
        
        # Cleanup
        asyncio.create_task(FileCleaner.schedule_deletion(download_path, 300))
        
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")

@app.on_callback_query()
async def handle_callback(client, callback_query):
    user_id = callback_query.from_user.id
    
    if user_id != ALLOWED_USER_ID:
        await callback_query.answer("❌ Unauthorized", show_alert=True)
        return
    
    action = callback_query.data
    
    if action == "single_search":
        user_states[user_id] = {"state": "awaiting_url", "mode": "single"}
        await callback_query.message.reply("📎 Send me the direct download link:")
    
    elif action == "multi_files":
        user_states[user_id] = {"state": "awaiting_urls", "urls": [], "mode": "multi"}
        await callback_query.message.reply(
            "📎 Send multiple download links (one per line):\n"
            "Type /done when finished"
        )
    
    elif action == "multi_terms":
        user_states[user_id] = {"state": "awaiting_url", "mode": "multi_terms"}
        await callback_query.message.reply("📎 Send me the direct download link:")
    
    elif action == "clean_files":
        deleted = 0
        freed_space = 0
        for file in TEMP_DIR.iterdir():
            if file.is_file():
                size = file.stat().st_size
                file.unlink(missing_ok=True)
                deleted += 1
                freed_space += size
        
        await callback_query.message.reply(
            f"🧹 **Cleanup Complete**\n"
            f"🗑 Deleted: {deleted} files\n"
            f"💾 Freed: {freed_space / (1024*1024):.1f}MB"
        )
    
    elif action == "status":
        cpu_count = mp.cpu_count()
        stat = os.statvfs(TEMP_DIR)
        free_space = (stat.f_bavail * stat.f_frsize) / (1024**3)
        
        temp_files = list(TEMP_DIR.iterdir())
        total_size = sum(f.stat().st_size for f in temp_files if f.is_file())
        
        await callback_query.message.reply(
            f"📊 **System Status**\n\n"
            f"🖥 **CPU Cores:** {cpu_count}\n"
            f"💿 **Free Disk:** {free_space:.1f}GB\n"
            f"📁 **Temp Files:** {len(temp_files)}\n"
            f"💾 **Temp Size:** {total_size / (1024*1024):.1f}MB\n"
            f"🗑 **Auto-delete:** 1 hour"
        )
    
    await callback_query.answer()

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

async def start_cleanup_task():
    """Start background cleanup task"""
    asyncio.create_task(FileCleaner.cleanup_old_files())

if __name__ == "__main__":
    print("⚡ High-Performance File Search Bot starting...")
    print(f"CPU Cores: {mp.cpu_count()}")
    print(f"Auto-delete: Files older than {MAX_FILE_AGE/60:.0f} minutes")
    
    # Start cleanup task
    loop = asyncio.get_event_loop()
    loop.create_task(FileCleaner.cleanup_old_files())
    
    app.run()
