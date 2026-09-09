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
import json
import time

# Configuration from Railway
API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ALLOWED_USER_ID = int(os.environ.get("ALLOWED_USER_ID", "0"))

# Performance settings
CHUNK_SIZE = 128 * 1024 * 1024  # 128MB chunks for better performance
MAX_WORKERS = min(mp.cpu_count(), 8)  # Limit to avoid overload
READ_BUFFER = 2 * 1024 * 1024  # 2MB buffer
MAX_FILE_AGE = 3600  # 1 hour in seconds
MAX_RESULT_LINES = 100000  # Max lines per result file

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
search_progress = {}  # Track progress for each user

class FileCleaner:
    @staticmethod
    async def cleanup_old_files():
        """Auto-delete old files"""
        while True:
            try:
                current_time = datetime.now().timestamp()
                for file in TEMP_DIR.iterdir():
                    if file.is_file():
                        file_age = current_time - file.stat().st_mtime
                        if file_age > MAX_FILE_AGE:
                            file.unlink(missing_ok=True)
                            print(f"🗑 Auto-deleted: {file.name}")
            except Exception as e:
                print(f"Cleanup error: {e}")
            await asyncio.sleep(300)  # Every 5 minutes
    
    @staticmethod
    async def schedule_deletion(file_path: Path, delay_seconds: int = 600):
        """Schedule file deletion"""
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
        
        try:
            with open(self.file_path, 'rb') as f:
                # Use memory mapping for large files
                mm = mmap.mmap(f.fileno(), chunk_size, offset=chunk_start, access=mmap.ACCESS_READ)
                try:
                    pos = 0
                    match_count = 0
                    while True:
                        pos = mm.find(search_bytes, pos)
                        if pos == -1:
                            break
                        
                        # Find line boundaries
                        line_start = mm.rfind(b'\n', 0, pos) + 1
                        line_end = mm.find(b'\n', pos)
                        if line_end == -1:
                            line_end = chunk_size
                        
                        line = mm[line_start:line_end]
                        matches.append(line)
                        match_count += 1
                        pos = line_end + 1
                        
                        # Limit matches per chunk to avoid memory issues
                        if match_count >= 50000:
                            break
                finally:
                    mm.close()
        except Exception as e:
            print(f"Search error in chunk: {e}")
        
        return len(matches), matches
    
    async def search_parallel(self, search_term: str, progress_callback=None) -> Tuple[int, List[bytes]]:
        all_matches = []
        total_matches = 0
        
        chunk_tasks = []
        for i in range(self.num_chunks):
            chunk_start = i * CHUNK_SIZE
            chunk_size = min(CHUNK_SIZE, self.file_size - chunk_start)
            chunk_tasks.append((chunk_start, chunk_size, search_term))
        
        # Process chunks in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_WORKERS, self.num_chunks)) as executor:
            futures = [executor.submit(self.search_chunk, task) for task in chunk_tasks]
            
            completed = 0
            for future in concurrent.futures.as_completed(futures):
                matches_count, matches = future.result()
                total_matches += matches_count
                all_matches.extend(matches)
                completed += 1
                
                if progress_callback:
                    await progress_callback(completed, self.num_chunks)
        
        return total_matches, all_matches

class FastDownloader:
    @staticmethod
    async def download_with_progress(url: str, dest_path: Path, progress_callback=None):
        connector = aiohttp.TCPConnector(
            limit=10,
            limit_per_host=5,
            ttl_dns_cache=300,
            use_dns_cache=True,
            force_close=True
        )
        
        timeout = aiohttp.ClientTimeout(total=3600, connect=60, sock_read=120)
        
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
                            if percent - last_update >= 5:  # Update every 5%
                                await progress_callback(downloaded, total_size)
                                last_update = percent

@app.on_message(filters.command("start"))
async def start_command(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        await message.reply("❌ Unauthorized")
        return
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 Single Search", callback_data="single")],
        [InlineKeyboardButton("📁 Multi Files + Multi Terms", callback_data="multi_both")],
        [InlineKeyboardButton("🧹 Clean Files", callback_data="clean")],
        [InlineKeyboardButton("📊 Status", callback_data="status")]
    ])
    
    await message.reply(
        "⚡ **Ultimate File Search Bot**\n\n"
        "**Features:**\n"
        "• Multiple files search\n"
        "• Multiple terms search\n"
        "• Combined multi-file + multi-term\n"
        "• Memory-mapped parallel engine\n"
        "• Auto-delete old files\n"
        "• Live progress updates\n\n"
        "**Commands:**\n"
        "/search - Single search\n"
        "/multisearch - Multi files + terms\n"
        "/clean - Delete temp files\n"
        "/status - System status\n\n"
        "Or use buttons below:",
        reply_markup=keyboard
    )

@app.on_message(filters.command("search"))
async def search_command(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        await message.reply("❌ Unauthorized")
        return
    
    user_states[message.from_user.id] = {
        "state": "awaiting_url",
        "mode": "single"
    }
    await message.reply("📎 Send me the direct download link:")

@app.on_message(filters.command("multisearch"))
async def multisearch_command(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        await message.reply("❌ Unauthorized")
        return
    
    user_states[message.from_user.id] = {
        "state": "awaiting_urls",
        "urls": [],
        "mode": "multi_both"
    }
    
    await message.reply(
        "📎 **MULTI-FILE + MULTI-TERM SEARCH**\n\n"
        "**Step 1:** Send download links (one per line)\n\n"
        "Example:\n"
        "https://example.com/file1.txt\n"
        "https://example.com/file2.txt\n"
        "https://example.com/file3.txt\n\n"
        "Type /done when finished"
    )

@app.on_message(filters.command("done"))
async def done_command(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        await message.reply("❌ Unauthorized")
        return
    
    user_id = message.from_user.id
    state_data = user_states.get(user_id, {})
    
    # Check if we're in URL collection mode
    if state_data.get("state") == "awaiting_urls":
        urls = state_data.get("urls", [])
        
        if urls:
            # Move to terms collection
            user_states[user_id] = {
                "state": "awaiting_terms",
                "urls": urls,
                "terms": [],
                "mode": "multi_both"
            }
            
            await message.reply(
                f"✅ Received {len(urls)} files\n\n"
                "**Step 2:** Send search terms (one per line)\n\n"
                "Example:\n"
                "password\n"
                "admin\n"
                "email\n"
                "api_key\n\n"
                "Type /done when finished"
            )
        else:
            await message.reply("❌ No URLs received. Send links first.")
    
    # Check if we're in terms collection mode
    elif state_data.get("state") == "awaiting_terms":
        terms = state_data.get("terms", [])
        urls = state_data.get("urls", [])
        
        if terms:
            # START THE SEARCH!
            total_searches = len(urls) * len(terms)
            
            await message.reply(
                f"✅ Received {len(terms)} search terms\n\n"
                f"📁 Files: {len(urls)}\n"
                f"🔍 Terms: {len(terms)}\n"
                f"🔢 Total searches: {len(urls)} × {len(terms)} = {total_searches}\n\n"
                "⚡ Starting search... This may take a while.\n"
                "📊 Progress will be shown live."
            )
            
            # Clear the state before processing
            user_states.pop(user_id, None)
            
            # Start the multi-search
            await process_multi_search(client, message, urls, terms)
        else:
            await message.reply("❌ No search terms received. Send terms first.")
    
    else:
        await message.reply(
            "ℹ️ No pending operation.\n\n"
            "Use /multisearch to start a new multi-file search."
        )

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

@app.on_message(filters.command("status"))
async def status_command(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        await message.reply("❌ Unauthorized")
        return
    
    cpu_count = mp.cpu_count()
    stat = os.statvfs(TEMP_DIR)
    free_space = (stat.f_bavail * stat.f_frsize) / (1024**3)
    
    temp_files = list(TEMP_DIR.iterdir())
    total_size = sum(f.stat().st_size for f in temp_files if f.is_file())
    
    await message.reply(
        f"📊 **System Status**\n\n"
        f"🖥 **CPU Cores:** {cpu_count}\n"
        f"💿 **Free Disk:** {free_space:.1f}GB\n"
        f"📁 **Temp Files:** {len(temp_files)}\n"
        f"💾 **Temp Size:** {total_size / (1024*1024):.1f}MB\n"
        f"⚡ **Engine:** Memory-mapped parallel\n"
        f"🗑 **Auto-delete:** 1 hour\n"
        f"📦 **Chunk Size:** 128MB\n"
        f"🔢 **Max Workers:** {MAX_WORKERS}"
    )

@app.on_message(filters.text & filters.private)
async def handle_messages(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        await message.reply("❌ Unauthorized")
        return
    
    # Don't process if it's a command
    if message.text.startswith('/'):
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
        await message.reply("🔍 Now send the search term:")
    
    elif state == "awaiting_urls":
        # Add URLs
        urls = state_data.get("urls", [])
        new_urls = [url.strip() for url in message.text.split('\n') if url.strip()]
        urls.extend(new_urls)
        
        user_states[user_id] = {
            "state": "awaiting_urls",
            "urls": urls,
            "mode": mode
        }
        
        await message.reply(
            f"✅ Added {len(new_urls)} URLs (Total: {len(urls)})\n"
            "Send more or type /done to continue"
        )
    
    elif state == "awaiting_terms":
        # Add search terms
        terms = state_data.get("terms", [])
        new_terms = [term.strip() for term in message.text.split('\n') if term.strip()]
        terms.extend(new_terms)
        
        user_states[user_id] = {
            "state": "awaiting_terms",
            "urls": state_data.get("urls", []),
            "terms": terms,
            "mode": mode
        }
        
        await message.reply(
            f"✅ Added {len(new_terms)} terms (Total: {len(terms)})\n"
            "Send more or type /done to start search"
        )
    
    elif state == "awaiting_search_term":
        search_term = message.text.strip()
        
        if mode == "single":
            url = state_data.get("url")
            user_states.pop(user_id, None)
            await process_single_search(client, message, url, search_term)
        
        elif mode == "multi_both":
            terms = state_data.get("terms", [search_term])
            urls = state_data.get("urls", [])
            user_states.pop(user_id, None)
            await process_multi_search(client, message, urls, terms)

async def process_single_search(client, message, url, search_term):
    """Single file, single term"""
    status_msg = await message.reply("⚡ Starting search...")
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        download_path = TEMP_DIR / f"download_{message.from_user.id}_{timestamp}.txt"
        output_path = TEMP_DIR / f"results_{message.from_user.id}_{timestamp}.txt"
        
        # Download
        downloader = FastDownloader()
        async def download_progress(downloaded, total):
            percent = (downloaded / total) * 100
            try:
                await status_msg.edit_text(
                    f"⬇️ **Downloading:** {percent:.1f}%\n"
                    f"📦 {downloaded / (1024*1024):.1f}MB / {total / (1024*1024):.1f}MB"
                )
            except:
                pass
        
        await downloader.download_with_progress(url, download_path, download_progress)
        
        # Search
        await status_msg.edit_text("🔍 Searching...")
        searcher = FastFileSearcher(str(download_path))
        
        async def search_progress(completed, total):
            try:
                await status_msg.edit_text(
                    f"🔍 **Searching...**\n"
                    f"📊 Progress: {completed}/{total} chunks"
                )
            except:
                pass
        
        start_time = datetime.now()
        matches_count, matches = await searcher.search_parallel(search_term, search_progress)
        search_time = (datetime.now() - start_time).total_seconds()
        
        if matches_count > 0:
            async with aiofiles.open(output_path, 'wb') as f:
                for match in matches[:MAX_RESULT_LINES]:
                    await f.write(match + b'\n')
            
            # Send in smaller chunks if file is large
            file_size = os.path.getsize(output_path)
            if file_size > 40 * 1024 * 1024:  # >40MB
                parts = split_file(output_path, 40 * 1024 * 1024)
                for part_idx, part_path in enumerate(parts, 1):
                    await message.reply_document(
                        part_path,
                        caption=f"📄 Part {part_idx}/{len(parts)} | {matches_count:,} matches\n⚡ {search_time:.2f}s"
                    )
                    asyncio.create_task(FileCleaner.schedule_deletion(part_path, 300))
            else:
                await message.reply_document(
                    output_path,
                    caption=f"📄 Found {matches_count:,} matches\n⚡ {search_time:.2f}s"
                )
            
            asyncio.create_task(FileCleaner.schedule_deletion(output_path, 300))
        else:
            await status_msg.edit_text(f"❌ No matches for '{search_term}'")
        
        asyncio.create_task(FileCleaner.schedule_deletion(download_path, 300))
        await status_msg.delete()
        
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")

async def process_multi_search(client, message, urls, terms):
    """Multiple files + Multiple terms with live progress"""
    total_files = len(urls)
    total_terms = len(terms)
    total_searches = total_files * total_terms
    
    # Create a unique progress message
    progress_msg = await message.reply(
        f"⚡ **MULTI-SEARCH STARTED**\n\n"
        f"📁 Files: {total_files}\n"
        f"🔍 Terms: {total_terms}\n"
        f"🔢 Total searches: {total_searches}\n\n"
        f"⬇️ Downloading files..."
    )
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        downloaded_files = []
        downloader = FastDownloader()
        
        # Download all files with progress
        for i, url in enumerate(urls, 1):
            download_path = TEMP_DIR / f"download_{message.from_user.id}_{timestamp}_file{i}.txt"
            
            await progress_msg.edit_text(
                f"⬇️ **Downloading file {i}/{total_files}**\n"
                f"📊 Progress: {i}/{total_files}\n"
                f"📁 File: {url[:50]}..."
            )
            
            await downloader.download_with_progress(url, download_path)
            downloaded_files.append((i, download_path))
            
            # Schedule deletion
            asyncio.create_task(FileCleaner.schedule_deletion(download_path, 1800))
        
        # Search all files for all terms
        search_count = 0
        total_matches_all = 0
        
        for file_idx, file_path in downloaded_files:
            searcher = FastFileSearcher(str(file_path))
            
            for term_idx, term in enumerate(terms, 1):
                search_count += 1
                
                # Update progress
                await progress_msg.edit_text(
                    f"🔍 **Searching...**\n\n"
                    f"📁 File: {file_idx}/{total_files}\n"
                    f"🔍 Term: '{term}'\n"
                    f"📊 Overall: {search_count}/{total_searches}\n"
                    f"📈 Term: {term_idx}/{total_terms}\n"
                    f"⏳ Please wait..."
                )
                
                matches_count, matches = await searcher.search_parallel(term)
                total_matches_all += matches_count
                
                if matches_count > 0:
                    # Create output file
                    safe_term = term.replace('/', '_').replace('\\', '_').replace(':', '_')[:30]
                    output_path = TEMP_DIR / f"results_{message.from_user.id}_{timestamp}_file{file_idx}_term_{safe_term}.txt"
                    
                    # Write header
                    header = f"=== File {file_idx}: {urls[file_idx-1]} ===\n"
                    header += f"=== Search Term: {term} ===\n"
                    header += f"=== Matches: {matches_count} ===\n\n"
                    
                    try:
                        async with aiofiles.open(output_path, 'wb') as f:
                            await f.write(header.encode('utf-8'))
                            for match in matches[:MAX_RESULT_LINES]:
                                await f.write(match + b'\n')
                        
                        file_size = os.path.getsize(output_path)
                        
                        # Send file in parts if too large
                        if file_size > 40 * 1024 * 1024:  # >40MB
                            parts = split_file(output_path, 40 * 1024 * 1024)
                            for part_idx, part_path in enumerate(parts, 1):
                                try:
                                    await message.reply_document(
                                        part_path,
                                        caption=f"📄 File {file_idx} | '{term}' | Part {part_idx}/{len(parts)} | {matches_count:,} matches"
                                    )
                                except Exception as e:
                                    await message.reply(f"⚠️ Error sending part {part_idx}: {str(e)}")
                                asyncio.create_task(FileCleaner.schedule_deletion(part_path, 300))
                        else:
                            try:
                                await message.reply_document(
                                    output_path,
                                    caption=f"📄 File {file_idx} | '{term}' | {matches_count:,} matches"
                                )
                            except Exception as e:
                                await message.reply(f"⚠️ Error sending results for '{term}': {str(e)}")
                        
                        asyncio.create_task(FileCleaner.schedule_deletion(output_path, 600))
                    except Exception as e:
                        await message.reply(f"⚠️ Error creating results for '{term}': {str(e)}")
                else:
                    try:
                        await message.reply(f"❌ File {file_idx} | '{term}': No matches")
                    except:
                        pass
                
                # Update progress after each term
                await progress_msg.edit_text(
                    f"🔍 **Searching...**\n\n"
                    f"📁 File: {file_idx}/{total_files}\n"
                    f"🔍 Term: '{term}' - ✅ Done\n"
                    f"📊 Overall: {search_count}/{total_searches}\n"
                    f"📈 Found: {matches_count:,} matches"
                )
        
        # Final summary
        await progress_msg.edit_text(
            f"✅ **SEARCH COMPLETE**\n\n"
            f"📁 Files processed: {total_files}\n"
            f"🔍 Terms searched: {total_terms}\n"
            f"🔢 Total searches: {total_searches}\n"
            f"📊 Total matches: {total_matches_all:,}\n\n"
            f"🗑 Files will auto-delete in 30 minutes\n"
            f"📦 All results sent successfully!"
        )
        
    except Exception as e:
        error_msg = f"❌ Error: {str(e)}"
        try:
            await progress_msg.edit_text(error_msg)
        except:
            await message.reply(error_msg)

@app.on_callback_query()
async def handle_callback(client, callback_query):
    user_id = callback_query.from_user.id
    
    if user_id != ALLOWED_USER_ID:
        await callback_query.answer("❌ Unauthorized", show_alert=True)
        return
    
    action = callback_query.data
    
    if action == "single":
        user_states[user_id] = {"state": "awaiting_url", "mode": "single"}
        await callback_query.message.reply("📎 Send me the direct download link:")
    
    elif action == "multi_both":
        user_states[user_id] = {
            "state": "awaiting_urls",
            "urls": [],
            "mode": "multi_both"
        }
        await callback_query.message.reply(
            "📎 Send download links (one per line):\n"
            "Type /done when finished"
        )
    
    elif action == "clean":
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
            f"🗑 **Auto-delete:** 1 hour\n"
            f"📦 **Max File Size:** Unlimited\n"
            f"📊 **Max Results per file:** {MAX_RESULT_LINES:,}"
        )
    
    await callback_query.answer()

def split_file(file_path: Path, max_size: int) -> List[Path]:
    """Split large file into smaller parts"""
    parts = []
    file_size = os.path.getsize(file_path)
    num_parts = (file_size + max_size - 1) // max_size
    
    try:
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
    except Exception as e:
        print(f"Error splitting file: {e}")
    
    return parts

if __name__ == "__main__":
    print("⚡ Ultimate Multi-File Search Bot starting...")
    print(f"CPU Cores: {mp.cpu_count()}")
    print(f"Max Workers: {MAX_WORKERS}")
    print(f"Chunk Size: {CHUNK_SIZE / (1024*1024):.0f}MB")
    print(f"Auto-delete: Files older than {MAX_FILE_AGE/60:.0f} minutes")
    
    # Start cleanup task
    loop = asyncio.get_event_loop()
    loop.create_task(FileCleaner.cleanup_old_files())
    
    app.run()
