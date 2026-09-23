import asyncio
import os
import mmap
import hashlib
import concurrent.futures
import aiohttp
import aiofiles
import zipfile
import shutil
from datetime import datetime
from pathlib import Path
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
import multiprocessing as mp
from typing import List, Tuple, Dict, Optional
import json
import re
import uuid

# ================= CONFIG (Railway Environment Variables) =================
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ALLOWED_USER_ID = int(os.environ.get("ALLOWED_USER_ID", "0"))

# Detect Railway persistent volume (mount at /data)
BASE_DIR = Path(
    os.environ.get("RAILWAY_VOLUME_MOUNT_PATH")
    or os.environ.get("DATA_DIR")
    or "/tmp/file_search"
)
BASE_DIR.mkdir(parents=True, exist_ok=True)

FILES_DIR   = BASE_DIR / "files"
RESULTS_DIR = BASE_DIR / "results"
ZIP_DIR     = BASE_DIR / "zips"
META_DIR    = BASE_DIR / "meta"
for d in (FILES_DIR, RESULTS_DIR, ZIP_DIR, META_DIR):
    d.mkdir(exist_ok=True)

REGISTRY_FILE  = META_DIR / "registry.json"
HISTORY_FILE   = META_DIR / "history.json"
BOOKMARKS_FILE = META_DIR / "bookmarks.json"

# ================= PERFORMANCE =================
CHUNK_SIZE     = 128 * 1024 * 1024   # 128 MB
MAX_WORKERS    = min(mp.cpu_count(), 8)
READ_BUFFER    = 2 * 1024 * 1024
MAX_RESULT_LINES = 100000
ZIP_SPLIT_SIZE = 45 * 1024 * 1024

# ================= APP =================
app = Client(
    "file_search_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=MAX_WORKERS * 2,
    workdir=str(BASE_DIR),   # session file stored on persistent volume
)

user_states: Dict[int, dict] = {}

# ================= REGISTRY (PERSISTENT JSON STORAGE) =================
class Registry:
    @staticmethod
    def _load(path: Path) -> dict:
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    @staticmethod
    def _save(path: Path, data: dict):
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        tmp.replace(path)

    @staticmethod
    def all_files() -> dict:
        return Registry._load(REGISTRY_FILE)

    @staticmethod
    def add_file(file_id: str, meta: dict):
        data = Registry._load(REGISTRY_FILE)
        data[file_id] = meta
        Registry._save(REGISTRY_FILE, data)

    @staticmethod
    def get_file(file_id: str) -> Optional[dict]:
        return Registry._load(REGISTRY_FILE).get(file_id)

    @staticmethod
    def find_by_url(url: str) -> Optional[Tuple[str, dict]]:
        for fid, meta in Registry._load(REGISTRY_FILE).items():
            if meta.get("url") == url:
                return fid, meta
        return None

    @staticmethod
    def find_by_name(name: str) -> Optional[Tuple[str, dict]]:
        for fid, meta in Registry._load(REGISTRY_FILE).items():
            if meta.get("name", "").lower() == name.lower():
                return fid, meta
        return None

    @staticmethod
    def add_history(entry: dict):
        data = Registry._load(HISTORY_FILE)
        entry["id"] = str(uuid.uuid4())[:8]
        entry["time"] = datetime.now().isoformat()
        data[entry["id"]] = entry
        if len(data) > 500:
            for k in list(data.keys())[:-500]:
                del data[k]
        Registry._save(HISTORY_FILE, data)
        return entry["id"]

    @staticmethod
    def all_history() -> dict:
        return Registry._load(HISTORY_FILE)

    @staticmethod
    def add_bookmark(result_id: str, meta: dict):
        data = Registry._load(BOOKMARKS_FILE)
        data[result_id] = meta
        Registry._save(BOOKMARKS_FILE, data)

    @staticmethod
    def all_bookmarks() -> dict:
        return Registry._load(BOOKMARKS_FILE)

# ================= FAST SEARCHER =================
class FastFileSearcher:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.file_size = os.path.getsize(file_path)
        self.num_chunks = max(1, (self.file_size + CHUNK_SIZE - 1) // CHUNK_SIZE)

    def search_chunk(self, chunk_info: Tuple[int, int, str]) -> Tuple[int, List[bytes]]:
        chunk_start, chunk_size, term = chunk_info
        matches = []
        needle = term.encode("utf-8", errors="ignore")
        try:
            with open(self.file_path, "rb") as f:
                mm = mmap.mmap(f.fileno(), chunk_size, offset=chunk_start, access=mmap.ACCESS_READ)
                try:
                    pos, count = 0, 0
                    while True:
                        pos = mm.find(needle, pos)
                        if pos == -1:
                            break
                        ls = mm.rfind(b"\n", 0, pos) + 1
                        le = mm.find(b"\n", pos)
                        if le == -1:
                            le = chunk_size
                        line = mm[ls:le]
                        if len(line) < 5000:
                            matches.append(line)
                            count += 1
                        pos = le + 1
                        if count >= 50000:
                            break
                finally:
                    mm.close()
        except Exception as e:
            print(f"[chunk err] {e}")
        return len(matches), matches

    async def search_parallel(self, term: str, progress_cb=None) -> Tuple[int, List[bytes]]:
        all_matches, total = [], 0
        tasks = []
        for i in range(self.num_chunks):
            start = i * CHUNK_SIZE
            size = min(CHUNK_SIZE, self.file_size - start)
            tasks.append((start, size, term))

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_WORKERS, self.num_chunks)) as ex:
            futures = [ex.submit(self.search_chunk, t) for t in tasks]
            done = 0
            for fut in concurrent.futures.as_completed(futures):
                c, m = fut.result()
                total += c
                all_matches.extend(m)
                done += 1
                if progress_cb:
                    try:
                        await progress_cb(done, self.num_chunks)
                    except Exception:
                        pass
        return total, all_matches

# ================= DOWNLOADER =================
class FastDownloader:
    @staticmethod
    async def download(url: str, dest: Path, progress_cb=None):
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=5, ttl_dns_cache=300,
                                         use_dns_cache=True, force_close=True)
        timeout = aiohttp.ClientTimeout(total=7200, connect=60, sock_read=180)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as s:
            async with s.get(url, allow_redirects=True) as r:
                if r.status != 200:
                    raise Exception(f"HTTP {r.status}")
                total_size = int(r.headers.get("content-length", 0))
                downloaded, last = 0, 0
                async with aiofiles.open(dest, "wb", buffering=READ_BUFFER) as f:
                    async for chunk in r.content.iter_chunked(READ_BUFFER):
                        await f.write(chunk)
                        downloaded += len(chunk)
                        if progress_cb and total_size > 0:
                            pct = (downloaded / total_size) * 100
                            if pct - last >= 5:
                                await progress_cb(downloaded, total_size)
                                last = pct
        return dest

# ================= HELPERS =================
def human_size(b: int) -> str:
    for u in ["B", "KB", "MB", "GB", "TB"]:
        if b < 1024:
            return f"{b:.2f}{u}"
        b /= 1024
    return f"{b:.2f}PB"

def file_id_from_url(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:16]

def split_file_into_zips(files: List[Path], zip_name: str) -> List[Path]:
    parts, current_zip, current_size, idx = [], None, 0, 1
    def open_new():
        nonlocal current_zip, current_size, idx
        path = ZIP_DIR / f"{zip_name}_part{idx}.zip"
        current_zip = zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED, compresslevel=6)
        parts.append(path)
        current_size = 0
        idx += 1
    open_new()
    for f in files:
        if current_size + f.stat().st_size > ZIP_SPLIT_SIZE:
            current_zip.close()
            open_new()
        current_zip.write(f, arcname=f.name)
        current_size += f.stat().st_size
    if current_zip:
        current_zip.close()
    return parts

async def download_or_reuse(url: str, idx: int, status_msg) -> Tuple[Path, str]:
    existing = Registry.find_by_url(url)
    if existing:
        fid, meta = existing
        if Path(meta["path"]).exists():
            return Path(meta["path"]), fid

    fid = file_id_from_url(url) + f"_{idx}"
    dest = FILES_DIR / f"{fid}.bin"

    async def prog(done, total):
        try:
            await status_msg.edit_text(
                f"⬇️ File {idx}: {(done/total)*100:.1f}%\n"
                f"📦 {human_size(done)} / {human_size(total)}"
            )
        except Exception:
            pass

    await FastDownloader.download(url, dest, prog)
    Registry.add_file(fid, {
        "url": url, "name": dest.name, "path": str(dest),
        "size": dest.stat().st_size, "downloaded": datetime.now().isoformat(),
        "searches": 0,
    })
    return dest, fid

# ================= SEARCH RUNNER =================
async def run_search_on_files(client, message, entries: List[Tuple[int, Path, str]], terms: List[str]):
    total_files, total_terms = len(entries), len(terms)
    total_searches = total_files * total_terms
    status = await message.reply("⚡ Starting...")
    all_results, total_matches = [], 0
    run_id = str(uuid.uuid4())[:6]

    try:
        counter = 0
        for (fidx, fpath, fid) in entries:
            searcher = FastFileSearcher(str(fpath))
            for term in terms:
                counter += 1
                try:
                    await status.edit_text(
                        f"🔍 Searching\n📁 {fidx}/{total_files}\n"
                        f"🔍 `{term}`\n📊 {counter}/{total_searches}"
                    )
                except Exception:
                    pass

                count, matches = await searcher.search_parallel(term)
                total_matches += count

                if count > 0:
                    safe = re.sub(r"[^A-Za-z0-9._-]", "_", term)[:30]
                    out = RESULTS_DIR / f"r_{run_id}_f{fidx}_{safe}.txt"
                    async with aiofiles.open(out, "wb") as f:
                        header = (f"=== File: {fid} ===\n=== Term: {term} ===\n"
                                  f"=== Matches: {count} ===\n\n").encode()
                        await f.write(header)
                        for m in matches[:MAX_RESULT_LINES]:
                            await f.write(m + b"\n")
                    all_results.append(out)
                    try:
                        await message.reply_document(out,
                            caption=f"📄 File {fidx} | `{term}` | {count:,} matches")
                    except Exception as e:
                        await message.reply(f"⚠️ Send failed for `{term}`: {e} (in ZIP)")

        if all_results:
            try:
                await status.edit_text("🗜 Creating ZIP...")
            except Exception:
                pass
            zips = split_file_into_zips(all_results, f"search_{run_id}")
            for i, z in enumerate(zips, 1):
                await message.reply_document(z, caption=f"🗜 ZIP Part {i}/{len(zips)}")

        Registry.add_history({
            "terms": terms, "files": total_files,
            "matches": total_matches,
            "result_paths": [str(p) for p in all_results],
        })

        try:
            await status.edit_text(
                f"✅ Complete\n📁 {total_files} files\n🔍 {total_terms} terms\n"
                f"💥 {total_matches:,} matches\n📦 {len(all_results)} result files"
            )
        except Exception:
            pass

    except Exception as e:
        try:
            await status.edit_text(f"❌ Error: {e}")
        except Exception:
            await message.reply(f"❌ Error: {e}")

# ================= COMMANDS =================
@app.on_message(filters.command("start"))
async def cmd_start(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        return await message.reply("❌ Unauthorized")
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 New Search (URL)", callback_data="single"),
         InlineKeyboardButton("📁 Multi Files + Terms", callback_data="multi_both")],
        [InlineKeyboardButton("🗂 Saved Files", callback_data="list_files"),
         InlineKeyboardButton("♻️ Recheck Saved", callback_data="recheck_menu")],
        [InlineKeyboardButton("📜 History", callback_data="history"),
         InlineKeyboardButton("🧹 Clean Results", callback_data="clean")],
        [InlineKeyboardButton("📊 Status", callback_data="status")],
    ])
    await message.reply(
        "⚡ **Persistent File Search Bot**\n\n"
        "• Files saved forever (no auto-delete)\n"
        "• Re-search saved files instantly (no redownload)\n"
        "• Results delivered as ZIP\n\n"
        "**Commands:**\n"
        "/search — Single URL search\n"
        "/multisearch — Multi files + multi terms\n"
        "/files — List saved files\n"
        "/recheck — Re-search a saved file\n"
        "/allfiles — Search ALL saved files\n"
        "/history — View search history\n"
        "/clean — Clean results\n"
        "/status — System status",
        reply_markup=kb
    )

@app.on_message(filters.command("search"))
async def cmd_search(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        return await message.reply("❌ Unauthorized")
    user_states[message.from_user.id] = {"state": "awaiting_url", "mode": "single"}
    await message.reply("📎 Send URL (or saved file id / name):")

@app.on_message(filters.command("multisearch"))
async def cmd_multisearch(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        return await message.reply("❌ Unauthorized")
    user_states[message.from_user.id] = {"state": "awaiting_urls", "urls": [], "mode": "multi_both"}
    await message.reply("📎 Send URLs (one per line). /done when finished.\n"
                        "Already-saved URLs are reused automatically.")

@app.on_message(filters.command("files"))
async def cmd_files(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        return await message.reply("❌ Unauthorized")
    files = Registry.all_files()
    if not files:
        return await message.reply("📂 No saved files.")
    lines = [f"🗂 **Saved Files ({len(files)})**\n"]
    for fid, m in files.items():
        ok = "✅" if Path(m["path"]).exists() else "❌"
        lines.append(f"{ok} `{fid}` | {m.get('name','?')} | {human_size(m.get('size',0))}")
    lines.append("\n💡 Reuse: `/recheck <id>`")
    await message.reply("\n".join(lines))

@app.on_message(filters.command("recheck"))
async def cmd_recheck(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        return await message.reply("❌ Unauthorized")
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        files = Registry.all_files()
        if not files:
            return await message.reply("📂 No saved files.")
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"📄 {fid} — {m['name'][:20]}", callback_data=f"recheck:{fid}")]
            for fid, m in list(files.items())[:20]
        ])
        return await message.reply("♻️ Pick a file:", reply_markup=kb)
    fid = parts[1].strip()
    meta = Registry.get_file(fid)
    if not meta:
        return await message.reply(f"❌ File `{fid}` not found.")
    user_states[message.from_user.id] = {
        "state": "awaiting_terms_for_recheck", "file_ids": [fid],
        "terms": [], "mode": "recheck"
    }
    await message.reply(f"♻️ Recheck `{fid}` — {meta['name']}\nSend terms, then /done")

@app.on_message(filters.command("allfiles"))
async def cmd_allfiles(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        return await message.reply("❌ Unauthorized")
    files = Registry.all_files()
    if not files:
        return await message.reply("📂 No saved files.")
    user_states[message.from_user.id] = {
        "state": "awaiting_terms_allfiles", "file_ids": list(files.keys()),
        "terms": [], "mode": "allfiles"
    }
    await message.reply(f"🌐 Search across ALL {len(files)} saved files.\nSend terms, then /done")

@app.on_message(filters.command("history"))
async def cmd_history(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        return await message.reply("❌ Unauthorized")
    hist = Registry.all_history()
    if not hist:
        return await message.reply("📜 No history.")
    lines = [f"📜 **Recent ({len(hist)})**\n"]
    for rid, h in list(hist.items())[-15:][::-1]:
        lines.append(f"`{rid}` | {h['time'][:19]} | {h.get('matches',0):,} matches")
    await message.reply("\n".join(lines))

@app.on_message(filters.command("clean"))
async def cmd_clean(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        return await message.reply("❌ Unauthorized")
    deleted = freed = 0
    for d in (RESULTS_DIR, ZIP_DIR):
        for f in d.iterdir():
            if f.is_file():
                freed += f.stat().st_size
                f.unlink(missing_ok=True)
                deleted += 1
    await message.reply(f"🧹 Cleaned {deleted} files ({human_size(freed)})\n"
                        f"✅ Source files preserved.")

@app.on_message(filters.command("status"))
async def cmd_status(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        return await message.reply("❌ Unauthorized")
    files = Registry.all_files()
    total = sum(Path(m["path"]).stat().st_size for m in files.values() if Path(m["path"]).exists())
    stat = os.statvfs(BASE_DIR)
    free = stat.f_bavail * stat.f_frsize
    await message.reply(
        f"📊 **Status**\n"
        f"🗂 Saved: {len(files)} ({human_size(total)})\n"
        f"📜 History: {len(Registry.all_history())}\n"
        f"💿 Free: {human_size(free)}\n"
        f"📂 Data dir: `{BASE_DIR}`\n"
        f"🗑 Auto-delete: OFF"
    )

# =====================================================================
# IMPORTANT: /done handler MUST be defined BEFORE the generic text handler
# otherwise the generic handler intercepts the /done command and stops it.
# =====================================================================
@app.on_message(filters.command("done"))
async def cmd_done(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        return await message.reply("❌ Unauthorized")
    uid = message.from_user.id
    sd = user_states.get(uid, {})
    state = sd.get("state")

    if state == "awaiting_urls":
        urls = sd.get("urls", [])
        if not urls:
            return await message.reply("❌ No URLs.")
        user_states[uid] = {"state": "awaiting_terms", "urls": urls, "terms": [], "mode": "multi_both"}
        return await message.reply(f"✅ {len(urls)} files.\n\nSend search terms (one per line), /done when finished.")

    if state == "awaiting_terms":
        terms = sd.get("terms", [])
        urls = sd.get("urls", [])
        if not terms:
            return await message.reply("❌ No terms.")
        user_states.pop(uid, None)
        return await process_multi(client, message, urls, terms)

    if state in ("awaiting_terms_for_recheck", "awaiting_terms_allfiles"):
        terms = sd.get("terms", [])
        file_ids = sd.get("file_ids", [])
        if not terms:
            return await message.reply("❌ No terms.")
        user_states.pop(uid, None)
        return await process_saved_batch(client, message, file_ids, terms)

    await message.reply("ℹ️ Nothing pending.")

# ================= MESSAGE HANDLER (Generic Text) =================
@app.on_message(filters.text & filters.private)
async def handle_text(client: Client, message: Message):
    if message.from_user.id != ALLOWED_USER_ID:
        return await message.reply("❌ Unauthorized")
    
    # Ignore any command messages that were not caught by command handlers
    if message.text.startswith("/"):
        return

    uid = message.from_user.id
    sd = user_states.get(uid, {})
    state = sd.get("state")
    mode = sd.get("mode", "single")

    if state == "awaiting_url":
        raw = message.text.strip()
        reuse = Registry.get_file(raw)
        if reuse:
            user_states[uid] = {"state": "awaiting_search_term", "file_id": raw, "mode": "single_saved"}
            return await message.reply(f"♻️ Reusing `{raw}`. Send search term:")
        reuse_by_name = Registry.find_by_name(raw)
        if reuse_by_name:
            fid, meta = reuse_by_name
            user_states[uid] = {"state": "awaiting_search_term", "file_id": fid, "mode": "single_saved"}
            return await message.reply(f"♻️ Reusing `{fid}`. Send search term:")
        user_states[uid] = {"state": "awaiting_search_term", "url": raw, "mode": "single"}
        return await message.reply("🔍 Send the search term:")

    if state == "awaiting_urls":
        urls = sd.get("urls", [])
        new = [u.strip() for u in message.text.split("\n") if u.strip()]
        urls.extend(new)
        user_states[uid] = {"state": "awaiting_urls", "urls": urls, "mode": mode}
        return await message.reply(f"✅ +{len(new)} URLs (Total {len(urls)})\nSend more or /done")

    if state == "awaiting_terms":
        terms = sd.get("terms", [])
        new = [t.strip() for t in message.text.split("\n") if t.strip()]
        terms.extend(new)
        user_states[uid] = {**sd, "terms": terms}
        return await message.reply(f"✅ +{len(new)} terms (Total {len(terms)})\nSend more or /done")

    if state in ("awaiting_terms_for_recheck", "awaiting_terms_allfiles"):
        terms = sd.get("terms", [])
        new = [t.strip() for t in message.text.split("\n") if t.strip()]
        terms.extend(new)
        user_states[uid] = {**sd, "terms": terms}
        return await message.reply(f"✅ +{len(new)} terms (Total {len(terms)})\nSend more or /done")

    if state == "awaiting_search_term":
        term = message.text.strip()
        user_states.pop(uid, None)
        if mode == "single_saved":
            await process_single_saved(client, message, sd["file_id"], term)
        else:
            await process_single(client, message, sd["url"], term)

# ================= PROCESSORS =================
async def process_single(client, message, url, term):
    status = await message.reply("⚡ Preparing...")
    try:
        path, fid = await download_or_reuse(url, 1, status)
        searcher = FastFileSearcher(str(path))
        count, matches = await searcher.search_parallel(term)
        if count == 0:
            return await status.edit_text(f"❌ No matches for `{term}`")
        safe = re.sub(r"[^A-Za-z0-9._-]", "_", term)[:30]
        out = RESULTS_DIR / f"single_{fid}_{safe}.txt"
        async with aiofiles.open(out, "wb") as f:
            await f.write(f"=== {fid} ===\n=== {term} ===\n=== {count} ===\n\n".encode())
            for m in matches[:MAX_RESULT_LINES]:
                await f.write(m + b"\n")
        await message.reply_document(out, caption=f"📄 `{term}` | {count:,} matches")
        zips = split_file_into_zips([out], f"single_{fid}_{safe}")
        for i, z in enumerate(zips, 1):
            await message.reply_document(z, caption=f"🗜 ZIP {i}/{len(zips)}")
        Registry.add_history({"terms": [term], "files": 1, "matches": count,
                              "result_paths": [str(out)]})
        await status.delete()
    except Exception as e:
        await status.edit_text(f"❌ Error: {e}")

async def process_single_saved(client, message, file_id, term):
    meta = Registry.get_file(file_id)
    if not meta or not Path(meta["path"]).exists():
        return await message.reply("❌ Saved file missing.")
    return await process_single(client, message, meta["url"], term)

async def process_multi(client, message, urls, terms):
    status = await message.reply("⚡ Preparing downloads...")
    entries, reused = [], 0
    try:
        for i, url in enumerate(urls, 1):
            p, fid = await download_or_reuse(url, i, status)
            entries.append((i, p, fid))
            if Registry.find_by_url(url):
                reused += 1
    except Exception as e:
        return await status.edit_text(f"❌ Download error: {e}")
    if reused:
        try:
            await status.edit_text(f"♻️ Reused {reused} saved files.")
            await asyncio.sleep(1)
        except Exception:
            pass
    await run_search_on_files(client, message, entries, terms)

async def process_saved_batch(client, message, file_ids, terms):
    entries = []
    for i, fid in enumerate(file_ids, 1):
        meta = Registry.get_file(fid)
        if meta and Path(meta["path"]).exists():
            entries.append((i, Path(meta["path"]), fid))
    if not entries:
        return await message.reply("❌ No valid saved files.")
    await run_search_on_files(client, message, entries, terms)

# ================= CALLBACKS =================
@app.on_callback_query()
async def on_cb(client: Client, q: CallbackQuery):
    uid = q.from_user.id
    if uid != ALLOWED_USER_ID:
        return await q.answer("❌ Unauthorized", show_alert=True)
    d = q.data

    if d == "single":
        user_states[uid] = {"state": "awaiting_url", "mode": "single"}
        await q.message.reply("📎 Send URL or saved file id:")
    elif d == "multi_both":
        user_states[uid] = {"state": "awaiting_urls", "urls": [], "mode": "multi_both"}
        await q.message.reply("📎 Send URLs (one per line). /done when finished.")
    elif d == "list_files":
        files = Registry.all_files()
        if not files:
            await q.message.reply("📂 No saved files.")
        else:
            lines = [f"🗂 **{len(files)} files**\n"]
            for fid, m in files.items():
                ok = "✅" if Path(m["path"]).exists() else "❌"
                lines.append(f"{ok} `{fid}` | {m['name']} | {human_size(m.get('size',0))}")
            await q.message.reply("\n".join(lines))
    elif d == "recheck_menu":
        files = Registry.all_files()
        if not files:
            await q.message.reply("📂 No saved files.")
        else:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"📄 {fid} — {m['name'][:20]}", callback_data=f"recheck:{fid}")]
                for fid, m in list(files.items())[:20]
            ])
            await q.message.reply("♻️ Pick a file:", reply_markup=kb)
    elif d.startswith("recheck:"):
        fid = d.split(":", 1)[1]
        meta = Registry.get_file(fid)
        if not meta:
            await q.answer("Not found", show_alert=True)
        else:
            user_states[uid] = {"state": "awaiting_terms_for_recheck",
                                "file_ids": [fid], "terms": [], "mode": "recheck"}
            await q.message.reply(f"♻️ Recheck `{fid}`\nSend terms, then /done")
    elif d == "history":
        hist = Registry.all_history()
        if not hist:
            await q.message.reply("📜 No history.")
        else:
            lines = [f"📜 **{len(hist)} searches**\n"]
            for rid, h in list(hist.items())[-15:][::-1]:
                lines.append(f"`{rid}` | {h['time'][:19]} | {h.get('matches',0):,} matches")
            await q.message.reply("\n".join(lines))
    elif d == "clean":
        deleted = freed = 0
        for dd in (RESULTS_DIR, ZIP_DIR):
            for f in dd.iterdir():
                if f.is_file():
                    freed += f.stat().st_size
                    f.unlink(missing_ok=True)
                    deleted += 1
        await q.message.reply(f"🧹 Cleaned {deleted} files ({human_size(freed)})\n"
                              f"✅ Sources preserved.")
    elif d == "status":
        files = Registry.all_files()
        total = sum(Path(m["path"]).stat().st_size for m in files.values() if Path(m["path"]).exists())
        stat = os.statvfs(BASE_DIR)
        free = stat.f_bavail * stat.f_frsize
        await q.message.reply(
            f"📊 **Status**\n🗂 Saved: {len(files)} ({human_size(total)})\n"
            f"📜 History: {len(Registry.all_history())}\n"
            f"💿 Free: {human_size(free)}\n📂 Data: `{BASE_DIR}`"
        )
    await q.answer()

# ================= RUN =================
if __name__ == "__main__":
    print(f"⚡ Bot starting...")
    print(f"CPU: {mp.cpu_count()} | Workers: {MAX_WORKERS}")
    print(f"Data dir: {BASE_DIR}")
    print(f"Volume detected: {'YES' if os.environ.get('RAILWAY_VOLUME_MOUNT_PATH') else 'NO'}")
    app.run()
