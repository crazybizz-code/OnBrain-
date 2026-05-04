"""
OnBrain AI - Telegram Mini App Backend
Full-featured FastAPI server — reuses core logic from bot.py
"""

import asyncio
import io
import json
import logging
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ─── Import core functions from bot.py ───────────────────────────────────────
# bot.py only runs the bot under `if __name__ == "__main__"`, so importing
# it here just loads all the helper functions safely.
from bot import (
    Session,
    Config,
    _python_answer,
    _build_schema_info,
    _extract_sheet_id,
    ask_grok,
    fetch_sheet_public,
    parse_excel,
    supa_upsert_user,
    _rows_to_text,
    MAX_ROWS,
    MAX_CHARS,
)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("miniapp")

# ─── Config ──────────────────────────────────────────────────────────────────
try:
    config = Config.from_env()
except Exception as e:
    logger.warning(f"Config.from_env() failed: {e} — some features disabled")
    config = None

# ─── In-memory session store ─────────────────────────────────────────────────
_sessions: dict[int, Session] = {}

def get_session(uid: int) -> Session:
    if uid not in _sessions:
        _sessions[uid] = Session()
    return _sessions[uid]

# ─── SQLite user store ────────────────────────────────────────────────────────
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_DB_env = os.environ.get("SQLITE_TOKEN_DB", "")
# If env path directory doesn't exist (e.g. server-only path like /data), fall back to project dir
if _DB_env and not os.path.exists(os.path.dirname(_DB_env) or "."):
    _DB = os.path.join(_BASE_DIR, "google_tokens.db")
else:
    _DB = _DB_env or os.path.join(_BASE_DIR, "google_tokens.db")

def _ensure_users_table():
    con = sqlite3.connect(_DB)
    con.execute("""
        CREATE TABLE IF NOT EXISTS miniapp_users (
            telegram_id   INTEGER PRIMARY KEY,
            full_name     TEXT,
            username      TEXT,
            lang          TEXT DEFAULT 'uz',
            registered_at TEXT
        )
    """)
    con.commit()
    con.close()

_ensure_users_table()

def db_get_user(uid: int) -> dict | None:
    con = sqlite3.connect(_DB)
    row = con.execute(
        "SELECT telegram_id, full_name, username, lang FROM miniapp_users WHERE telegram_id=?",
        (uid,)
    ).fetchone()
    con.close()
    if row:
        return {"telegram_id": row[0], "full_name": row[1], "username": row[2], "lang": row[3]}
    return None

def db_upsert_user(uid: int, full_name: str, username: str, lang: str = "uz"):
    con = sqlite3.connect(_DB)
    con.execute("""
        INSERT INTO miniapp_users (telegram_id, full_name, username, lang, registered_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(telegram_id) DO UPDATE SET
            full_name=excluded.full_name,
            username=excluded.username,
            lang=excluded.lang
    """, (uid, full_name, username, lang, datetime.now(timezone.utc).isoformat()))
    con.commit()
    con.close()

# ─── FastAPI app ──────────────────────────────────────────────────────────────
from contextlib import asynccontextmanager
import subprocess
import sys

_bot_process: Optional[subprocess.Popen] = None

@asynccontextmanager
async def lifespan(application: FastAPI):
    """Start bot.py as a background subprocess alongside the web server."""
    global _bot_process
    bot_py = os.path.join(os.path.dirname(__file__), "bot.py")
    if os.path.exists(bot_py):
        logger.info("🤖 Starting bot.py as background process...")
        _bot_process = subprocess.Popen(
            [sys.executable, bot_py],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        logger.info(f"🤖 bot.py PID={_bot_process.pid}")
    yield
    if _bot_process:
        logger.info("🛑 Stopping bot.py...")
        _bot_process.terminate()

app = FastAPI(title="OnBrain AI Mini App", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static files
_STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
if os.path.isdir(_STATIC_DIR):
    app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")

# ─── Pydantic models ──────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    telegram_id: int
    full_name: str
    username: str = ""
    lang: str = "uz"

class ChatRequest(BaseModel):
    telegram_id: int
    message: str

class SheetsRequest(BaseModel):
    telegram_id: int
    url: str

class ClearRequest(BaseModel):
    telegram_id: int

class LangRequest(BaseModel):
    telegram_id: int
    lang: str

# ─── Routes ───────────────────────────────────────────────────────────────────

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_INDEX_HTML = os.path.join(_BASE_DIR, "static", "index.html")

@app.get("/health")
async def health():
    return {"status": "ok", "service": "OnBrain AI Mini App"}

@app.get("/miniapp")
async def serve_miniapp():
    return FileResponse(_INDEX_HTML)

@app.get("/")
async def root():
    return FileResponse(_INDEX_HTML)

# ── User management ──────────────────────────────────────────────────────────

@app.post("/api/register")
async def register(req: RegisterRequest):
    try:
        db_upsert_user(req.telegram_id, req.full_name, req.username, req.lang)
        try:
            supa_upsert_user(req.telegram_id, req.username, req.full_name, "", req.lang)
        except Exception:
            pass
        sess = get_session(req.telegram_id)
        sess.lang = req.lang
        return {"success": True, "user": db_get_user(req.telegram_id)}
    except Exception as e:
        logger.error(f"Register error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/user/{telegram_id}")
async def get_user(telegram_id: int):
    user = db_get_user(telegram_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return {"success": True, "user": user}

@app.post("/api/lang")
async def set_lang(req: LangRequest):
    sess = get_session(req.telegram_id)
    sess.lang = req.lang
    u = db_get_user(req.telegram_id)
    db_upsert_user(req.telegram_id, u.get("full_name", "") if u else "", u.get("username", "") if u else "", req.lang)
    return {"success": True}

# ── Data sources ─────────────────────────────────────────────────────────────

@app.post("/api/upload")
async def upload_excel(telegram_id: int = Form(...), file: UploadFile = File(...)):
    """Upload and parse Excel → store in session"""
    try:
        fname = file.filename or "file.xlsx"
        if not fname.lower().endswith((".xlsx", ".xls", ".xlsm")):
            raise HTTPException(status_code=400, detail="Faqat Excel fayl (.xlsx, .xls) qabul qilinadi")
        content = await file.read()
        uid = telegram_id
        rows = parse_excel(fname, content, uid=uid)
        if not rows or len(rows) < 2:
            raise HTTPException(status_code=400, detail="Fayl bo'sh yoki o'qib bo'lmadi")
        header = rows[0]
        sess = get_session(uid)
        sess.sources = [s for s in sess.sources if s.get("source_type") != "excel"]
        src_name = fname.rsplit(".", 1)[0]
        sess.sources.append({
            "source_type": "excel",
            "source_name": src_name,
            "source_url": "",
            "data": rows,
        })
        sess.schema_info = _build_schema_info(sess.sources)
        sess.step = "in_chat"
        sess.last_found_names = []
        return {
            "success": True,
            "source_name": src_name,
            "rows": len(rows) - 1,
            "cols": len(header),
            "headers": [str(h).strip() for h in header[:20]],
            "total_sources": len(sess.sources),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload error uid={telegram_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/sheets")
async def connect_sheets(req: SheetsRequest):
    """Connect a public Google Sheets URL → store in session"""
    try:
        uid = req.telegram_id
        url = req.url.strip()
        if not url:
            raise HTTPException(status_code=400, detail="URL bo'sh")
        sheet_id = _extract_sheet_id(url)
        if not sheet_id:
            raise HTTPException(status_code=400, detail="Google Sheets linki noto'g'ri. Masol: https://docs.google.com/spreadsheets/d/XXXX/edit")
        data = await fetch_sheet_public(sheet_id)
        if not data:
            raise HTTPException(status_code=400, detail="Sheets'ga ulanib bo'lmadi. Faylni 'Hamma' uchun ochiq qiling.")
        sess = get_session(uid)
        sess.sources = [s for s in sess.sources if s.get("sheet_id") != sheet_id]
        first_tab = next(iter(data.keys()), "Sheet")
        row_count = sum(len(v) - 1 for v in data.values() if isinstance(v, list) and len(v) > 1)
        sess.sources.append({
            "source_type": "google_sheets",
            "source_name": first_tab,
            "source_url": url,
            "sheet_id": sheet_id,
            "data": data,
        })
        sess.schema_info = _build_schema_info(sess.sources)
        sess.step = "in_chat"
        sess.last_found_names = []
        sess.last_sheet_url = url
        return {
            "success": True,
            "source_name": first_tab,
            "tabs": list(data.keys()),
            "rows": row_count,
            "total_sources": len(sess.sources),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Sheets error uid={req.telegram_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/session/{telegram_id}")
async def get_session_info(telegram_id: int):
    sess = get_session(telegram_id)
    sources_info = []
    for src in sess.sources:
        info = {"type": src.get("source_type", "?"), "name": src.get("source_name", "?"), "url": src.get("source_url", "")}
        d = src.get("data")
        if isinstance(d, list) and d:
            info["rows"] = len(d) - 1
        elif isinstance(d, dict):
            info["tabs"] = list(d.keys())
            info["rows"] = sum(len(v) - 1 for v in d.values() if isinstance(v, list) and len(v) > 1)
        sources_info.append(info)
    return {"success": True, "has_data": bool(sess.sources), "sources": sources_info, "lang": sess.lang}

@app.post("/api/clear")
async def clear_session(req: ClearRequest):
    sess = get_session(req.telegram_id)
    sess.sources = []
    sess.excel_data = []
    sess.sheets_data = {}
    sess.schema_info = ""
    sess.last_found_names = []
    sess.step = "idle"
    return {"success": True}

# ── Chat ──────────────────────────────────────────────────────────────────────

@app.post("/api/chat")
async def chat(req: ChatRequest):
    try:
        uid = req.telegram_id
        question = req.message.strip()
        if not question:
            raise HTTPException(status_code=400, detail="Savol bo'sh")
        sess = get_session(uid)
        lang = sess.lang or "uz"

        # Disambiguation: user typed a number after candidate list
        if sess.disambiguation_candidates:
            if question.isdigit():
                idx = int(question) - 1
                if 0 <= idx < len(sess.disambiguation_candidates):
                    chosen = sess.disambiguation_candidates[idx]
                    sess.last_found_names = [chosen]
                    sess.disambiguation_candidates = []
                    question = chosen
                else:
                    sess.disambiguation_candidates = []

        # 1. Try Python exact answer (person search / aggregations)
        py_ans = None
        try:
            py_ans = _python_answer(question, sess)
        except Exception as e:
            logger.warning(f"python_answer error uid={uid}: {e}")

        if py_ans is not None:
            # If python search returned "not found" and we have sources + AI key, ask AI instead
            _not_found_phrases = ["topilmadi", "not found", "не найден", "bazasida topilmadi"]
            _is_not_found = any(p in (py_ans or "").lower() for p in _not_found_phrases)
            if _is_not_found and sess.sources and config and config.grok_key:
                pass  # fall through to AI
            else:
                return {"success": True, "answer": py_ans, "source": "python"}

        # 2. No data → ask user to upload
        if not sess.sources:
            no_data = {
                "uz": "📂 Hali ma'lumot yuklanmagan. Excel fayl yuboring yoki Google Sheets linki qo'shing.",
                "ru": "📂 Данные не загружены. Отправьте Excel или добавьте Google Sheets.",
                "en": "📂 No data loaded. Please upload an Excel file or add a Google Sheets link.",
            }
            return {"success": True, "answer": no_data.get(lang, no_data["uz"]), "source": "system"}

        # 3. Build context and ask AI
        if not config or not config.grok_key:
            return {"success": True, "answer": "⚠️ AI kalit mavjud emas.", "source": "error"}

        all_rows = []
        for src in sess.sources:
            d = src.get("data")
            if isinstance(d, list) and d:
                all_rows.extend(d[:MAX_ROWS])
            elif isinstance(d, dict):
                for tab_rows in d.values():
                    if isinstance(tab_rows, list):
                        all_rows.extend(tab_rows[:MAX_ROWS])

        context = _rows_to_text(all_rows)[:MAX_CHARS]
        ai_ans = await ask_grok(question, context, config.grok_key, lang, schema_info=sess.schema_info)
        return {"success": True, "answer": ai_ans, "source": "ai"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Chat error uid={req.telegram_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ─── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("MINIAPP_PORT", "8001"))
    logger.info(f"🚀 OnBrain AI Mini App → http://localhost:{port}/miniapp")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
