# OnBrain AI Bot v2.2.0 - Koyeb Migration (March 22, 2026)

import asyncio

import io

import json

import logging

import os

import re

import sqlite3

from html import escape as html_escape

import secrets

import time

import uuid

from dataclasses import dataclass, field

from datetime import datetime, timezone

from typing import Any



import openpyxl

import xlrd

from aiohttp import web

from aiogram import Bot, Dispatcher, F

from aiogram.client.default import DefaultBotProperties

from aiogram.enums import ParseMode

from aiogram.exceptions import TelegramBadRequest, TelegramAPIError

from aiogram.filters import CommandStart, Command

from aiogram.types import (

    CallbackQuery,

    InlineKeyboardButton,

    InlineKeyboardMarkup,

    KeyboardButton,

    Message,

    ReplyKeyboardMarkup,

    ReplyKeyboardRemove,

    WebAppInfo,

)

from dotenv import load_dotenv

from googleapiclient.discovery import build

from google.auth.transport.requests import Request as GoogleAuthRequest

from google.oauth2.credentials import Credentials

from google_auth_oauthlib.flow import Flow

import httpx

import requests

import aiohttp



# Audio processing pipeline — OGG→WAV conversion, chunking, transcription
try:
    from audio_processor import transcribe_audio as _transcribe_audio
    AUDIO_PROCESSOR_AVAILABLE = True
except ImportError:
    AUDIO_PROCESSOR_AVAILABLE = False

# OpenAI - used for voice transcription via Whisper (fallback)

try:

    from openai import AsyncOpenAI as _AsyncOpenAI

    OPENAI_WHISPER_AVAILABLE = True

except ImportError:

    _AsyncOpenAI = None

    OPENAI_WHISPER_AVAILABLE = False




import gspread



# Import Data Indexing Service (optional — graceful fallback if file missing)

try:

    from data_indexing_service import DataIndexingService

except ImportError:

    DataIndexingService = None  # type: ignore





logging.basicConfig(

    level=logging.INFO,

    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",

)

logger = logging.getLogger("onbrain-ai-bot")



# ==================== ANIMATION & UI HELPERS ====================



class UIAnimations:

    """User-friendly animation strings for loading states and feedback"""

    

    # Loading animations

    LOADING_SPINNER = ["⚡", "⚡", "⚡", "⚡", "⚡", "⚡", "⚡", "⚡", "⚡", "⚡"]

    

    # Status messages with professional emojis

    LOADING = "⚙️| Processing"

    SUCCESS = "✅ Done"

    ERROR = "❌ Error"

    PROCESSING = "⏳П Working"

    CONNECTING = "🔗 Connecting"

    SYNCING = "📍 Syncing"

    UPLOADING = "📤 Uploading"

    DOWNLOADING = "📥 Downloading"

    

    # Stage descriptions

    STAGES = {

        "authenticating": "🔐 Authenticating with Google",

        "loading_sheets": "📊 Loading spreadsheets",

        "reading_data": "📖 Reading data",

        "indexing": "📑П Indexing content",

        "formatting": "✨ Formatting response",

        "searching": "🔍 Searching",

        "connecting_drive": "📁 Connecting to Google Drive",

        "reading_folder": "📁 Reading folder structure",

        "processing_files": "📄 Processing files",

    }

    

    @staticmethod

    def loading_message(action: str, details: str = "") -> str:

        """Generate a friendly loading message

        

        Args:

            action: Key from STAGES dict

            details: Additional info to show (e.g., "3/10 items")

        """

        emoji = UIAnimations.STAGES.get(action, "⚙️|")

        msg = f"{emoji} {action.replace('_', ' ').title()}"

        if details:

            msg += f"\n📬 {details}"

        msg += "\n\n⏳ Biroz kuting..."

        return msg

    

    @staticmethod

    def success_message(action: str, result: str = "") -> str:

        """Generate a success message"""

        msg = f"✅ <b>Success!</b>\n"

        msg += f"📬 {action}\n"

        if result:

            msg += f"📊 {result}"

        return msg

    

    @staticmethod

    def error_message(action: str, error: str = "", retry_hint: str = True) -> str:

        """Generate an error message"""

        msg = f"❌ <b>Oops! Something went wrong</b>\n"

        msg += f"📬 {action}\n"

        if error:

            msg += f"❌❌ {error[:100]}\n"

        if retry_hint:

            msg += f"\n📍 Qaytadan urinib ko'ring or contact support."

        return msg



# ==================== DYNAMIC GREETINGS ====================



import random

from datetime import datetime, timezone



class DynamicGreetings:

    """Generate time-based dynamic greetings in Uzbek for chat mode"""



    MORNING_GREETINGS = [

        "Good morning, {name}! 🌅 Ready to learn something new today?",

        "Hello, {name}! 🌅П New day, — new opportunities.",

        "Good morning, {name}! 🌄 I'm ready for your plans today.",

    ]



    AFTERNOON_GREETINGS = [

        "Good morning, {name}! 🌅П Ask a question, — I'm ready to answer.",

        "Hello, {name}! 🍽 I can help you during lunch too.",

        "Hello, {name}! 📋 Shall we talk about today's tasks?",

    ]



    EVENING_GREETINGS = [

        "Good evening, {name}! 🌆 I'm ready to help you this evening.",

        "Hello, {name}! 🌙 I'm right here for your evening questions too.",

        "Good evening, {name}! ✨ Want to talk about what you accomplished today?",

    ]



    NIGHT_GREETINGS = [

        "Hello, {name}! 🌃 Even if it's late, I'm ready to help.",

        "Hello, {name}! 🌙 I'll answer your questions at night too.",

        "Hello, {name}! 🌌 Can't sleep? I'm right here.",

    ]



    FOLLOW_UPS = [

        "Type any question you have.",

        "How can I help you?",

        "What's your question?",

        "Qaysi mavzu sizni qiziqtiradi?",

        "If you need help — just write!",

    ]

    

    @staticmethod

    def get_time_period() -> str:

        """Get current time period (morning/afternoon/evening/night)"""

        now = datetime.now(timezone.utc)

        hour = now.hour

        

        if 5 <= hour < 12:

            return "morning"

        elif 12 <= hour < 17:

            return "afternoon"

        elif 17 <= hour < 21:

            return "evening"

        else:

            return "night"

    

    @staticmethod

    def get_dynamic_greeting(user_name: str = "") -> str:

        """Get a time-based, personalized greeting in Uzbek"""

        period = DynamicGreetings.get_time_period()

        name = user_name.strip() if user_name else "Do'stim"



        if period == "morning":

            template = random.choice(DynamicGreetings.MORNING_GREETINGS)

        elif period == "afternoon":

            template = random.choice(DynamicGreetings.AFTERNOON_GREETINGS)

        elif period == "evening":

            template = random.choice(DynamicGreetings.EVENING_GREETINGS)

        else:

            template = random.choice(DynamicGreetings.NIGHT_GREETINGS)



        greeting = template.format(name=name)

        follow_up = random.choice(DynamicGreetings.FOLLOW_UPS)



        return (

            f"💬 <b>Chat Mode</b>\n\n"

            f"{greeting}\n\n"

            f"{follow_up} 💬"

        )



# ==================== VERSION & FEATURES ====================

BOT_VERSION = "2.1.0"

FEATURES = {

    "ai_qa": True,           # AI Q&A from indexed data

    "data_indexing": True,   # LlamaIndex VectorStore indexing

    "google_sheets": True,   # Google Sheets integration

    "google_drive": True,    # Google Drive folder reading

    "tavily_search": True,   # Web search via Tavily

    "grok_ai": True,         # Grok AI for spreadsheet Q&A

}

logger.info(f"🤖 OnBrain AI Bot v{BOT_VERSION} - Features: {FEATURES}")



# ==================== SECURITY UTILITIES ====================



class RateLimiter:

    """Rate limiting to prevent brute force attacks"""

    def __init__(self, max_requests: int = 10, time_window: int = 60):

        self.max_requests = max_requests

        self.time_window = time_window

        self.requests: dict[int, list[float]] = {}

    

    def is_allowed(self, user_id: int) -> bool:

        """Check if user is within rate limit"""

        now = time.time()

        if user_id not in self.requests:

            self.requests[user_id] = []

        

        # Remove old requests outside the time window

        self.requests[user_id] = [

            req_time for req_time in self.requests[user_id]

            if now - req_time < self.time_window

        ]

        

        if len(self.requests[user_id]) >= self.max_requests:

            logger.warning(f"⚠️ Rate limit exceeded for user {user_id}")

            return False

        

        self.requests[user_id].append(now)

        return True





class InputValidator:

    """Validate and sanitize user inputs"""

    

    @staticmethod

    def validate_email(email: str) -> bool:

        """Validate email format"""

        if not email or len(email) > 254:

            return False

        return EMAIL_REGEX.match(email) is not None

    

    @staticmethod

    def validate_name(name: str) -> bool:

        """Validate user name"""

        if not name or len(name) > 100:

            return False

        if len(name) < 2:

            return False

        # Allow letters (Latin and Cyrillic), spaces, hyphens, apostrophes

        return bool(re.match(r"^[a-zA-Z\u0400-\u04FF\u02BB0-9\s\-'\.]+$", name))

    

    @staticmethod

    def sanitize_string(text: str, max_length: int = 500) -> str:

        """Sanitize string input"""

        if not text:

            return ""

        # Remove null bytes and control characters

        text = text.replace('\x00', '').replace('\n', ' ').replace('\r', '')

        # Limit length

        return text[:max_length].strip()

    

    @staticmethod

    def validate_sheet_id(sheet_id: str) -> bool:

        """Validate Google Sheets ID format"""

        # Google Sheets IDs are typically 44 characters of alphanumeric, -, and _

        if not sheet_id or len(sheet_id) > 100:

            return False

        return bool(re.match(r"^[a-zA-Z0-9\-_]+$", sheet_id))

    

    @staticmethod

    def validate_phone(phone: str) -> bool:

        """Validate phone number format"""

        if not phone or len(phone) > 20:

            return False

        # Remove common formatting characters

        cleaned = re.sub(r'[\s\-\(\)\.+]', '', phone)

        # Should be mostly digits

        return len(cleaned) >= 7 and sum(c.isdigit() for c in cleaned) >= 7





class SessionManager:

    """Manage user sessions with timeout"""

    def __init__(self, timeout_seconds: int = 3600):

        self.timeout_seconds = timeout_seconds

        self.sessions: dict[int, tuple[Any, float]] = {}

    

    def get(self, user_id: int) -> Any | None:

        """Get session, return None if expired"""

        if user_id not in self.sessions:

            return None

        

        session, created_time = self.sessions[user_id]

        if time.time() - created_time > self.timeout_seconds:

            logger.warning(f"⚙️░ Session expired for user {user_id}")

            del self.sessions[user_id]

            return None

        

        return session

    

    def set(self, user_id: int, session: Any) -> None:

        """Set or update session"""

        self.sessions[user_id] = (session, time.time())

    

    def delete(self, user_id: int) -> None:

        """Delete session"""

        self.sessions.pop(user_id, None)

    

    def cleanup_expired(self) -> None:

        """Remove all expired sessions"""

        now = time.time()

        expired = [

            uid for uid, (_, created) in self.sessions.items()

            if now - created > self.timeout_seconds

        ]

        for uid in expired:

            del self.sessions[uid]

        if expired:

            logger.info(f"🧹 Cleaned up {len(expired)} expired sessions")





class FileValidator:

    """Validate uploaded files for security"""

    

    # Maximum file sizes (in bytes)

    MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB

    MAX_EXCEL_SIZE = 5 * 1024 * 1024  # 5 MB

    

    # Allowed file extensions

    ALLOWED_EXTENSIONS = {'.xlsx', '.xls', '.xlsm'}

    

    # Suspicious patterns in Excel files

    SUSPICIOUS_PATTERNS = [

        b'cmd.exe',

        b'powershell',

        b'bash',

        b'eval(',

        b'exec(',

        b'__import__',

    ]

    

    @staticmethod

    def validate_excel_file(file_name: str, file_content: bytes) -> tuple[bool, str]:

        """

        Validate Excel file for security

        Returns: (is_valid, error_message)

        """

        # 1. Check file extension

        _, ext = file_name.rsplit('.', 1) if '.' in file_name else ('', '')

        ext = f".{ext.lower()}"

        

        if ext not in FileValidator.ALLOWED_EXTENSIONS:

            return False, f"❌ Fayl turi ruxsatga tushmasligi kerak: {ext}"

        

        # 2. Check file size

        file_size = len(file_content)

        if file_size > FileValidator.MAX_EXCEL_SIZE:

            size_mb = file_size / (1024 * 1024)

            max_mb = FileValidator.MAX_EXCEL_SIZE / (1024 * 1024)

            return False, f"❌ Fayl juda katta ({size_mb:.1f} MB). Max: {max_mb:.0f} MB"

        

        if file_size == 0:

            return False, "❌ Fayl bo'sh."

        

        # 3. Check for suspicious patterns

        for pattern in FileValidator.SUSPICIOUS_PATTERNS:

            if pattern in file_content:

                logger.warning(f"⚠️ Suspicious pattern detected in file: {pattern}")

                return False, "❌ Fayl xavfsiz emas (shubhali kontent topildi)"

        

        # 4. Try to parse the file to ensure it's valid

        try:

            if ext == '.xlsx':

                from openpyxl import load_workbook

                workbook = load_workbook(io.BytesIO(file_content), data_only=True, read_only=True)

                

                # Check for VBA macros (potential security risk)

                if hasattr(workbook, 'vba_archive') and workbook.vba_archive:

                    logger.warning(f"⚠️ File contains VBA macros")

                    return False, "❌ File contains VBA macros. Please send a clean file"

                

                # Get basic info

                sheet_count = len(workbook.sheetnames)

                logger.info(f"✅ Excel file validated: {sheet_count} sheets")

                

            elif ext == '.xls':

                import xlrd

                workbook = xlrd.open_workbook(file_contents=file_content, on_demand=True)

                sheet_count = workbook.nsheets

                logger.info(f"✅ Excel file validated: {sheet_count} sheets")

        

        except Exception as e:

            logger.error(f"❌ Failed to parse Excel file: {e}")

            return False, f"❌ Could not read file: {str(e)[:50]}"

        

        return True, "✅ File verified"

    

    @staticmethod

    def sanitize_filename(filename: str) -> str:

        """Sanitize filename to prevent directory traversal"""

        # Remove path separators

        filename = filename.replace('\\', '').replace('/', '')

        # Remove null bytes

        filename = filename.replace('\x00', '')

        # Limit length

        return filename[:255]





# Initialize security components

rate_limiter = RateLimiter(max_requests=20, time_window=60)  # 20 requests per minute

input_validator = InputValidator()



EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")



# ---------------------------------------------------------------------------

# SQLite-backed Google token store

# Tokens are persisted to a local SQLite file so they survive bot restarts.

# Supabase is still used as a secondary backup; SQLite is the primary fast

# store that works even when Supabase is unavailable.

# ---------------------------------------------------------------------------



class SQLiteTokenStore:

    """Persist Google OAuth tokens in a local SQLite database."""



    # Use SQLITE_TOKEN_DB env var; if the directory doesn't exist, fall back to /tmp

    _raw_path = os.environ.get("SQLITE_TOKEN_DB", "google_tokens.db")

    _db_dir = os.path.dirname(_raw_path) if os.path.dirname(_raw_path) else "."

    DB_PATH = _raw_path if (os.path.exists(_db_dir) or _db_dir == ".") else os.path.join("/tmp", os.path.basename(_raw_path))



    def __init__(self) -> None:

        self._init_db()



    def _connect(self) -> sqlite3.Connection:

        conn = sqlite3.connect(self.DB_PATH, check_same_thread=False)

        conn.row_factory = sqlite3.Row

        return conn



    def _init_db(self) -> None:

        try:

            with self._connect() as conn:

                conn.execute("""

                    CREATE TABLE IF NOT EXISTS google_tokens (

                        telegram_id   INTEGER PRIMARY KEY,

                        credentials_json TEXT NOT NULL,

                        updated_at    TEXT NOT NULL

                    )

                """)

                conn.commit()

            logger.info("✅ SQLite token store initialised")

        except Exception as exc:

            logger.error(f"❌ SQLite token store init error: {exc}")



    def save(self, telegram_id: int, credentials_json: str) -> None:

        try:

            now = datetime.now(timezone.utc).isoformat()

            with self._connect() as conn:

                conn.execute("""

                    INSERT INTO google_tokens (telegram_id, credentials_json, updated_at)

                    VALUES (?, ?, ?)

                    ON CONFLICT(telegram_id) DO UPDATE SET

                        credentials_json = excluded.credentials_json,

                        updated_at       = excluded.updated_at

                """, (telegram_id, credentials_json, now))

                conn.commit()

            logger.info(f"💾 SQLite: saved token for user {telegram_id}")

        except Exception as exc:

            logger.warning(f"⚠️ SQLite token save error: {exc}")



    def load(self, telegram_id: int) -> str | None:

        try:

            with self._connect() as conn:

                row = conn.execute(

                    "SELECT credentials_json FROM google_tokens WHERE telegram_id = ?",

                    (telegram_id,)

                ).fetchone()

            if row:

                logger.info(f"✅ SQLite: loaded token for user {telegram_id}")

                return row["credentials_json"]

        except Exception as exc:

            logger.warning(f"⚠️ SQLite token load error: {exc}")

        return None



    def delete(self, telegram_id: int) -> None:

        try:

            with self._connect() as conn:

                conn.execute("DELETE FROM google_tokens WHERE telegram_id = ?", (telegram_id,))

                conn.commit()

        except Exception as exc:

            logger.warning(f"⚠️ SQLite token delete error: {exc}")





# Singleton token store 🔧 imported everywhere inside this module

_token_store = SQLiteTokenStore()



# ---------------------------------------------------------------------------

# SQLite-backed workspace store

# Persists connected folder/sheet metadata and cached spreadsheet data so

# users never have to re-upload a link after a bot restart.

#

# Schema (future-proof multi-tenant structure):

#   companies       🔧 one company per admin; holds folder_id / folder_url

#   company_users   🔧 links telegram_id 📊 company_id

#   sheets_cache    🔧 cached JSON data per sheet_id

# ---------------------------------------------------------------------------



class WorkspaceStore:

    """Persist folder/sheet connections and cached data across restarts."""



    # Use same fallback logic as SQLiteTokenStore

    _raw_path = os.environ.get("SQLITE_TOKEN_DB", "google_tokens.db")

    _db_dir = os.path.dirname(_raw_path) if os.path.dirname(_raw_path) else "."

    DB_PATH = _raw_path if (os.path.exists(_db_dir) or _db_dir == ".") else os.path.join("/tmp", os.path.basename(_raw_path))



    def _connect(self) -> sqlite3.Connection:

        conn = sqlite3.connect(self.DB_PATH, check_same_thread=False)

        conn.row_factory = sqlite3.Row

        conn.execute("PRAGMA journal_mode=WAL")  # safe concurrent reads

        return conn



    def init_db(self) -> None:

        try:

            with self._connect() as conn:

                conn.executescript("""

                    CREATE TABLE IF NOT EXISTS companies (

                        company_id   INTEGER PRIMARY KEY AUTOINCREMENT,

                        admin_user_id INTEGER NOT NULL UNIQUE,

                        folder_id    TEXT,

                        folder_url   TEXT,

                        sheet_id     TEXT,

                        sheet_name   TEXT,

                        mode         TEXT DEFAULT 'sheets',

                        folder_spreadsheets TEXT,

                        selected_spreadsheets TEXT,

                        connected_at TEXT NOT NULL

                    );



                    CREATE TABLE IF NOT EXISTS company_users (

                        user_id    INTEGER PRIMARY KEY,

                        company_id INTEGER NOT NULL,

                        joined_at  TEXT NOT NULL,

                        FOREIGN KEY (company_id) REFERENCES companies(company_id)

                    );



                    CREATE TABLE IF NOT EXISTS sheets_cache (

                        cache_key    TEXT PRIMARY KEY,

                        company_id   INTEGER NOT NULL,

                        sheet_name   TEXT,

                        data         TEXT NOT NULL,

                        last_updated TEXT NOT NULL,

                        FOREIGN KEY (company_id) REFERENCES companies(company_id)

                    );

                """)

                conn.commit()

            logger.info("✅ SQLite workspace store initialised")

        except Exception as exc:

            logger.error(f"❌ SQLite workspace store init error: {exc}")



    # ------------------------------------------------------------------

    # Save / load a user's workspace (folder or single-sheet connection)

    # ------------------------------------------------------------------



    def save_workspace(

        self,

        telegram_id: int,

        *,

        mode: str,                              # "folder" | "sheets"

        folder_id: str | None = None,

        folder_url: str | None = None,

        sheet_id: str | None = None,

        sheet_name: str | None = None,

        folder_spreadsheets: list | None = None,

        selected_spreadsheets: list | None = None,

    ) -> int:

        """Upsert a company row for this admin and return company_id.

        Saves to SQLite first (fast), then mirrors to Supabase (persistent)."""

        now = datetime.now(timezone.utc).isoformat()

        fs_json  = json.dumps(folder_spreadsheets or [])

        sel_json = json.dumps(selected_spreadsheets or [])

        company_id = -1



        # 1. SQLite

        try:

            with self._connect() as conn:

                conn.execute("""

                    INSERT INTO companies

                        (admin_user_id, folder_id, folder_url, sheet_id, sheet_name,

                         mode, folder_spreadsheets, selected_spreadsheets, connected_at)

                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)

                    ON CONFLICT(admin_user_id) DO UPDATE SET

                        folder_id              = excluded.folder_id,

                        folder_url             = excluded.folder_url,

                        sheet_id               = excluded.sheet_id,

                        sheet_name             = excluded.sheet_name,

                        mode                   = excluded.mode,

                        folder_spreadsheets    = excluded.folder_spreadsheets,

                        selected_spreadsheets  = excluded.selected_spreadsheets,

                        connected_at           = excluded.connected_at

                """, (telegram_id, folder_id, folder_url, sheet_id, sheet_name,

                      mode, fs_json, sel_json, now))

                row = conn.execute(

                    "SELECT company_id FROM companies WHERE admin_user_id = ?",

                    (telegram_id,)

                ).fetchone()

                company_id = row["company_id"]

                conn.execute("""

                    INSERT INTO company_users (user_id, company_id, joined_at)

                    VALUES (?, ?, ?)

                    ON CONFLICT(user_id) DO UPDATE SET

                        company_id = excluded.company_id,

                        joined_at  = excluded.joined_at

                """, (telegram_id, company_id, now))

                conn.commit()

            logger.info(f"💾 SQLite workspace saved for user {telegram_id} (company {company_id})")

        except Exception as exc:

            logger.warning(f"⚠️ SQLite save_workspace error: {exc}")



        # 2. Supabase mirror (belt-and-suspenders, survives container wipes)

        try:

            from supabase import create_client as _sc

            _sb = _sc(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_ANON_KEY"))

            payload = {

                "admin_user_id": telegram_id,

                "folder_id": folder_id,

                "folder_url": folder_url,

                "sheet_id": sheet_id,

                "sheet_name": sheet_name,

                "mode": mode,

                "folder_spreadsheets": fs_json,

                "selected_spreadsheets": sel_json,

                "connected_at": now,

            }

            resp = _sb.table("workspaces").upsert(payload, on_conflict="admin_user_id").execute()

            if resp.data:

                sb_cid = resp.data[0].get("company_id") or resp.data[0].get("id")

                logger.info(f"💾 Supabase workspace saved for user {telegram_id}")

                # If SQLite didn't assign a company_id, use Supabase's

                if company_id < 0 and sb_cid:

                    company_id = int(sb_cid)

        except Exception as exc:

            logger.warning(f"⚠️ Supabase save_workspace error (SQLite backup ok): {exc}")



        return company_id



    def load_workspace(self, telegram_id: int) -> dict | None:

        """Return the saved workspace dict for this user, or None.

        Tries SQLite first, falls back to Supabase, backfills SQLite."""

        # 1. SQLite

        try:

            with self._connect() as conn:

                row = conn.execute("""

                    SELECT c.*

                    FROM companies c

                    JOIN company_users cu ON cu.company_id = c.company_id

                    WHERE cu.user_id = ?

                    ORDER BY c.connected_at DESC

                    LIMIT 1

                """, (telegram_id,)).fetchone()

            if not row:

                with self._connect() as conn:

                    row = conn.execute(

                        "SELECT * FROM companies WHERE admin_user_id = ? ORDER BY connected_at DESC LIMIT 1",

                        (telegram_id,)

                    ).fetchone()

            if row:

                return dict(row)

        except Exception as exc:

            logger.warning(f"⚠️ SQLite load_workspace error: {exc}")



        # 2. Supabase fallback

        try:

            from supabase import create_client as _sc

            _sb = _sc(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_ANON_KEY"))

            resp = _sb.table("workspaces").select("*").eq("admin_user_id", telegram_id).order("connected_at", desc=True).limit(1).execute()

            if resp.data:

                ws = resp.data[0]

                logger.info(f"✅ Supabase📊SQLite workspace backfill for user {telegram_id}")

                # Backfill SQLite

                try:

                    now = datetime.now(timezone.utc).isoformat()

                    with self._connect() as conn:

                        conn.execute("""

                            INSERT INTO companies

                                (admin_user_id, folder_id, folder_url, sheet_id, sheet_name,

                                 mode, folder_spreadsheets, selected_spreadsheets, connected_at)

                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)

                            ON CONFLICT(admin_user_id) DO UPDATE SET

                                folder_id=excluded.folder_id, folder_url=excluded.folder_url,

                                sheet_id=excluded.sheet_id, sheet_name=excluded.sheet_name,

                                mode=excluded.mode, folder_spreadsheets=excluded.folder_spreadsheets,

                                selected_spreadsheets=excluded.selected_spreadsheets,

                                connected_at=excluded.connected_at

                        """, (telegram_id, ws.get("folder_id"), ws.get("folder_url"),

                              ws.get("sheet_id"), ws.get("sheet_name"), ws.get("mode", "sheets"),

                              ws.get("folder_spreadsheets", "[]"), ws.get("selected_spreadsheets", "[]"),

                              ws.get("connected_at", now)))

                        sqlite_row = conn.execute(

                            "SELECT company_id FROM companies WHERE admin_user_id = ?", (telegram_id,)

                        ).fetchone()

                        if sqlite_row:

                            conn.execute("""

                                INSERT INTO company_users (user_id, company_id, joined_at)

                                VALUES (?, ?, ?)

                                ON CONFLICT(user_id) DO UPDATE SET company_id=excluded.company_id

                            """, (telegram_id, sqlite_row["company_id"], now))

                        conn.commit()

                except Exception as bf_exc:

                    logger.debug(f"SQLite backfill error: {bf_exc}")

                return ws

        except Exception as exc:

            logger.warning(f"⚠️ Supabase load_workspace error: {exc}")



        return None



    def delete_workspace(self, telegram_id: int) -> None:

        """Remove workspace + cache for this user (used by /disconnect)."""

        # 1. SQLite

        try:

            with self._connect() as conn:

                row = conn.execute(

                    "SELECT company_id FROM companies WHERE admin_user_id = ?",

                    (telegram_id,)

                ).fetchone()

                if row:

                    company_id = row["company_id"]

                    conn.execute("DELETE FROM sheets_cache WHERE company_id = ?", (company_id,))

                    conn.execute("DELETE FROM company_users WHERE company_id = ?", (company_id,))

                    conn.execute("DELETE FROM companies WHERE company_id = ?", (company_id,))

                else:

                    conn.execute("DELETE FROM company_users WHERE user_id = ?", (telegram_id,))

                conn.commit()

            logger.info(f"🗑️П SQLite workspace deleted for user {telegram_id}")

        except Exception as exc:

            logger.warning(f"⚠️ SQLite delete_workspace error: {exc}")



        # 2. Supabase

        try:

            from supabase import create_client as _sc

            _sb = _sc(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_ANON_KEY"))

            _sb.table("workspaces").delete().eq("admin_user_id", telegram_id).execute()

            _sb.table("sheets_cache_sb").delete().eq("admin_user_id", telegram_id).execute()

            logger.info(f"🗑️П Supabase workspace deleted for user {telegram_id}")

        except Exception as exc:

            logger.warning(f"⚠️ Supabase delete_workspace error: {exc}")



    # ------------------------------------------------------------------

    # Sheet data cache

    # ------------------------------------------------------------------



    def save_cache(self, company_id: int, cache_key: str, sheet_name: str, data: Any, telegram_id: int = 0) -> None:

        """Save spreadsheet data as JSON to SQLite + Supabase."""

        now = datetime.now(timezone.utc).isoformat()

        try:

            data_json = json.dumps(data, ensure_ascii=False)

        except Exception:

            data_json = "[]"



        # 1. SQLite

        try:

            with self._connect() as conn:

                conn.execute("""

                    INSERT INTO sheets_cache (cache_key, company_id, sheet_name, data, last_updated)

                    VALUES (?, ?, ?, ?, ?)

                    ON CONFLICT(cache_key) DO UPDATE SET

                        data         = excluded.data,

                        sheet_name   = excluded.sheet_name,

                        last_updated = excluded.last_updated

                """, (cache_key, company_id, sheet_name, data_json, now))

                conn.commit()

            logger.debug(f"💾 SQLite cache saved: {cache_key}")

        except Exception as exc:

            logger.warning(f"⚠️ SQLite save_cache error: {exc}")



        # 2. Supabase (only save if we have a telegram_id to use as owner key)

        if telegram_id:

            try:

                from supabase import create_client as _sc

                _sb = _sc(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_ANON_KEY"))

                _sb.table("sheets_cache_sb").upsert({

                    "cache_key": cache_key,

                    "admin_user_id": telegram_id,

                    "sheet_name": sheet_name,

                    "data": data_json,

                    "last_updated": now,

                }, on_conflict="cache_key").execute()

                logger.debug(f"💾 Supabase cache saved: {cache_key}")

            except Exception as exc:

                logger.debug(f"⚠️ Supabase save_cache error (SQLite ok): {exc}")



    def load_all_cache(self, company_id: int, telegram_id: int = 0) -> dict[str, Any]:

        """Load all cached sheet data. Tries SQLite first, falls back to Supabase."""

        result: dict[str, Any] = {}



        # 1. SQLite

        try:

            with self._connect() as conn:

                rows = conn.execute(

                    "SELECT cache_key, data FROM sheets_cache WHERE company_id = ?",

                    (company_id,)

                ).fetchall()

            for row in rows:

                try:

                    result[row["cache_key"]] = json.loads(row["data"])

                except Exception:

                    pass

        except Exception as exc:

            logger.warning(f"⚠️ SQLite load_all_cache error: {exc}")



        if result:

            return result



        # 2. Supabase fallback

        if telegram_id:

            try:

                from supabase import create_client as _sc

                _sb = _sc(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_ANON_KEY"))

                resp = _sb.table("sheets_cache_sb").select("cache_key, data, sheet_name").eq("admin_user_id", telegram_id).execute()

                if resp.data:

                    logger.info(f"✅ Supabase📊SQLite cache backfill for user {telegram_id}: {len(resp.data)} entries")

                    for entry in resp.data:

                        key = entry["cache_key"]

                        try:

                            parsed = json.loads(entry["data"])

                            result[key] = parsed

                            # Backfill SQLite

                            try:

                                with self._connect() as conn:

                                    conn.execute("""

                                        INSERT INTO sheets_cache (cache_key, company_id, sheet_name, data, last_updated)

                                        VALUES (?, ?, ?, ?, ?)

                                        ON CONFLICT(cache_key) DO UPDATE SET

                                            data=excluded.data, sheet_name=excluded.sheet_name,

                                            last_updated=excluded.last_updated

                                    """, (key, company_id, entry.get("sheet_name", ""), entry["data"],

                                          datetime.now(timezone.utc).isoformat()))

                                    conn.commit()

                            except Exception:

                                pass

                        except Exception:

                            pass

            except Exception as exc:

                logger.warning(f"⚠️ Supabase load_all_cache error: {exc}")



        return result





# Singleton workspace store

_workspace_store = WorkspaceStore()

_workspace_store.init_db()





def save_google_token(telegram_id: int, credentials_json: str) -> None:

    """Save Google credentials to BOTH SQLite (primary) and Supabase (backup)."""

    # 1. SQLite 🔧 fast, always available

    _token_store.save(telegram_id, credentials_json)

    # 2. Supabase 🔧 persistent across container re-creations

    try:

        from supabase import create_client as _sc

        _sb = _sc(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_ANON_KEY"))

        resp = _sb.table("users").select("telegram_id").eq("telegram_id", telegram_id).execute()

        if resp.data:

            _sb.table("users").update({

                "google_credentials": credentials_json,

                "google_credentials_updated_at": datetime.now(timezone.utc).isoformat(),

            }).eq("telegram_id", telegram_id).execute()

            logger.info(f"💾 Supabase: saved token for user {telegram_id}")

        else:

            logger.warning(f"⚠️ Supabase: user {telegram_id} not found, token not saved to Supabase")

    except Exception as exc:

        logger.warning(f"⚠️ Supabase token save error (SQLite backup still worked): {exc}")





def load_google_token(telegram_id: int) -> str | None:

    """Load Google credentials 🔧 tries SQLite first, then Supabase."""

    creds_json = _token_store.load(telegram_id)

    if creds_json:

        return creds_json

    # Fall back to Supabase

    try:

        from supabase import create_client as _sc

        _sb = _sc(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_ANON_KEY"))

        resp = _sb.table("users").select("google_credentials").eq("telegram_id", telegram_id).execute()

        if resp.data and resp.data[0].get("google_credentials"):

            creds_json = resp.data[0]["google_credentials"]

            # Backfill SQLite so next call is fast

            _token_store.save(telegram_id, creds_json)

            logger.info(f"✅ Supabase📊SQLite backfill for user {telegram_id}")

            return creds_json

    except Exception as exc:

        logger.warning(f"⚠️ Supabase token load error: {exc}")

    return None





def load_and_refresh_google_token(telegram_id: int) -> str | None:

    """Load token, auto-refresh if expired, save refreshed token, return JSON or None.



    Returns ``None`` only when:

      - No token exists anywhere (user has never authenticated), OR

      - The refresh_token itself is expired/revoked (user must re-authenticate).

    """

    creds_json = load_google_token(telegram_id)

    if not creds_json:

        return None

    try:

        data = json.loads(creds_json)

        creds = Credentials.from_authorized_user_info(data, scopes=SCOPES)



        # Check for old read-only scopes 📊 force re-auth

        stored_scopes = data.get("scopes", [])

        if any("readonly" in s for s in stored_scopes):

            logger.warning(f"⚠️ Old read-only scopes for user {telegram_id}. Re-auth required.")

            return None



        if not creds.valid:

            if creds.expired and creds.refresh_token:

                logger.info(f"📍 Token expired for user {telegram_id}, refreshing...")

                creds.refresh(GoogleAuthRequest())

                refreshed_json = creds.to_json()

                # Persist the freshly-issued token immediately

                save_google_token(telegram_id, refreshed_json)

                logger.info(f"✅ Token refreshed and saved for user {telegram_id}")

                return refreshed_json

            else:

                # No refresh_token or some other invalidity

                logger.warning(f"⚠️ Token invalid and cannot be refreshed for user {telegram_id}")

                return None

        return creds_json

    except Exception as exc:

        logger.warning(f"⚠️ load_and_refresh_google_token error for user {telegram_id}: {exc}")

        return None





SCOPES = [

    "https://www.googleapis.com/auth/spreadsheets",  # Full read/write access to Sheets

    "https://www.googleapis.com/auth/drive",         # Full read/write access to Drive (folders, files, etc)

]

# REDIRECT_URI is now set from config - see Config class

MAIN_MENU_SHEETS = "📊 Google Sheets ulash"

MAIN_MENU_EXCEL = "📁 Upload Excel File"

MAX_ROWS_FOR_CONTEXT = 1000

MAX_COLS_FOR_CONTEXT = 50

MAX_CHARS_CONTEXT = 120000





@dataclass

class Config:

    bot_token: str

    tavily_api_key: str = ""

    # Google OAuth (optional)

    google_client_id: str = ""

    google_client_secret: str = ""

    # Database (optional - SQLite is primary)

    supabase_url: str = ""

    supabase_anon_key: str = ""

    # Grok AI (xAI) - for intelligent spreadsheet Q&A

    grok_api_key: str = ""

    # Groq AI - for voice transcription (Whisper)
    # OpenAI - for voice transcription (Whisper)
    openai_whisper_key: str = ""

    # Google Service Account — for Drive folder access without OAuth
    # Set GOOGLE_SA_CREDENTIALS env var to the full JSON key string
    google_sa_credentials: str = ""

    # Server configuration - can be overridden via env vars

    server_host: str = "0.0.0.0"  # Listen on all interfaces for production

    server_port: int = 8080  # Koyeb / Render both use 8080 by default

    google_redirect_uri: str = "https://onbrain.koyeb.app/"  # For Google OAuth



    @classmethod

    def from_env(cls) -> "Config":

        load_dotenv()

        required = {

            "BOT_TOKEN": os.getenv("BOT_TOKEN", "").strip(),

        }

        missing = [k for k, v in required.items() if not v]

        if missing:

            raise RuntimeError(
                "BOT_TOKEN env o'zgaruvchisi o'rnatilmagan. Please add it in Koyeb settings."
            )















        # Optional server configuration

        server_host = os.getenv("SERVER_HOST", "0.0.0.0").strip()

        # Koyeb and Render both inject PORT env var; fallback to 8080

        server_port = int(os.getenv("PORT", os.getenv("SERVER_PORT", "8080")))

        

        # Google OAuth redirect URI

        google_redirect_uri = os.getenv("GOOGLE_REDIRECT_URI", "").strip()

        if not google_redirect_uri:

            domain = os.getenv("APP_DOMAIN", "onbrain.koyeb.app").strip()

            if domain == "localhost":

                google_redirect_uri = f"http://localhost:{server_port}/"

            else:

                google_redirect_uri = f"https://{domain}/"

        

        # Optional: Grok AI key for spreadsheet Q&A

        grok_api_key = os.getenv("GROK_API_KEY", "").strip()

        # Optional: Groq AI key for voice transcription
        # Optional: OpenAI key for voice transcription (Whisper)
        openai_whisper_key = os.getenv("OPENAI_API_KEY", "").strip()

        

        return cls(

            bot_token=required["BOT_TOKEN"],

            tavily_api_key=os.getenv("TAVILY_API_KEY", "").strip(),

            google_client_id=os.getenv("GOOGLE_CLIENT_ID", "").strip(),

            google_client_secret=os.getenv("GOOGLE_CLIENT_SECRET", "").strip(),

            supabase_url=os.getenv("SUPABASE_URL", "").strip(),

            supabase_anon_key=os.getenv("SUPABASE_ANON_KEY", "").strip(),

            grok_api_key=grok_api_key,

            openai_whisper_key=openai_whisper_key,

            google_sa_credentials=os.getenv("GOOGLE_SA_CREDENTIALS", "").strip(),

            server_host=server_host,

            server_port=server_port,

            google_redirect_uri=google_redirect_uri,

        )





@dataclass

class UserSession:

    step: str = "idle"

    full_name: str | None = None

    email: str | None = None

    phone_number: str | None = None

    sheet_id: str | None = None

    sheet_name: str | None = None

    sheet_data: list[list[Any]] = field(default_factory=list)

    excel_data: list[list[Any]] = field(default_factory=list)  # Active file rows (backward compat)

    # ── Multi-file support ──────────────────────────────────────────────
    # Maps filename -> rows. Lets users keep multiple uploaded Excels.
    # Active file is pointed to by active_excel_name + excel_data.
    excel_files: dict[str, list[list[Any]]] = field(default_factory=dict)
    active_excel_name: str | None = None  # Key into excel_files

    all_sheets_data: dict[str, list[list[Any]]] = field(default_factory=dict)  # All sheets from Google Sheets

    google_credentials_json: str | None = None

    pending_sheets: dict[str, str] = field(default_factory=dict)

    # For folder mode

    folder_spreadsheets: list[dict[str, str]] = field(default_factory=list)  # List of {id, name, url}

    selected_spreadsheets: list[str] = field(default_factory=list)  # Selected sheet IDs

    all_folder_sheets_data: dict[str, dict[str, list[list[Any]]]] = field(default_factory=dict)  # sheet_id -> {sheet_name -> data}

    auth_mode: str | None = None  # Track which button was clicked: "sheets" or "folder"

    # Data Indexing

    indexing_service: Any = None  # DataIndexingService instance

    folder_id: str | None = None  # Current folder ID being indexed

    web_search_mode: bool = False  # When True, bypass spreadsheet and use Tavily

    chat_history: list = field(default_factory=list)  # List of {"role": ..., "content": ...} for conversation context





class SessionStore:

    def __init__(self, timeout_seconds: int = 86400) -> None:  # 24 hours

        self._store: dict[int, UserSession] = {}

        self._timestamps: dict[int, float] = {}

        self.timeout_seconds = timeout_seconds

        # Warn users 30 min before session expires (tracks who was already warned)
        self._warned_expiry: set[int] = set()
        self.expiry_warning_seconds: int = 1800  # 30 minutes



    def get(self, telegram_id: int) -> UserSession:

        # Check if session exists and is not expired

        if telegram_id in self._timestamps:

            if time.time() - self._timestamps[telegram_id] > self.timeout_seconds:

                logger.warning(f"⚙️░ Session expired for user {telegram_id}")

                self._store.pop(telegram_id, None)

                self._timestamps.pop(telegram_id, None)

        

        if telegram_id not in self._store:

            self._store[telegram_id] = UserSession()

            self._timestamps[telegram_id] = time.time()

        

        # ---------------------------------------------------------------

        # Load Google credentials if not already in memory.

        # ---------------------------------------------------------------

        if not self._store[telegram_id].google_credentials_json:

            refreshed = load_and_refresh_google_token(telegram_id)

            if refreshed:

                self._store[telegram_id].google_credentials_json = refreshed

                logger.info(f"✅ Loaded (and refreshed if needed) Google token for user {telegram_id}")



            # ---------------------------------------------------------------

            # Restore workspace (folder / sheet connection) from DB.

            # ---------------------------------------------------------------

            self._restore_workspace(telegram_id)



        else:

            # Update timestamp on access

            self._timestamps[telegram_id] = time.time()

            # Also restore workspace if data is missing (e.g. after partial session)

            sess = self._store[telegram_id]

            # Skip restore if Excel data is loaded — Excel is memory-only (never in DB)
            if not sess.all_folder_sheets_data and not sess.all_sheets_data and not sess.excel_data:

                self._restore_workspace(telegram_id)

        

        return self._store[telegram_id]



    def _restore_workspace(self, telegram_id: int) -> None:

        """Silently restore folder/sheet connection from SQLite workspace store.
        Also restores any previously uploaded Excel files from the excel_files table."""

        sess = self._store[telegram_id]

        # Skip if data is already in memory

        if sess.all_folder_sheets_data or sess.all_sheets_data:

            # Still load Excel files even if Google Sheets already restored
            if not sess.excel_files:
                loaded = self.load_all_excel_from_db(telegram_id)
                if loaded:
                    sess.excel_files = loaded
                    # Restore active file: use the most recently saved one
                    last_name = list(loaded.keys())[-1]
                    if not sess.excel_data:  # only if not already in memory
                        sess.excel_data = loaded[last_name]
                        sess.active_excel_name = last_name
            return

        try:

            ws = _workspace_store.load_workspace(telegram_id)

            if not ws:

                return

            company_id = ws["company_id"]

            mode = ws.get("mode", "sheets")

            sess.folder_id = ws.get("folder_id")

            sess.sheet_id  = ws.get("sheet_id")

            sess.sheet_name = ws.get("sheet_name")

            # Restore spreadsheet list metadata

            if ws.get("folder_spreadsheets"):

                try:

                    sess.folder_spreadsheets = json.loads(ws["folder_spreadsheets"])

                except Exception:

                    pass

            if ws.get("selected_spreadsheets"):

                try:

                    sess.selected_spreadsheets = json.loads(ws["selected_spreadsheets"])

                except Exception:

                    pass

            # Restore cached sheet data

            cached = _workspace_store.load_all_cache(company_id, telegram_id=telegram_id)

            if mode == "folder" and cached:

                # Rebuild all_folder_sheets_data: {sheet_id: {sheet_title: rows}}

                folder_data: dict[str, dict[str, list]] = {}

                for cache_key, data in cached.items():

                    # cache_key format: "folder:{sheet_id}:{sheet_title}"

                    if cache_key.startswith("folder:"):

                        parts = cache_key.split(":", 2)

                        if len(parts) == 3:

                            _, sid, stitle = parts

                            if sid not in folder_data:

                                folder_data[sid] = {}

                            folder_data[sid][stitle] = data

                if folder_data:

                    sess.all_folder_sheets_data = folder_data

                    sess.step = "in_chat"

                    logger.info(f"✅ Restored folder workspace for user {telegram_id}: {len(folder_data)} spreadsheets")

            elif mode == "sheets" and cached:

                # Rebuild all_sheets_data: {sheet_title: rows}

                sheets_data: dict[str, list] = {}

                for cache_key, data in cached.items():

                    if cache_key.startswith("sheet:"):

                        parts = cache_key.split(":", 2)

                        if len(parts) == 3:

                            _, _, stitle = parts

                            sheets_data[stitle] = data

                if sheets_data:

                    sess.all_sheets_data = sheets_data

                    sess.step = "in_chat"

                    logger.info(f"✅ Restored sheet workspace for user {telegram_id}: {len(sheets_data)} tabs")

        except Exception as exc:

            logger.warning(f"⚠️ _restore_workspace error for user {telegram_id}: {exc}")

    

    def get_expiry_warnings(self) -> list[int]:
        """Return user IDs whose session expires within expiry_warning_seconds
        and who have not been warned yet. Call this from the cleanup loop."""
        now = time.time()
        warn_ids: list[int] = []
        for uid, ts in self._timestamps.items():
            time_left = self.timeout_seconds - (now - ts)
            if 0 < time_left <= self.expiry_warning_seconds and uid not in self._warned_expiry:
                warn_ids.append(uid)
                self._warned_expiry.add(uid)
        return warn_ids

    # ── Excel persistence (SQLite) ────────────────────────────────────────
    # Structured so it can be swapped for Redis/PostgreSQL later by
    # replacing these two methods only.

    def save_excel_to_db(self, telegram_id: int, filename: str, rows: list) -> None:
        """Persist an Excel file's rows to SQLite so they survive session expiry."""
        import json as _json, sqlite3 as _sq3
        db_path = os.environ.get("SQLITE_TOKEN_DB", "google_tokens.db")
        try:
            with _sq3.connect(db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS excel_files (
                        telegram_id INTEGER NOT NULL,
                        filename    TEXT    NOT NULL,
                        rows_json   TEXT    NOT NULL,
                        saved_at    TEXT    NOT NULL,
                        PRIMARY KEY (telegram_id, filename)
                    )
                """)
                conn.execute("""
                    INSERT INTO excel_files (telegram_id, filename, rows_json, saved_at)
                    VALUES (?, ?, ?, datetime('now'))
                    ON CONFLICT(telegram_id, filename) DO UPDATE SET
                        rows_json = excluded.rows_json,
                        saved_at  = excluded.saved_at
                """, (telegram_id, filename, _json.dumps(rows)))
            logger.info(f"💾 Excel persisted to DB: user={telegram_id} file={filename} rows={len(rows)}")
        except Exception as exc:
            logger.warning(f"⚠️ Could not save Excel to DB: {exc}")

    def load_all_excel_from_db(self, telegram_id: int) -> dict[str, list]:
        """Load all persisted Excel files for a user from SQLite.
        Returns {filename: rows}. Empty dict if nothing saved."""
        import json as _json, sqlite3 as _sq3
        db_path = os.environ.get("SQLITE_TOKEN_DB", "google_tokens.db")
        result: dict[str, list] = {}
        try:
            with _sq3.connect(db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS excel_files (
                        telegram_id INTEGER NOT NULL,
                        filename    TEXT    NOT NULL,
                        rows_json   TEXT    NOT NULL,
                        saved_at    TEXT    NOT NULL,
                        PRIMARY KEY (telegram_id, filename)
                    )
                """)
                rows = conn.execute(
                    "SELECT filename, rows_json FROM excel_files WHERE telegram_id = ?",
                    (telegram_id,)
                ).fetchall()
                for fname, rjson in rows:
                    result[fname] = _json.loads(rjson)
            if result:
                logger.info(f"📂 Loaded {len(result)} Excel file(s) from DB for user {telegram_id}")
        except Exception as exc:
            logger.warning(f"⚠️ Could not load Excel from DB: {exc}")
        return result

    def cleanup_expired(self) -> None:

        """Remove expired sessions"""

        now = time.time()

        expired = [

            uid for uid, ts in self._timestamps.items()

            if now - ts > self.timeout_seconds

        ]

        for uid in expired:

            self._store.pop(uid, None)

            self._timestamps.pop(uid, None)

        if expired:

            logger.info(f"🧹 Cleaned up {len(expired)} expired sessions")





class SupabaseService:

    def __init__(self, supabase_url: str, supabase_key: str) -> None:

        self.url = supabase_url.rstrip('/')

        self.key = supabase_key

        self.headers = {

            "apikey": supabase_key,

            "Authorization": f"Bearer {supabase_key}",

            "Content-Type": "application/json",

        }



    async def get_user_by_telegram(self, telegram_id: int) -> dict[str, Any] | None:

        return await asyncio.to_thread(self._get_user_by_telegram_sync, telegram_id)



    def _get_user_by_telegram_sync(self, telegram_id: int) -> dict[str, Any] | None:

        try:

            response = httpx.get(

                f"{self.url}/rest/v1/users?telegram_id=eq.{telegram_id}&limit=1",

                headers=self.headers,

            )

            data = response.json()

            return data[0] if data else None

        except Exception as exc:

            logger.error(f"Get user by telegram error: {exc}")

            return None



    async def get_user_by_email(self, email: str) -> dict[str, Any] | None:

        return await asyncio.to_thread(self._get_user_by_email_sync, email)



    def _get_user_by_email_sync(self, email: str) -> dict[str, Any] | None:

        try:

            response = httpx.get(

                f"{self.url}/rest/v1/users?email=eq.{email}&limit=1",

                headers=self.headers,

            )

            data = response.json()

            return data[0] if data else None

        except Exception as exc:

            logger.error(f"Get user by email error: {exc}")

            return None



    async def create_user(

        self, 

        telegram_id: int, 

        full_name: str, 

        email: str,

        phone_number: str | None = None,

    ) -> bool:

        """Create user - returns True if successful"""

        try:

            result = await asyncio.to_thread(

                self._create_user_sync, 

                telegram_id, 

                full_name, 

                email,

                phone_number,

            )

            return result

        except Exception as exc:

            logger.error(f"create_user async wrapper error: {exc}")

            return False



    def _create_user_sync(

        self, 

        telegram_id: int, 

        full_name: str, 

        email: str,

        phone_number: str | None = None,

    ) -> bool:

        """Create user in Supabase - returns True if successful"""

        try:

            payload = {

                "telegram_id": telegram_id,

                "first_name": full_name.split()[0] if full_name else "User",

                "last_name": " ".join(full_name.split()[1:]) if len(full_name.split()) > 1 else "",

                "email": email,

            }

            # Add phone_number if provided

            if phone_number:

                payload["phone_number"] = phone_number

            

            logger.info(f"📤 Creating user {telegram_id} with payload: {payload}")

            

            response = httpx.post(

                f"{self.url}/rest/v1/users",

                headers=self.headers,

                json=payload,

            )

            

            logger.info(f"📊 Supabase response status: {response.status_code}")

            

            # Check for status code errors

            if response.status_code >= 400:

                error_detail = response.text

                logger.error(f"Supabase error {response.status_code}: {error_detail}")

                

                # If phone_number is causing issues, retry without it

                if "phone_number" in error_detail.lower() and phone_number:

                    logger.warning(f"⚠️  Phone number field error, retrying without phone_number...")

                    payload.pop("phone_number", None)

                    response = httpx.post(

                        f"{self.url}/rest/v1/users",

                        headers=self.headers,

                        json=payload,

                    )

                    if response.status_code < 400:

                        logger.info(f"✅ User {telegram_id} created successfully (without phone)")

                        return True

                

                raise Exception(f"Supabase error: {error_detail}")

            

            response.raise_for_status()

            logger.info(f"✅ User {telegram_id} created successfully")

            return True

            

        except Exception as exc:

            logger.error(f"❌ Create user error: {exc}")

            return False



    async def save_integration(self, telegram_id: int, sheet_id: str, sheet_name: str) -> None:

        await asyncio.to_thread(self._save_integration_sync, telegram_id, sheet_id, sheet_name)



    def _save_integration_sync(self, telegram_id: int, sheet_id: str, sheet_name: str) -> None:

        try:

            # Deactivate old integrations

            httpx.patch(

                f"{self.url}/rest/v1/integrations?telegram_id=eq.{telegram_id}",

                headers=self.headers,

                json={"is_active": False},

            )

            # Create new integration

            payload = {

                "telegram_id": telegram_id,

                "sheet_id": sheet_id,

                "sheet_name": sheet_name,

                "is_active": True,

            }

            response = httpx.post(

                f"{self.url}/rest/v1/integrations",

                headers=self.headers,

                json=payload,

            )

            response.raise_for_status()

        except Exception as exc:

            logger.error(f"Save integration error: {exc}")

            raise



    async def get_active_integration(self, telegram_id: int) -> dict[str, Any] | None:

        return await asyncio.to_thread(self._get_active_integration_sync, telegram_id)



    def _get_active_integration_sync(self, telegram_id: int) -> dict[str, Any] | None:

        try:

            response = httpx.get(

                f"{self.url}/rest/v1/integrations?telegram_id=eq.{telegram_id}&is_active=eq.true&order=created_at.desc&limit=1",

                headers=self.headers,

            )

            data = response.json()

            return data[0] if data else None

        except Exception as exc:

            logger.error(f"Get active integration error: {exc}")

            return None



    async def save_message(self, telegram_id: int, question: str, answer: str) -> None:

        await asyncio.to_thread(self._save_message_sync, telegram_id, question, answer)



    def _save_message_sync(self, telegram_id: int, question: str, answer: str) -> None:

        payload = {

            "id": str(uuid.uuid4()),

            "telegram_id": telegram_id,

            "question": question,

            "answer": answer,

        }

        try:

            response = httpx.post(

                f"{self.url}/rest/v1/messages",

                headers=self.headers,

                json=payload,

            )

            response.raise_for_status()

        except Exception as exc:

            logger.warning(f"Failed to save message: {exc}")





class GoogleOAuthService:

    def __init__(self, client_id: str, client_secret: str, redirect_uri: str = "https://onbrain.koyeb.app/") -> None:

        self.client_config = {

            "web": {

                "client_id": client_id,

                "client_secret": client_secret,

                "auth_uri": "https://accounts.google.com/o/oauth2/auth",

                "token_uri": "https://oauth2.googleapis.com/token",

            }

        }

        self.redirect_uri = redirect_uri

        self.pending_flows: dict[str, dict[str, Any]] = {}



    def create_auth_url(self, telegram_id: int) -> str:

        flow = Flow.from_client_config(

            self.client_config, scopes=SCOPES, redirect_uri=self.redirect_uri

        )

        auth_url, state = flow.authorization_url(

            access_type="offline",

            include_granted_scopes="true",

            prompt="consent",

        )

        self.pending_flows[state] = {

            "flow": flow,

            "telegram_id": telegram_id,

            "created_at": time.time(),

        }

        logger.info(f"🔑 Created OAuth state for user {telegram_id}. Total pending: {len(self.pending_flows)}. State prefix: {state[:12]}...")

        self.cleanup_stale_flows()

        return auth_url



    def cleanup_stale_flows(self) -> None:

        now = time.time()

        # Increase timeout to 1 hour (3600 seconds) instead of 15 minutes

        # This prevents "OAuth state expired" errors if user takes time to authorize

        stale = [state for state, item in self.pending_flows.items() if now - item["created_at"] > 3600]

        for state in stale:

            self.pending_flows.pop(state, None)

            logger.debug(f"Cleaned up stale OAuth state (older than 1 hour)")



    def exchange_code(self, state: str, code: str) -> tuple[int, Credentials]:

        logger.info(f"🔑 Exchange code called. State prefix: {state[:12]}... Total pending: {len(self.pending_flows)}")

        if state not in self.pending_flows:

            logger.warning(f"❌ OAuth state not found. State prefix: {state[:12]}... Available states: {len(self.pending_flows)}")

            # Log available state prefixes for debugging

            for s in self.pending_flows:

                logger.warning(f"   Available state prefix: {s[:12]}...")

            raise ValueError("OAuth state not found or expired. Qaytadan urinib ko'ring (State not in pending flows)")

        flow_item = self.pending_flows.pop(state)

        flow: Flow = flow_item["flow"]

        flow.fetch_token(code=code)

        telegram_id: int = flow_item["telegram_id"]

        return telegram_id, flow.credentials





# ---------------------------------------------------------------------------

# Safe HTML message helpers 🔧 catch TelegramBadRequest and fall back to plain

# text so the bot never crashes on an unparseable HTML st

# ---------------------------------------------------------------------------



def _strip_html_tags(text: str) -> str:

    """Remove common HTML tags used in bot messages."""

    import re as _re

    return _re.sub(r"</?(?:b|i|u|s|code|pre|a)[^>]*>", "", text)





def _escape_url_for_html(url: str) -> str:

    """Escape a URL so it is safe inside HTML parse_mode messages.



    Telegram's HTML parser chokes on bare ``&`` (and ``<``, ``>``) inside

    attribute values and message text.  This helper encodes them.

    """

    return url.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")





async def _fetch_all_public_sheets(sheet_id: str) -> dict:
    """
    Fetch all tabs from a publicly shared Google Sheet without OAuth credentials.

    Strategy:
      1. Try the Sheets gviz HTML export to discover all tab names.
      2. For each tab, export as CSV and parse rows.
      3. Fall back to single-tab CSV if tab discovery fails.

    Returns dict[sheet_title -> list[list[str]]].
    Raises RuntimeError if the sheet cannot be read at all.
    """
    import csv
    import io as _io
    import re as _re
    import urllib.parse as _up
    import aiohttp as _aiohttp

    base_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
    result: dict = {}

    _timeout_short = _aiohttp.ClientTimeout(total=15)
    _timeout_long  = _aiohttp.ClientTimeout(total=30)

    async with _aiohttp.ClientSession() as http:
        # Step 1: discover tab names via gviz/tq endpoint (more reliable than HTML export)
        tab_names: list = []
        try:
            # Try gviz endpoint first - returns sheet metadata
            gviz_url = f"{base_url}/gviz/tq?tqx=out:json"
            async with http.get(gviz_url, timeout=_timeout_short, allow_redirects=True) as resp:
                if resp.status in (401, 403):
                    raise RuntimeError("Jadval yopiq. Share it as 'Anyone with the link'.")
                elif resp.status == 404:
                    raise RuntimeError("Jadval topilmadi. Check the link.")
                elif resp.status == 200:
                    pass  # OK, continue with HTML tab discovery
        except RuntimeError:
            raise
        except Exception as _e:
            logger.debug("gviz check failed (non-fatal): %s", _e)

        # Try HTML export to discover tab names
        try:
            html_url = f"{base_url}/export?format=html"
            async with http.get(html_url, timeout=_timeout_short, allow_redirects=True) as resp:
                if resp.status == 200:
                    html_text = await resp.text(errors="replace")
                    found = _re.findall(
                        r'<li[^>]+data-sheet-index[^>]*>.*?<span[^>]*>(.*?)</span>',
                        html_text, _re.DOTALL
                    )
                    if found:
                        tab_names = [t.strip() for t in found if t.strip()]
                elif resp.status in (401, 403):
                    raise RuntimeError("Jadval yopiq. Share it as 'Anyone with the link'.")
                elif resp.status == 404:
                    raise RuntimeError("Jadval topilmadi. Check the link.")
        except RuntimeError:
            raise
        except Exception as _e:
            logger.debug("Tab discovery failed (non-fatal): %s", _e)

        if not tab_names:
            tab_names = ["Sheet1"]

        # Step 2: fetch each tab as CSV
        any_ok = False
        for tab in tab_names:
            try:
                csv_url = f"{base_url}/export?format=csv&sheet={_up.quote(tab)}"
                async with http.get(csv_url, timeout=_timeout_long, allow_redirects=True) as resp:
                    if resp.status == 200:
                        raw = await resp.read()
                        text = raw.decode("utf-8", errors="replace")
                        rows = list(csv.reader(_io.StringIO(text)))
                        if rows:
                            result[tab] = rows
                            any_ok = True
                            logger.info(
                                "Fetched tab '%s' from sheet %s (%d rows)", tab, sheet_id, len(rows)
                            )
                    elif resp.status in (401, 403) and not any_ok:
                        raise RuntimeError("Jadval yopiq. Share it as 'Anyone with the link'.")
                    else:
                        logger.warning(
                            "CSV export status %s for tab '%s' sheet %s", resp.status, tab, sheet_id
                        )
            except RuntimeError:
                raise
            except Exception as _te:
                logger.warning("Failed to fetch tab '%s': %s", tab, _te)

        if not any_ok:
            raise RuntimeError(
                f"Could not read any tab from sheet {sheet_id}. "
                "Jadval 'Havolaga ega har kim' sifatida ulashilganligiga ishonch hosil qiling."
            )

    return result


def _extract_sheet_id(text: str) -> str | None:

    """

    Extract Google Sheet ID from various URL formats.

    """

    if not text:

        return None



    text = text.strip()



    # Remove /edit, /share, /export and query params from the end

    text = re.sub(r'(/edit.*|/share.*|/export.*|\?usp.*)$', '', text)



    # 1. Standard docs.google.com/spreadsheets/d/<ID> URLs

    m = re.search(r'docs\.google\.com/spreadsheets/d/([a-zA-Z0-9_-]{10,})', text)

    if m:

        return m.group(1)



    # 2. Published "d/e/<pubid>" links — strip the "e/" prefix, use the long ID

    m = re.search(r'docs\.google\.com/spreadsheets/d/e/([a-zA-Z0-9_-]{10,})', text)

    if m:

        return m.group(1)



    # 3. Sheets API URL  sheets.googleapis.com/v4/spreadsheets/<ID>

    m = re.search(r'sheets\.googleapis\.com/v4/spreadsheets/([a-zA-Z0-9_-]{10,})', text)

    if m:

        return m.group(1)



    # 4. User pasted a raw sheet ID (no URL around it)

    m = re.match(r'^([a-zA-Z0-9_-]{20,})$', text)

    if m:

        return m.group(1)



    return None



    text = text.strip()



    # 1. Standard docs.google.com/spreadsheets/d/<ID> URLs

    m = re.search(r'docs\.google\.com/spreadsheets/d/([a-zA-Z0-9_-]{10,})', text)

    if m:

        return m.group(1)



    # 2. Published "d/e/<pubid>" links — strip the "e/" prefix, use the long ID

    m = re.search(r'docs\.google\.com/spreadsheets/d/e/([a-zA-Z0-9_-]{10,})', text)

    if m:

        return m.group(1)



    # 3. Sheets API URL  sheets.googleapis.com/v4/spreadsheets/<ID>

    m = re.search(r'sheets\.googleapis\.com/v4/spreadsheets/([a-zA-Z0-9_-]{10,})', text)

    if m:

        return m.group(1)



    # 4. User pasted a raw sheet ID (no URL around it)

    m = re.match(r'^([a-zA-Z0-9_-]{20,})$', text)

    if m:

        return m.group(1)



    return None





def _looks_like_sheets_url(text: str) -> bool:

    """Return True if *text* looks like it could be a Google Sheets link,

    even if we can't extract a sheet ID from it.  Used to give the user a

    more helpful error instead of 'Bu Google Sheets linki emas'.

    """

    lower = text.lower()

    return any(kw in lower for kw in (

        "docs.google.com/spreadsheets",

        "sheets.googleapis.com",

        "spreadsheets/d/",

        "sheets.google.com",

        "google.com/spreadsheets",

    ))





async def safe_send(target, text: str, *, parse_mode: str = "HTML", **kwargs):

    """Send *text* via ``target.answer`` (Message) or ``target.edit_text``

    (CallbackQuery.message).  If the Telegram API rejects the HTML, retry

    once with the tags stripped and ``parse_mode`` removed so the user still

    gets a response instead of an error.



    *target* 🔧 ``Message`` or the ``.message`` attribute of a ``CallbackQuery``

    """

    send_fn = getattr(target, "answer", None) or getattr(target, "edit_text", None)

    if send_fn is None:

        raise TypeError(f"Cannot send to {type(target)}")

    try:

        return await send_fn(text, parse_mode=parse_mode, **kwargs)

    except (TelegramBadRequest, TelegramAPIError) as exc:

        logger.warning("HTML parse failed, falling back to plain text: %s", exc)

        plain = _strip_html_tags(text)

        try:

            return await send_fn(plain, **kwargs)

        except Exception as inner:

            logger.error("Plain-text fallback also failed: %s", inner)





async def safe_edit(msg, text: str, *, parse_mode: str = "HTML", **kwargs):

    """Like safe_send but specifically for ``callback.message.edit_text``."""

    try:

        return await msg.edit_text(text, parse_mode=parse_mode, **kwargs)

    except (TelegramBadRequest, TelegramAPIError) as exc:

        logger.warning("HTML parse failed on edit, falling back: %s", exc)

        plain = _strip_html_tags(text)

        try:

            return await msg.edit_text(plain, **kwargs)

        except Exception as inner:

            logger.error("Plain-text edit fallback also failed: %s", inner)





def build_main_menu() -> InlineKeyboardMarkup:

    """Build main menu with Chat, Sheets, Excel, and Folder buttons"""

    return InlineKeyboardMarkup(

        inline_keyboard=[

            [InlineKeyboardButton(

                text="💬 Chat with AI",

                callback_data="chat_start"

            )],

            [InlineKeyboardButton(text="📊 Google Sheets", callback_data="sheets")],

            [InlineKeyboardButton(text="📁 Google Drive", callback_data="folder")],

            [InlineKeyboardButton(text="📄 Upload Excel", callback_data="excel")],

        ]

    )





def build_assistant_keyboard() -> InlineKeyboardMarkup:

    """Build keyboard with Assistant chat and main menu buttons"""

    return InlineKeyboardMarkup(

        inline_keyboard=[

            [InlineKeyboardButton(text="💬 Davom etish", callback_data="chat_start")],

            [InlineKeyboardButton(text="🏠 Main Menu", callback_data="main_menu")],

        ]

    )





def build_chat_response_keyboard() -> InlineKeyboardMarkup:

    """Build keyboard for chat responses with continue and exit buttons"""

    return InlineKeyboardMarkup(

        inline_keyboard=[

            [InlineKeyboardButton(text="💬 Davom etish", callback_data="chat_continue"),

             InlineKeyboardButton(text="🎤 Ovozli savol", callback_data="voice_hint")],

            [InlineKeyboardButton(text="🌐 Internet qidiruv", callback_data="web_search_mode")],

            [InlineKeyboardButton(text="🚪 Chatdan chiqish", callback_data="exit_chat")]

        ]

    )





def build_retry_keyboard(context: str = "sheets") -> InlineKeyboardMarkup:

    """Build keyboard with Retry, Assistant and Main menu buttons

    

    Args:

        context: The context for the retry button ("sheets" or "folder")

    """

    retry_callback = f"retry_{context}"  # "retry_sheets" or "retry_folder"

    return InlineKeyboardMarkup(

        inline_keyboard=[

            [InlineKeyboardButton(text="📍 Retry", callback_data=retry_callback)],

            [InlineKeyboardButton(text="💬 Chat", callback_data="chat_start")],

            [InlineKeyboardButton(text="🏠 Home", callback_data="main_menu")],

        ]

    )





def is_valid_email(email: str) -> bool:

    return bool(EMAIL_REGEX.match(email.strip()))





def limit_2d_table(rows: list[list[Any]]) -> list[list[str]]:

    trimmed: list[list[str]] = []

    for row in rows[:MAX_ROWS_FOR_CONTEXT]:

        trimmed.append([str(col) if col is not None else "" for col in row[:MAX_COLS_FOR_CONTEXT]])

    return trimmed





def table_to_text(rows: list[list[Any]]) -> str:

    trimmed = limit_2d_table(rows)

    if not trimmed:

        return "No data found."

    lines = [" | ".join(row) for row in trimmed]

    text = "\n".join(lines)

    return text[:MAX_CHARS_CONTEXT]





def credentials_from_json(credentials_json: str, telegram_id: int | None = None) -> Credentials:

    """Parse credentials JSON, auto-refresh if expired, and persist the refreshed token.



    Pass ``telegram_id`` so that a freshly-refreshed token is immediately saved

    back to SQLite + Supabase, preventing the need to re-authenticate after

    every bot restart.

    """

    data = json.loads(credentials_json)

    creds = Credentials.from_authorized_user_info(data, scopes=SCOPES)

    

    # Check if stored credentials have old scopes (only .readonly)

    stored_scopes = data.get("scopes", [])

    has_old_scopes = any("readonly" in scope for scope in stored_scopes)

    

    # If old scopes detected, raise error to trigger re-authentication

    if has_old_scopes:

        logger.warning(f"⚠️ Old credential scopes detected (read-only). User needs to re-authenticate.")

        raise ValueError("Credentials have old scopes. Please re-authenticate to grant full access.")

    

    # Refresh if expired, then save the new token so it survives future restarts

    if creds.expired and creds.refresh_token:

        try:

            creds.refresh(GoogleAuthRequest())

            logger.info("✅ Credentials refreshed successfully")

            if telegram_id:

                save_google_token(telegram_id, creds.to_json())

        except Exception as e:

            logger.warning(f"⚠️ Could not refresh credentials: {e}")

    

    return creds





def list_google_sheets(credentials: Credentials) -> list[dict[str, str]]:

    if credentials.expired and credentials.refresh_token:

        credentials.refresh(GoogleAuthRequest())

    gc = gspread.authorize(credentials)

    files = gc.list_spreadsheet_files()

    sheets = [{"id": item["id"], "name": item.get("name", "Unnamed spreadsheet")} for item in files]

    if sheets:

        return sheets

    books = gc.openall()

    return [{"id": book.id, "name": book.title} for book in books]





def fetch_sheet_rows(credentials: Credentials, sheet_id: str) -> dict[str, list[list[Any]]]:

    """Fetch ALL sheets from a Google Sheets spreadsheet"""

    if credentials.expired and credentials.refresh_token:

        credentials.refresh(GoogleAuthRequest())

    gc = gspread.authorize(credentials)

    workbook = gc.open_by_key(sheet_id)

    

    # Get all worksheets in the spreadsheet

    all_sheets = {}

    for worksheet in workbook.worksheets():

        sheet_name = worksheet.title

        try:

            rows = worksheet.get_all_values()

            all_sheets[sheet_name] = rows

            logger.info(f"📊 Read sheet '{sheet_name}' with {len(rows)} rows")

        except Exception as e:

            logger.warning(f"⚠️ Could not read sheet '{sheet_name}': {e}")

            all_sheets[sheet_name] = []

    

    logger.info(f"✅ Successfully read {len(all_sheets)} sheets from spreadsheet")

    return all_sheets





def parse_excel_bytes(file_name: str, content: bytes) -> list[list[Any]]:

    lower = file_name.lower()

    if lower.endswith(".xlsx"):

        wb = openpyxl.load_workbook(io.BytesIO(content), data_only=True, read_only=True)

        ws = wb.active

        rows = []

        for row in ws.iter_rows(values_only=True):

            rows.append(list(row))

        return rows

    if lower.endswith(".xls"):

        wb = xlrd.open_workbook(file_contents=content)

        ws = wb.sheet_by_index(0)

        rows = []

        for r in range(ws.nrows):

            rows.append(ws.row_values(r))

        return rows

    raise ValueError("Invalid file type.")





class AppContext:

    def __init__(self, config: Config) -> None:

        self.config = config

        self.supabase_service = SupabaseService(

            config.supabase_url, config.supabase_anon_key

        )

        self.sessions = SessionStore()

        # Google OAuth Service (for Google Sheets)

        self.oauth_service = GoogleOAuthService(

            client_id=config.google_client_id,

            client_secret=config.google_client_secret,

            redirect_uri=config.google_redirect_uri,

        )

        self.tavily_api_key = config.tavily_api_key  # Store key for Tavily API

        self.grok_api_key = config.grok_api_key      # Store key for Grok AI (xAI)

        self.bot: Bot | None = None

    

    def _save_credentials_sync(self, telegram_id: int, credentials_json: str) -> None:

        """Synchronously save Google credentials to SQLite + Supabase."""

        save_google_token(telegram_id, credentials_json)



    async def handle_oauth_callback(self, state: str, code: str) -> str:

        try:

            telegram_id, credentials = await asyncio.to_thread(

                self.oauth_service.exchange_code, state, code

            )

            session = self.sessions.get(telegram_id)

            creds_json = credentials.to_json()

            session.google_credentials_json = creds_json

            logger.info(f"✅ OAuth successful for user {telegram_id}")

            

            # Save credentials to SQLite + Supabase immediately so they survive restarts

            await asyncio.to_thread(save_google_token, telegram_id, creds_json)

            

            # Check which mode user chose

            if session.auth_mode == "folder":

                session.step = "waiting_folder_link"

                message_text = ("✅ <b>Google hisobiga muvaffaqiyatli ulandi!</b>\n\n"
                               "Endi Google Drive papka havolasini yuboring:\n\n"
                               "📋 <b>Misol:</b>\n"
                               "<code>https://drive.google.com/drive/folders/1ABC123xyz</code>")




            else:

                # Default to sheets mode

                session.step = "waiting_sheet_link"

                message_text = "✅ <b>Google hisobiga muvaffaqiyatli ulandi!</b>\n\n" \
                              "Endi Google Sheets havolasini yuboring:\n\n" \
                              "📋 <b>Misol:</b>\n" \
                              "<code>https://docs.google.com/spreadsheets/d/1Abc123xyz/edit</code>"

            

            # Send success message

            if self.bot:

                try:

                    await self.bot.send_message(

                        telegram_id,

                        message_text,

                        parse_mode="HTML"

                    )

                except (TelegramBadRequest, TelegramAPIError) as send_err:

                    logger.warning("HTML send failed in OAuth callback, retrying plain: %s", send_err)

                    try:

                        await self.bot.send_message(

                            telegram_id,

                            _strip_html_tags(message_text),

                        )

                    except Exception as inner:

                        logger.error("Plain-text fallback also failed in OAuth callback: %s", inner)

            return "✅ Permission granted! Now send the link"

        

        except ValueError:

            # Re-raise ValueError so _google_callback can handle state errors

            raise

        except Exception as e:

            logger.error(f"❌ OAuth callback error: {e}", exc_info=True)

            return f"❌ OAuth error: {str(e)[:100]}"





class OAuthServer:

    def __init__(self, context: AppContext) -> None:

        self.context = context

        self.runner: web.AppRunner | None = None

        self.site: web.TCPSite | None = None



    async def start(self) -> None:

        app = web.Application()

        # Add routes for Google OAuth callback and health check

        app.add_routes([

            web.get("/", self._google_callback),  # Google OAuth callback

            web.get("/health", self._health_check),  # Health check endpoint

        ])

        self.runner = web.AppRunner(app)

        await self.runner.setup()

        # Use config values for host and port

        host = self.context.config.server_host

        port = self.context.config.server_port

        self.site = web.TCPSite(self.runner, host=host, port=port)

        await self.site.start()

        logger.info(f"✅ OAuth callback server running at {self.context.config.google_redirect_uri}")



    async def stop(self) -> None:

        if self.runner:

            await self.runner.cleanup()



    async def _health_check(self, request: web.Request) -> web.Response:

        """Health check endpoint - returns bot version and status"""

        try:

            return web.json_response({

                "status": "✅ OK",

                "version": BOT_VERSION,

                "features": FEATURES,

                "timestamp": datetime.now().isoformat(),

                "ai_qa_enabled": FEATURES.get("ai_qa", False),

                "data_indexing_enabled": FEATURES.get("data_indexing", False),

            })

        except Exception as e:

            logger.warning(f"⚠️ Health check error: {e}")

            return web.json_response({

                "status": "✅ OK",  # Always return OK for health check

                "version": BOT_VERSION,

            })



    async def _google_callback(self, request: web.Request) -> web.Response:

        """Handle Google OAuth callback"""

        state = request.query.get("state")

        code = request.query.get("code")

        error = request.query.get("error")

        if error:

            return web.Response(

                text=f"OAuth error: {error}. Return to the Telegram bot and try again"

            )

        if not state or not code:

            return web.Response(text="Invalid OAuth request.")

        try:

            msg = await self.context.handle_oauth_callback(state, code)

            return web.Response(text=msg)

        except ValueError as ve:

            error_str = str(ve)

            logger.warning(f"⚠️ OAuth state error: {error_str}")

            # Provide helpful message for state errors

            if "state not in" in error_str.lower() or "state not found" in error_str.lower():

                return web.Response(

                    text="❌ OAuth sessiyasi muddati tugagan.\n\n"

                         "Telegram botga qayting, /start yuboring va qaytadan urinib ko'ring\n\n"

                         "If the problem persists, try again in a few minutes"

                )

            else:

                return web.Response(text=f"❌ OAuth error: {error_str[:80]}")

        except Exception as exc:

            logger.exception("OAuth callback error: %s", exc)

            return web.Response(

                text=f"❌ Xato: {str(exc)[:100]}.\n\n"

                     "/start ga qayting va qaytadan urinib ko'ring"

            )





def register_handlers(dp: Dispatcher, ctx: AppContext) -> None:

    @dp.message(CommandStart())

    async def start_handler(message: Message) -> None:

        """Handle /start command"""

        telegram_id = message.from_user.id

        session = ctx.sessions.get(telegram_id)

        

        logger.info(f"🚀▒ /start - User {telegram_id}")

        

        try:

            # Check if user already registered

            logger.info(f"🔍 Checking if user {telegram_id} is registered in Supabase...")

            user = await ctx.supabase_service.get_user_by_telegram(telegram_id)

            logger.info(f"📊 Supabase result: {user}")

            

            if user:

                # User already registered - show main menu

                first_name = user.get("first_name", "")

                last_name = user.get("last_name", "")

                full_name = f"{first_name} {last_name}".strip()

                

                session.full_name = full_name

                session.email = user.get("email")



                # session.step may already be "in_chat" if workspace was restored

                # by SessionStore._restore_workspace(); don't overwrite it.

                if session.step not in ("in_chat", "selecting_folder_sheets"):

                    session.step = "ready"

                

                # Check for active sheet integration (Supabase metadata only)

                active = await ctx.supabase_service.get_active_integration(telegram_id)

                if active:

                    if not session.sheet_id:

                        session.sheet_id = active.get("sheet_id")

                    if not session.sheet_name:

                        session.sheet_name = active.get("sheet_name")

                

                logger.info(f"✅ User {telegram_id} already registered: {full_name}")



                # Build a greeting that reflects the actual state

                if session.step == "in_chat":

                    # Workspace already loaded — tell the user they can go straight to chat

                    if session.all_folder_sheets_data:

                        n = len(session.all_folder_sheets_data)

                        ws_hint = f"📁 {n} ta jadval yuklandi. Savolingizni berishingiz mumkin!"

                    elif session.all_sheets_data:

                        n = len(session.all_sheets_data)

                        ws_hint = f"📊 {session.sheet_name or 'Jadval'} ({n} ta varaq) yuklandi. Savolingizni berishingiz mumkin!"

                    else:

                        ws_hint = "Menyudan kerakli bo'limni tanlang:"

                        session.step = "ready"

                    await message.answer(

                        f"👋 Xush kelibsiz, {full_name}!\n\n{ws_hint}",

                        reply_markup=build_main_menu(),

                    )

                else:

                    await message.answer(

                        f"👋 Xush kelibsiz, {full_name}!\n\n"

                        "Menyudan kerakli bo'limni tanlang:",

                        reply_markup=build_main_menu(),

                    )

                logger.info(f"✅ Response sent to {telegram_id}")

                return

            

            # New user - start registration

            logger.info(f"👋 New user {telegram_id} - starting registration")

            session.step = "waiting_first_name"

            

            logger.info(f"📤 Sending registration prompt to {telegram_id}...")

            await message.answer(

            "Salom! 👋 OnBrain AI botiga xush kelibsiz.\n\n"

                "📝 <b>Ro'yxatdan o'tish uchun ismingizni yuboring:</b>",

                parse_mode="HTML",

            )

            logger.info(f"✅ Registration prompt sent to {telegram_id}")

            

        except Exception as exc:

            logger.exception(f"❌ /start handler error for user {telegram_id}: {exc}")

            try:

                await message.answer(

                    "❌ Xatolik yuz berdi. /start buyrug'ini qayta yuboring"

                )

            except Exception as e:

                logger.error(f"❌ Could not send error message: {e}")



    @dp.message(Command("chat"))

    async def chat_command_handler(message: Message) -> None:

        """Handle /chat command to start chatting"""

        telegram_id = message.from_user.id

        session = ctx.sessions.get(telegram_id)

        

        logger.info(f"💬 /chat command - User {telegram_id}")

        

        # Check if user is registered

        if session.step in {"waiting_first_name", "waiting_contact", "waiting_email"}:

            await message.answer("❌ Avval ro'yxatdan o'ting. /start buyrug'ini yuboring.")

            return

        

        try:

            session.step = "in_chat"

            

            # Get user's name for personalized greeting

            first_name = session.full_name or message.from_user.first_name or ""



            # Generate dynamic greeting (already includes Chat Mode header)

            greeting = DynamicGreetings.get_dynamic_greeting(first_name)



            await message.answer(

                greeting,

                parse_mode="HTML",

                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[

                    InlineKeyboardButton(text="🚪 Chat'ni tark etish", callback_data="exit_chat")

                ]])

            )

        except Exception as exc:

            logger.exception(f"❌ Chat command error: {exc}")

            await message.answer("❌ Xatolik yuz berdi. Qaytadan urinib ko'ring")



    @dp.message(Command("sheets"))

    async def sheets_command_handler(message: Message) -> None:

        """Handle /sheets command to connect Google Sheets"""

        telegram_id = message.from_user.id

        session = ctx.sessions.get(telegram_id)

        

        logger.info(f"📊 /sheets command - User {telegram_id}")

        

        # Check if user is registered

        if session.step in {"waiting_first_name", "waiting_contact", "waiting_email"}:

            await message.answer("❌ Avval ro'yxatdan o'ting. /start buyrug'ini yuboring.")

            return

        

        try:

            # Trigger sheets button handler

            session.auth_mode = "sheets"

            try:

                await message.answer(

                    "📊 <b>Connect Google Sheets</b>\n\n"

                    "Google Sheets fayli ulash uchun:\n\n"

                    "1️⃣ Google hisobiga kiring\n"

                    "2️⃣ Jadval havolasini yuboring:\n"

                    "<code>https://docs.google.com/spreadsheets/d/1ABC123xyz/edit</code>",

                    parse_mode="HTML"

                )

            except (TelegramBadRequest, TelegramAPIError) as exc:

                logger.warning("/sheets command HTML send failed, sending plain: %s", exc)

                await message.answer(

                    "📊 Connect Google Sheets\n\n"

                    "Google Sheets fayli ulash uchun:\n\n"

                    "1. Google hisobiga kiring\n"

                    "2. Jadval havolasini yuboring:\n"

                    "https://docs.google.com/spreadsheets/d/1ABC123xyz/edit"

                )

            

            # Try to load & auto-refresh token from SQLite/Supabase (works after restart too)

            if not session.google_credentials_json:

                refreshed = await asyncio.to_thread(load_and_refresh_google_token, telegram_id)

                if refreshed:

                    session.google_credentials_json = refreshed

                    logger.info(f"✅ Force-loaded & refreshed Google token for user {telegram_id}")

            

            # Now check again after reload attempt

            if not session.google_credentials_json:

                session.step = "waiting_auth"

                await message.answer(

                    "🔐 Google hisobingizga kiring:",

                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[

                        [InlineKeyboardButton(text="🔐 Google'access", callback_data="auth_google")]

                    ])

                )

        except Exception as exc:

            logger.exception(f"❌ Sheets command error: {exc}")

            await message.answer("❌ Xatolik yuz berdi. Qaytadan urinib ko'ring")



    @dp.message(Command("folder"))

    async def folder_command_handler(message: Message) -> None:

        """Handle /folder command to access Google Drive folder"""

        telegram_id = message.from_user.id

        session = ctx.sessions.get(telegram_id)

        

        logger.info(f"📁 /folder command - User {telegram_id}")

        

        # Check if user is registered

        if session.step in {"waiting_first_name", "waiting_contact", "waiting_email"}:

            await message.answer("❌ Avval ro'yxatdan o'ting. /start buyrug'ini yuboring.")

            return

        

        try:

            session.auth_mode = "folder"

            try:

                await message.answer(

                    "📁 <b>Google Drive Papka ulash</b>\n\n"

                    "Google Drive papka uchun:\n\n"

                    "1️⃣ Google hisobiga kiring\n"

                    "2️⃣ Papka havolasini yuboring:\n"

                    "<code>https://drive.google.com/drive/folders/1ABC123xyz</code>",

                    parse_mode="HTML"

                )

            except (TelegramBadRequest, TelegramAPIError) as exc:

                logger.warning("/folder command HTML send failed, sending plain: %s", exc)

                await message.answer(

                    "📁 Google Drive Papka ulash\n\n"

                    "Google Drive papka uchun:\n\n"

                    "1. Google hisobiga kiring\n"

                    "2. Papka havolasini yuboring:\n"

                    "https://drive.google.com/drive/folders/1ABC123xyz"

                )

            

            # Try to load & auto-refresh token from SQLite/Supabase (works after restart too)

            if not session.google_credentials_json:

                refreshed = await asyncio.to_thread(load_and_refresh_google_token, telegram_id)

                if refreshed:

                    session.google_credentials_json = refreshed

                    logger.info(f"✅ Force-loaded & refreshed Google token for user {telegram_id}")

            

            # Now check again after reload attempt

            if not session.google_credentials_json:

                session.step = "waiting_auth"

                await message.answer(

                    "🔐 Google hisobingizga kiring:",

                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[

                        [InlineKeyboardButton(text="🔐 Google'access", callback_data="auth_google")]

                    ])

                )

        except Exception as exc:

            logger.exception(f"❌ Folder command error: {exc}")

            await message.answer("❌ Xatolik yuz berdi. Qaytadan urinib ko'ring")



    @dp.message(Command("excel"))

    async def excel_command_handler(message: Message) -> None:

        """Handle /excel command to upload Excel file"""

        telegram_id = message.from_user.id

        session = ctx.sessions.get(telegram_id)

        

        logger.info(f"📁 /excel command - User {telegram_id}")

        

        # Check if user is registered

        if session.step in {"waiting_first_name", "waiting_contact", "waiting_email"}:

            await message.answer("❌ Avval ro'yxatdan o'ting. /start buyrug'ini yuboring.")

            return

        

        try:

            session.step = "waiting_excel_file"

            await message.answer(

                "📁 <b>Upload Excel File</b>\n\n"

                "Excel fayl yuboring (.xlsx, .xls):\n\n"

                "Reply to the bot with a file.",

                parse_mode="HTML"

            )

        except Exception as exc:

            logger.exception(f"❌ Excel command error: {exc}")

            await message.answer("❌ Xatolik yuz berdi. Qaytadan urinib ko'ring")



    @dp.message(Command("disconnect"))

    async def disconnect_command_handler(message: Message) -> None:

        """Disconnect the user's Google Drive/Sheets workspace and clear cached data."""

        telegram_id = message.from_user.id

        session = ctx.sessions.get(telegram_id)

        logger.info(f"🔌 /disconnect - User {telegram_id}")



        if session.step in {"waiting_first_name", "waiting_contact", "waiting_email"}:

            await message.answer("❌ Avval ro'yxatdan o'ting.")

            return



        # Clear session data

        session.sheet_id = None

        session.sheet_name = None

        session.sheet_data = []

        session.all_sheets_data = {}

        session.all_folder_sheets_data = {}

        session.folder_spreadsheets = []

        session.selected_spreadsheets = []

        session.folder_id = None

        session.pending_sheets = {}

        session.step = "ready"



        # Remove from SQLite workspace store

        await asyncio.to_thread(_workspace_store.delete_workspace, telegram_id)



        await message.answer(

            "🔌 <b>Connection disconnected.</b>\n\n"

            "Barcha spreadsheet ma'lumotlari o'chirildi.\n\n"

            "To reconnect, use /sheets or /folder command'ini yubo",

            parse_mode="HTML",

            reply_markup=build_main_menu(),

        )

        logger.info(f"✅ Workspace disconnected for user {telegram_id}")



    @dp.message(Command("help"))

    async def _help_command_handler(message: Message) -> None:

        """Handle /help command to show help information"""

        telegram_id = message.from_user.id

        logger.info(f"📋 /help command - User {telegram_id}")

        

        try:

            help_text = (

                "<b>📚 OnBrain AI Bot - Yordam</b>\n\n"

                "<b>Mavjud buyruqlar:</b>\n"

                "🏠 <b>/start</b> - Asosiy menyu\n"

                "💬 <b>/chat</b> - Assistant bilan suhbat\n"

                "📊 <b>/sheets</b> - Connect Google Sheets\n"

                "📁 <b>/folder</b> - Google Drive Papka ulash\n"

                "📄 <b>/excel</b> - Upload Excel File\n"

                "🔌 <b>/disconnect</b> - Disconnect the linked spreadsheet\n"

                "📋 <b>/help</b> - Show this help text\n\n"

                "<b>Asosiy funksiyalar:</b>\n"

                "📊 <b>Google Sheets</b> - Google Sheets fayllari bilan ishlash\n"

                "📁 <b>Google Drive</b> - Google Drive papkasidagi fayllari o'qish\n"

                "📄 <b>Excel</b> - Fayl yuklash va yuborish\n"

                "💬 <b>Chat</b> - AI yordamchi bilan suhbat\n"

                "🎤 <b>Ovozli savol</b> - Mikrofondan o'zbek tilida savol bering, AI javob beradi!\n\n"

                "<b>Qanday ishlatish:</b>\n"

                "1. /start buyrug'ini yuboring\n"

                "2. Asosiy menyudan kerakli bo'limni tanlang\n"

                "3. Google Sheets, Excel yoki papka ulang\n"

                "4. Savolingizni <b>yozing</b> yoki 🎤 <b>ovozda yuboring</b>!\n\n"

                "<i>Muammo bo'lsa, @aionbrain_bot ga yozing</i>"

            )

            await message.answer(help_text, parse_mode="HTML")

        except Exception as exc:

            logger.exception(f"❌ Help command error: {exc}")

            await message.answer("❌ Xatolik yuz berdi. Qaytadan urinib ko'ring")



    @dp.callback_query(F.data == "chat_start")

    async def chat_button_handler(callback_query: CallbackQuery) -> None:

        """Handle Chat button click"""

        telegram_id = callback_query.from_user.id

        session = ctx.sessions.get(telegram_id)

        

        logger.info(f"💬 Chat button clicked - User {telegram_id}")

        

        # Check if user is registered

        if session.step in {"waiting_first_name", "waiting_contact", "waiting_email"}:

            await callback_query.answer("❌ Avval ro'yxatdan o'ting!", show_alert=True)

            return

        

        try:

            session.step = "in_chat"

            first_name = session.full_name or callback_query.from_user.first_name or ""

            greeting = DynamicGreetings.get_dynamic_greeting(first_name)

            await callback_query.message.answer(

                greeting,

                parse_mode="HTML",

                reply_markup=InlineKeyboardMarkup(inline_keyboard=[

                    [InlineKeyboardButton(text="🚪 Chat'ni tark etish", callback_data="exit_chat")]

                ])

            )

            await callback_query.answer("✅ Chat rejimi faollashtirildi")

        except Exception as exc:

            logger.exception(f"❌ Chat button error: {exc}")

            await callback_query.answer("❌ Xatolik yuz berdi!", show_alert=True)



    @dp.callback_query(F.data == "exit_chat")

    async def exit_chat_handler(callback_query: CallbackQuery) -> None:

        """Handle Chatdan chiqish button click"""

        telegram_id = callback_query.from_user.id

        session = ctx.sessions.get(telegram_id)

        

        logger.info(f"🚪 Exit chat clicked - User {telegram_id}")

    

        try:

            session.step = "ready"

            await callback_query.answer("✅ Chat rejimi yopildi")

            

            # Show main menu with all options

            await callback_query.message.answer(

                "👋 <b>Asosiy menyu</b>\n\n<i>Nimani qilmoqchisiz?</i>",

                reply_markup=build_main_menu(),

                parse_mode="HTML"

            )

        except Exception as exc:

            logger.exception(f"❌ Exit chat error: {exc}")

            await callback_query.answer("❌ Xatolik yuz berdi!", show_alert=True)



    @dp.callback_query(F.data == "chat_continue")

    async def chat_continue_handler(callback_query: CallbackQuery) -> None:

        """Handle Davom etish button click - user wants to ask more questions"""

        telegram_id = callback_query.from_user.id

        session = ctx.sessions.get(telegram_id)

        

        logger.info(f"💬 Davom etish chat clicked - User {telegram_id}")

        

        try:

            session.step = "in_chat"

            session.web_search_mode = False  # Reset web search mode

            await callback_query.answer("✅ Chat davom etmoqda...", show_alert=False)

            await callback_query.message.answer(

                "💬 Keyingi savolingizni yuboring yoki /start orqali asosiy menyuga qayting."

            )

        except Exception as exc:

            logger.exception(f"❌ Chat continue error: {exc}")

            await callback_query.answer("❌ Xatolik yuz berdi!", show_alert=True)



    @dp.callback_query(F.data == "web_search_mode")

    async def web_search_mode_handler(callback_query: CallbackQuery) -> None:

        """Switch to internet web search mode (Tavily), bypasses spreadsheet data"""

        telegram_id = callback_query.from_user.id

        session = ctx.sessions.get(telegram_id)

        session.step = "in_chat"

        session.web_search_mode = True

        await callback_query.answer("🌐 Internet qidirish yoqildi", show_alert=False)

        await callback_query.message.answer(

            "🌐 <b>Internet qidirish rejimi</b>\n\n"

            "Savolingizni yozing, internetdan qidiraman.\n"

            "Press <b>Return to Chat</b> to go back to spreadsheet data.",

            parse_mode="HTML",

            reply_markup=InlineKeyboardMarkup(inline_keyboard=[

                [InlineKeyboardButton(text="📊 Spreadsheet rejimiga qaytish", callback_data="chat_continue")],

                [InlineKeyboardButton(text="🚪 Chat-ni tugatish", callback_data="exit_chat")],

            ])

        )




    @dp.callback_query(F.data == "voice_hint")

    async def voice_hint_handler(callback_query: CallbackQuery) -> None:

        """Show instructions on how to use the voice feature."""

        await callback_query.answer()


        # Show hint without navigating away from chat — just a dismiss button
        dismiss_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✖️ Yopish", callback_data="dismiss_hint")]
        ])

        await callback_query.message.answer(

            "🎤 <b>Ovozli savol yuboring!</b>\n\n"

            "Mikrofon tugmasini <b>bosib ushlab turing</b>, savolingizni ayting va qo'yib yuboring.",

            parse_mode="HTML",

            reply_markup=dismiss_keyboard

        )


    
    @dp.callback_query(F.data == "dismiss_hint")
    async def dismiss_hint_handler(callback_query: CallbackQuery) -> None:
        """Dismiss the voice hint popup and go back to chat response keyboard."""
        await callback_query.answer()
        try:
            await callback_query.message.delete()
        except Exception:
            pass

    @dp.callback_query(F.data == "main_menu")

    async def main_menu_handler(callback_query: CallbackQuery) -> None:

        """Handle Main Menu button - return to main menu"""

        telegram_id = callback_query.from_user.id

        session = ctx.sessions.get(telegram_id)

        

        logger.info(f"🏠 Main menu clicked - User {telegram_id}")

        

        try:

            session.step = "ready"

            await callback_query.answer("✅ Asosiy menyu")

            

            # Show main menu with all options

            await callback_query.message.answer(

                "👋 <b>Asosiy menyu</b>\n\n<i>Nimani qilmoqchisiz?</i>",

                reply_markup=build_main_menu(),

                parse_mode="HTML"

            )

        except Exception as exc:

            logger.exception(f"❌ Main menu error: {exc}")

            await callback_query.answer("❌ Xatolik yuz berdi!", show_alert=True)



    @dp.callback_query(F.data == "sheets")
    async def sheets_button_handler(callback_query: CallbackQuery) -> None:
        """Handle Google Sheets button click - ask user for the link"""
        telegram_id = callback_query.from_user.id
        session = ctx.sessions.get(telegram_id)
        logger.info(f"Sheets button clicked - User {telegram_id}")

        try:
            if session.step in {"waiting_name", "waiting_email"}:
                await callback_query.answer("Avval ro'yxatdan o'ting. /start buyrug'ini yuboring.", show_alert=True)
                return

            session.step = "waiting_sheet_link"

            try:
                await callback_query.message.edit_text(
                    "\U0001f4ca <b>Google Sheets</b>\n\n"
                    "Google Sheets havolasini yuboring:\n\n"
                    "1\ufe0f\u20e3 Google Sheets'da jadvalingizni oching\n"
                    "2\ufe0f\u20e3 <b>Ulashish</b> tugmasini bosing \u2192 <b>Havolaga ega har kim</b> \u2192 Ko'ruvchi\n"
                    "3\ufe0f\u20e3 Havolani nusxalab, shu yerga yuboring\n\n"
                    "\U0001f4cb <b>Misol:</b>\n"
                    "<code>https://docs.google.com/spreadsheets/d/1Abc123xyz/edit</code>",
                    parse_mode="HTML"
                )
            except Exception:
                await callback_query.message.answer(
                    "Google Sheets\n\n"
                    "Google Sheets havolasini yuboring.\n"
                    "Jadval 'Havolaga ega har kim' sifatida ulashilganligiga ishonch hosil qiling."
                )

            await callback_query.answer()

        except Exception as exc:
            logger.exception(f"Sheets button error: {exc}")
            await callback_query.answer("Xatolik yuz berdi!", show_alert=True)




    @dp.callback_query(F.data == "excel")

    async def excel_button_handler(callback_query: CallbackQuery) -> None:

        """Handle Excel button click from inline menu"""

        telegram_id = callback_query.from_user.id

        session = ctx.sessions.get(telegram_id)

        

        logger.info(f"📁 Excel button clicked - User {telegram_id}")

        

        try:

            if session.step in {"waiting_name", "waiting_email"}:

                await callback_query.answer("Avval ro'yxatdan o'ting. /start buyrug'ini yuboring.", show_alert=True)

                return

            

            session.step = "waiting_excel"

            await callback_query.message.edit_text(

                "Excel fayl yuboring (.xlsx yoki .xls).\n"

                "Fayl qabul qilingandan so'ng, ma'lumotlar asosida savollaringizga javob beraman."

            )

            await callback_query.answer("✅ Excel bo'rejimi yoqildi")

        except Exception as exc:

            logger.exception(f"❌ Excel button error: {exc}")

            await callback_query.answer("❌ Xatolik yuz berdi!", show_alert=True)



    @dp.callback_query(F.data == "folder")

    async def folder_button_handler(callback_query: CallbackQuery) -> None:

        """Handle Google Drive Folder button click"""

        telegram_id = callback_query.from_user.id

        session = ctx.sessions.get(telegram_id)

        

        logger.info(f"📁 Folder button clicked - User {telegram_id}")

        

        try:

            if session.step in {"waiting_name", "waiting_email"}:

                await callback_query.answer("Avval ro'yxatdan o'ting. /start buyrug'ini yuboring.", show_alert=True)

                return

            

            # Set auth mode to "folder" so OAuth knows what to do

            session.auth_mode = "folder"

            

            # Check if user already has Google credentials

            if session.google_credentials_json:

                # User already authenticated, ask for folder link

                try:

                    await callback_query.message.edit_text(

                        "📁 <b>Google Drive Papka ulash</b>\n\n"

                        "✅ Google hisobingizga ulangansiz!\n\n"

                        "Endi Google Drive papka havolasini yuboring:\n\n"

                        "1️⃣ Google Drive ni oching (drive.google.com)\n"

                        "2️⃣ Spreadsheet lar joylashgan papkani toping\n"

                        "3️⃣ Right-click the folder 📊 and click \"Share\"\n"

                        "4️⃣ Select \"Anyone with link\" access\n"

                        "5️⃣ Havolani nusxalab, botga yuboring\n\n"

                        "📋 <b>Misol:</b>\n"

                        "<code>https://drive.google.com/drive/folders/1ABC123xyz</code>",

                        parse_mode="HTML"

                    )

                except (TelegramBadRequest, TelegramAPIError) as exc:

                    logger.warning("Folder button edit_text failed, sending plain: %s", exc)

                    await callback_query.message.answer(

                        "📁 Google Drive Papka ulash\n\n"

                        "✅ Google hisobingizga ulangansiz!\n\n"

                        "Endi Google Drive papka havolasini yuboring:\n\n"

                        "Misol:\nhttps://drive.google.com/drive/folders/1ABC123xyz"

                    )

                session.step = "waiting_folder_link"

                await callback_query.message.answer("📬 Google Drive papka havolasini yuboring..")

            else:

                # Ask directly for folder link (no OAuth)

                try:

                    await callback_query.message.edit_text(

                        "🔒 <b>Google Drive Papka ulash</b>\n\n"

                        "Google Drive papka havolasini to'g'ridan-to'g'ri yuboring:\n\n"

                        "1️⃣ Google Drive ni oching (drive.google.com)\n"

                        "2️⃣ Spreadsheet lar joylashgan papkani toping\n"

                        "3️⃣ Right-click the folder 📊 and click \"Share\"\n"

                        "4️⃣ Select \"Anyone with link\" access\n"

                        "5️⃣ Havolani nusxalab, botga yuboring\n\n"

                        "🔒 <b>Misol:</b>\n"

                        "<code>https://drive.google.com/drive/folders/1ABC123xyz</code>",

                        parse_mode="HTML"

                    )

                except (TelegramBadRequest, TelegramAPIError) as exc:

                    logger.warning("Folder button edit_text failed, sending plain: %s", exc)

                    await callback_query.message.answer(

                        "🔒 Google Drive Papka ulash\n\n"

                        "Papka havolasini to'g'ridan-to'g'ri yuboring:\n\n"

                        "Misol:\nhttps://drive.google.com/drive/folders/1ABC123xyz"

                    )

                session.step = "waiting_folder_link"

                await callback_query.message.answer("🔒 Google Drive papka havolasini yuboring..")

            

            await callback_query.answer("✅ Folder bo'rejimi yoqildi")

        except Exception as exc:

            logger.exception(f"❌ Folder button error: {exc}")

            await callback_query.answer("❌ Xatolik yuz berdi!", show_alert=True)



    @dp.callback_query(F.data.startswith("retry_sheets"))
    async def retry_sheets_handler(callback_query: CallbackQuery) -> None:
        """Handle retry for sheets - ask for sheets link again"""
        telegram_id = callback_query.from_user.id
        session = ctx.sessions.get(telegram_id)
        logger.info(f"Retry sheets - User {telegram_id}")

        try:
            session.step = "waiting_sheet_link"
            try:
                await callback_query.message.answer(
                    "\U0001f4ca <b>Google Sheets</b>\n\n"
                    "Google Sheets havolasini yuboring:\n\n"
                    "1\ufe0f\u20e3 Google Sheets'da jadvalingizni oching\n"
                    "2\ufe0f\u20e3 <b>Ulashish</b> tugmasini bosing \u2192 <b>Havolaga ega har kim</b> \u2192 Ko'ruvchi\n"
                    "3\ufe0f\u20e3 Havolani nusxalab, shu yerga yuboring\n\n"
                    "\U0001f4cb <b>Misol:</b>\n"
                    "<code>https://docs.google.com/spreadsheets/d/1Abc123xyz/edit</code>",
                    parse_mode="HTML"
                )
            except Exception:
                await callback_query.message.answer(
                    "Google Sheets\n\n"
                    "Google Sheets havolasini yuboring.\n"
                    "U 'Havolaga ega har kim' sifatida ulashilganligiga ishonch hosil qiling."
                )
            await callback_query.answer()
        except Exception as exc:
            logger.exception(f"Retry sheets error: {exc}")
            await callback_query.answer("Xatolik yuz berdi!", show_alert=True)

    @dp.callback_query(F.data == "retry_folder")

    async def retry_folder_handler(callback_query: CallbackQuery) -> None:

        """Handle retry for folder - ask for folder link again"""

        telegram_id = callback_query.from_user.id

        session = ctx.sessions.get(telegram_id)

        

        logger.info(f"📍 Retry folder - User {telegram_id}")

        

        try:

            session.step = "waiting_folder_link"

            try:

                await callback_query.message.answer(

                    "📁 <b>Google Drive Papka ulash</b>\n\n"

                    "Google Drive papka havolasini yuboring:\n\n"

                    "1️⃣ Google Drive da papkani oching\n"

                    "2️⃣ Click the \"Share\" button\n"

                    "3️⃣ Havolani nusxalab, botga yuboring\n\n"

                    "📋 <b>Misol:</b>\n"

                    "<code>https://drive.google.com/drive/folders/1Abc123xyz?usp=sharing</code>",

                    parse_mode="HTML"

                )

            except (TelegramBadRequest, TelegramAPIError) as exc:

                logger.warning("Retry folder HTML failed, sending plain: %s", exc)

                await callback_query.message.answer(

                    "📁 Google Drive Papka ulash\n\n"

                    "Google Drive papka havolasini yuboring:\n\n"

                    "Misol:\nhttps://drive.google.com/drive/folders/1Abc123xyz?usp=sharing"

                )

            await callback_query.answer()

        except Exception as exc:

            logger.exception(f"❌ Retry folder error: {exc}")

            await callback_query.answer("❌ Xatolik yuz berdi!", show_alert=True)



    @dp.message(F.text == MAIN_MENU_SHEETS)
    async def connect_sheets_handler(message: Message) -> None:
        """Handle Google Sheets text button - ask user for the link"""
        telegram_id = message.from_user.id
        session = ctx.sessions.get(telegram_id)

        if session.step in {"waiting_name", "waiting_email"}:
            await message.answer("Avval ro'yxatdan o'ting. /start buyrug'ini yuboring.")
            return

        session.step = "waiting_sheet_link"

        try:
            await message.answer(
                "\U0001f4ca <b>Google Sheets</b>\n\n"
                "Google Sheets havolasini yuboring:\n\n"
                "1\ufe0f\u20e3 Google Sheets'da jadvalingizni oching\n"
                "2\ufe0f\u20e3 <b>Ulashish</b> tugmasini bosing \u2192 <b>Havolaga ega har kim</b> \u2192 Ko'ruvchi\n"
                "3\ufe0f\u20e3 Havolani nusxalab, shu yerga yuboring\n\n"
                "\U0001f4cb <b>Misol:</b>\n"
                "<code>https://docs.google.com/spreadsheets/d/1Abc123xyz/edit</code>",
                parse_mode="HTML"
            )
        except Exception:
            await message.answer(
                "Google Sheets\n\n"
                "Google Sheets havolasini yuboring.\n"
                "Jadval 'Havolaga ega har kim' sifatida ulashilganligiga ishonch hosil qiling."
            )

    @dp.message(F.text == MAIN_MENU_EXCEL)

    async def upload_excel_menu_handler(message: Message) -> None:

        telegram_id = message.from_user.id

        session = ctx.sessions.get(telegram_id)

        if session.step in {"waiting_name", "waiting_email"}:

            await message.answer("Avval ro'yxatdan o'ting. /start buyrug'ini yuboring.")

            return

        session.step = "waiting_excel"

        await message.answer(

            "Excel fayl yuboring (.xlsx yoki .xls).\n"

            "Fayl qabul qilingandan so'ng, ma'lumotlar asosida savollaringizga javob beraman."

        )



    @dp.message(F.text)

    async def text_handler(message: Message) -> None:

        """Unified text message handler for registration flow and chat"""

        telegram_id = message.from_user.id

        

        # ========== SECURITY: Rate Limiting ==========

        if not rate_limiter.is_allowed(telegram_id):

            await message.answer(

                "⚙️⚠️ <b>Juda ko'p so'requests!</b>\n\n"

                "Biroz kuting va qaytadan urinib ko'ring'\n\n"

                "🛡️ Bu sizni xavfsizlik xatera yuz beradigan hujumlardan himoya qiladi.",

                parse_mode="HTML"

            )

            return

        

        session = ctx.sessions.get(telegram_id)

        

        # ========== SECURITY: Input Validation ==========

        user_text = message.text.strip() if message.text else ""

        if not user_text or len(user_text) > 5000:

            await message.answer("❌ Xabar bo'sh yoki juda uzun.")

            return

        

        # Check for image URLs - AI doesn't support image input

        image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg')

        is_image_url = any(user_text.lower().endswith(ext) or f'image {ext}' in user_text.lower() for ext in image_extensions)

        

        # Also check for Google Drive image URLs or other common image links

        image_domains = ['drive.google.com/file', 'drive.google.com/uc?', 'photos.google.com', 'imgur.com', 'flickr.com']

        is_image_url = is_image_url or any(domain in user_text.lower() for domain in image_domains)

        

        if is_image_url:

            await message.answer(

                "🖼️🖼️ <b>Rasm yoki rasm havolasi qo'not supported</b>\n\n"

                "Currently only text messages are supported'llab-quvvatlanadi.\n"

                "Savolingizni matn sifatida yuboring",

                parse_mode="HTML"

            )

            return

        

        # ====== UNIVERSAL FOLDER LINK DETECTION ======

        # Check if user sent a Google Drive folder link from ANY step

        user_input = input_validator.sanitize_string(user_text)

        cleaned_input = re.sub(r'(/edit.*|/share.*|\?usp.*)$', '', user_input)

        

        is_folder_link = (

            "drive.google.com" in cleaned_input and 

            any(x in cleaned_input for x in ["folders", "open?id=", "drive/folders"])

        )

        

        if is_folder_link:

            # Check if user has Google authentication

            if not session.google_credentials_json:

                await message.answer(

                    "🔐 <b>Google hisobingizni ulashingiz kerak</b>\n\n"

                    "📁 Papka havolasini yuborishdan oldin:\n"

                    "1. \"📊 Connect Google Sheets\" tugmasini bosing\n"

                    "2. Google hisobingizga kiring\n"

                    "3. Keyin papka havolasini yuboring",

                    parse_mode="HTML"

                )

                return

            

            # Redirect to folder handling

            session.step = "waiting_folder_link"

            # Davom etish to the folder link handler below

        # =============================================

        

        # IMPORTANT: Check session.step and handle accordingly

        

        # ====== WAITING FOR GOOGLE SHEETS LINK ======

        if session.step == "waiting_sheet_link":

            user_input = input_validator.sanitize_string(user_text)

            

            # ---------- Extract sheet ID from any supported link format ----------

            sheet_id = _extract_sheet_id(user_input)



            if sheet_id:

                try:

                    # SECURITY: Validate Sheet ID
                    if not input_validator.validate_sheet_id(sheet_id):
                        logger.warning(f"Invalid sheet ID format from user {telegram_id}: {sheet_id}")
                        await message.answer("\u274c Google Sheets ID formati noto'g'ri.")
                        return

                    logger.info(f"\U0001f4ca User {telegram_id} provided Google Sheets link: {sheet_id}")
                    await message.answer(
                        UIAnimations.loading_message("loading_sheets", "Jadvalingizga ulanmoqda"),
                        parse_mode="HTML"
                    )

                    # Fetch all tabs using public CSV export (no OAuth needed)
                    all_sheets_data = await _fetch_all_public_sheets(sheet_id)

                    session.sheet_id = sheet_id
                    session.sheet_name = "Google Sheet"
                    session.all_sheets_data = all_sheets_data
                    session.sheet_data = []
                    session.excel_data = []
                    session.step = "in_chat"

                    sheet_summary = "\u2705 <b>Google Sheets muvaffaqiyatli ulandi!</b>\n\n"
                    sheet_summary += "\U0001f4ca <b>Topilgan jadvallar:</b>\n"
                    for tab_name, rows in all_sheets_data.items():
                        row_count = len(rows)
                        col_count = len(rows[0]) if rows else 0
                        sheet_summary += f"\U0001f4cb {html_escape(tab_name)}: {row_count} qator, {col_count} ustun\n"
                    sheet_summary += "\n\U0001f4ac Savolingizni yozing, jadval ma'lumotlari asosida javob beraman."

                    try:
                        await message.answer(sheet_summary, parse_mode="HTML", reply_markup=build_chat_response_keyboard())
                    except Exception:
                        plain = sheet_summary.replace("<b>", "").replace("</b>", "")
                        await message.answer(plain, reply_markup=build_chat_response_keyboard())

                    logger.info(f"\u2705 Sheet loaded for user {telegram_id}: {list(all_sheets_data.keys())}")

                except RuntimeError as rte:
                    err = str(rte)
                    logger.warning(f"Sheet fetch RuntimeError for {telegram_id}: {err}")
                    if "private" in err.lower() or "anyone with" in err.lower() or "permission" in err.lower() or "403" in err:
                        msg = (
                            "\U0001f512 <b>Jadval yopiq.</b>\n\n"
                            "Iltimos, uni ulashing:\n"
                            "1\u20e3 Google Sheets'ni oching\n"
                            "2\u20e3 <b>Ulashish</b> tugmasini bosing\n"
                            "3\u20e3 Kirishni o'rnating: <b>Havolaga ega har kim</b> \u2192 Ko'ruvchi\n"
                            "4\u20e3 Havolani qayta yuboring"
                        )
                    elif "not found" in err.lower() or "404" in err:
                        msg = "\u274c <b>Jadval topilmadi.</b>\n\nHavola to'g'riligini tekshiring."
                    else:
                        msg = f"\u274c Jadvalni o'qib bo'lmadi.\n\n{html_escape(err[:150])}"
                    try:
                        await message.answer(msg, parse_mode="HTML", reply_markup=build_retry_keyboard("sheets"))
                    except Exception:
                        await message.answer(msg.replace("<b>", "").replace("</b>", ""), reply_markup=build_retry_keyboard("sheets"))
                    session.step = "ready"

                except Exception as exc:
                    logger.exception(f"Error processing sheet link for user {telegram_id}: {exc}")
                    error_details = html_escape(str(exc)[:100])
                    msg = f"\u274c <b>An error occurred.</b>\n\n{error_details}\n\nQaytadan urinib ko'ring."
                    try:
                        await message.answer(msg, parse_mode="HTML", reply_markup=build_retry_keyboard("sheets"))
                    except Exception:
                        await message.answer("\u274c Xatolik yuz berdi. Qaytadan urinib ko'ring.", reply_markup=build_retry_keyboard("sheets"))
                    session.step = "ready"

            elif _looks_like_sheets_url(user_input):

                # It looks like a sheets URL but we couldn't extract an ID

                await message.answer(

                    "❌ Linkdan sheet ID ni ajratib ololmadim.\n\n"

                    "Havola to'g'riligini tekshiring\n"

                    "Quyidagi formatlardan birini yuboring:\n\n"

                    "• https://docs.google.com/spreadsheets/d/SHEET_ID/edit\n"

                    "• https://docs.google.com/spreadsheets/d/SHEET_ID/edit?usp=sharing\n"

                    "• Yoki faqat Sheet ID yuboring",

                    reply_markup=build_retry_keyboard()

                )

            else:

                await message.answer(

                    "❌ Google Sheets link not found.\n\n"

                    "📋 Quyidagi formatlardan birini yuboring:\n\n"

                    "• Google Sheets havolasi\n"

                    "• Yoki faqat Sheet ID (masalan: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms)\n\n"

                    "💡 Sheet ID — is the long string after /d/ in the link.",

                    reply_markup=build_retry_keyboard()

                )

        

        # ====== WAITING FOR GOOGLE DRIVE FOLDER LINK ======

        elif session.step == "waiting_folder_link":

            user_input = input_validator.sanitize_string(user_text)

            

            # ── Auth check: SA doesn't need OAuth; OAuth fallback does ─────
            try:
                from drive_service import DriveService as _DS
                _sa_ready = _DS.available()
            except ImportError:
                _sa_ready = False

            # If no SA AND no OAuth → tell user to share with SA email
            if not _sa_ready and not session.google_credentials_json:
                await message.answer(
                    "🔐 <b>Google Drive papkani ulash</b>\n\n"
                    "Papkani botga ulash uchun quyidagi emailga "
                    "<b>Viewer</b> huquqi bering:\n\n"
                    "<code>onbrain-ai@onbrain-ai-489203.iam.gserviceaccount.com</code>\n\n"
                    "<b>Qanday qilish kerak:</b>\n"
                    "1. Google Drive → Papkangizni toping\n"
                    "2. ⋮ → Share (Ulashish)\n"
                    "3. Yuqoridagi emailni qo\'shing → Viewer\n"
                    "4. Papka havolasini qayta yuboring.",
                    parse_mode="HTML",
                    reply_markup=build_retry_keyboard("folder"),
                )
                session.step = "waiting_folder_link"
                return

            # Clean the URL - remove /edit, /share, query params

            user_input = re.sub(r'(/edit.*|/share.*|\?usp.*)$', '', user_input)

            

            # Check if it's a Google Drive folder link

            # ─────────────────────────────────────────────────────────
            # Google Drive FOLDER link handling (Service Account)
            # ─────────────────────────────────────────────────────────

            if "drive.google.com" in user_input and any(
                x in user_input for x in ("folders/", "open?id=")
            ) and "spreadsheets" not in user_input:

                try:
                    from drive_service import process_drive_folder, extract_folder_id, SERVICE_ACCOUNT_EMAIL
                    SA_AVAILABLE = True
                except ImportError:
                    SA_AVAILABLE = False

                await message.answer(
                    "⏳ <b>Google Drive papka o'qilmoqda...</b>",
                    parse_mode="HTML",
                )

                folder_id = extract_folder_id(user_input)

                if not folder_id:
                    await message.answer(
                        "❌ Papka ID-ni ajratib ola olmadim.\n\n"
                        "📁 Quyidagi formatlardan birini yuboring:\n"
                        "• <code>https://drive.google.com/drive/folders/XXXX</code>\n"
                        "• <code>https://drive.google.com/folders/XXXX</code>\n"
                        "• <code>https://drive.google.com/open?id=XXXX</code>",
                        parse_mode="HTML",
                        reply_markup=build_retry_keyboard("folder"),
                    )
                    session.step = "ready"
                    return

                # ── Service Account path ───────────────────────────────────
                if SA_AVAILABLE:
                    result = await process_drive_folder(user_input)

                    if not result.ok:
                        await message.answer(
                            result.error,
                            parse_mode="HTML",
                            reply_markup=build_retry_keyboard("folder"),
                        )
                        session.step = "ready"
                        return

                    if not result.files:
                        await message.answer(
                            "📂 Papkada qo'llab-quvvatlanadigan fayl topilmadi.\n\n"
                            "Qo'llab-quvvatlanadigan turlar:\n"
                            "• Google Sheets\n• Excel (.xlsx)\n• CSV",
                            reply_markup=build_retry_keyboard("folder"),
                        )
                        session.step = "ready"
                        return

                    # Load all files into session
                    loaded, errors = [], []
                    for fr in result.files:
                        if fr.rows:
                            session.excel_files[fr.name] = fr.rows
                            loaded.append(fr.name)
                            logger.info(
                                "✅ Loaded '%s': %d rows for user %d",
                                fr.name, len(fr.rows), telegram_id,
                            )
                        elif fr.error:
                            errors.append(f"• {fr.name}: {fr.error}")

                    # Set active file to first loaded
                    if loaded:
                        session.active_excel_name = loaded[0]
                        session.excel_data = session.excel_files[loaded[0]]

                    session.folder_id = folder_id

                    # Save to SQLite for persistence
                    try:
                        ctx.session_store.save_excel_to_db(telegram_id, session)
                    except Exception:
                        pass

                    # Build confirmation message
                    summary = f"✅ <b>Google Drive papka ulandi!</b>\n\n"
                    summary += f"📊 <b>{len(loaded)} ta fayl yuklandi:</b>\n"
                    for i, name in enumerate(loaded[:15], 1):
                        rows = len(session.excel_files[name])
                        summary += f"{i}. 📋 {name} ({rows} qator)\n"
                    if len(loaded) > 15:
                        summary += f"... va yana {len(loaded)-15} ta fayl\n"
                    if errors:
                        summary += f"\n⚠️ <b>Yuklanmagan ({len(errors)}):</b>\n"
                        for e in errors[:5]:
                            summary += f"{e}\n"
                    summary += (
                        f"\n💬 <b>Endi savollaringizni bering!</b>\n"
                        f"Misol: <i>Umumiy ball necha? Kimning bali eng yuqori?</i>"
                    )
                    summary += f"\n\n📁 <b>Faollik fayli:</b> {loaded[0] if loaded else '—'}"

                    await message.answer(summary, parse_mode="HTML")
                    session.step = "in_chat"
                    logger.info(
                        "✅ Folder loaded: %d files, %d rows total for user %d",
                        len(loaded), result.total_rows, telegram_id,
                    )

                # ── OAuth fallback (old flow) ──────────────────────────────
                else:
                    if not session.google_credentials_json:
                        await message.answer(
                            "🔐 <b>Google Drive papkani ochish uchun avval ulaning</b>\n\n"
                            "Asosiy menyudagi <b>📊 Google Sheets ulash</b> tugmasini bosing.",
                            parse_mode="HTML",
                            reply_markup=build_main_menu(),
                        )
                        session.step = "ready"
                        return

                    try:
                        creds = credentials_from_json(
                            session.google_credentials_json, telegram_id=telegram_id
                        )
                    except ValueError as e:
                        logger.info("Old scopes for user %d: %s", telegram_id, e)
                        await message.answer(
                            "🔒 <b>Qayta autentifikatsiya kerak</b>\n\n"
                            "Google ruxsatlari eskirgan. "
                            "<b>📊 Google Sheets ulash</b> tugmasini qayta bosing.",
                            parse_mode="HTML",
                            reply_markup=build_main_menu(),
                        )
                        session.google_credentials_json = None
                        session.step = "ready"
                        return

                    try:
                        from google_drive_service import get_all_spreadsheets_from_folder
                        spreadsheets, error = await get_all_spreadsheets_from_folder(
                            creds, user_input
                        )
                    except Exception as e:
                        logger.error("OAuth folder error: %s", e, exc_info=True)
                        await message.answer(
                            f"❌ Papka o'qilmadi: {e}",
                            reply_markup=build_retry_keyboard("folder"),
                        )
                        session.step = "ready"
                        return

                    if error:
                        await message.answer(
                            error,
                            parse_mode="HTML",
                            reply_markup=build_retry_keyboard("folder"),
                        )
                        session.step = "ready"
                        return

                    if not spreadsheets:
                        await message.answer(
                            "❌ Papkada spreadsheet topilmadi.",
                            reply_markup=build_retry_keyboard("folder"),
                        )
                        session.step = "ready"
                        return

                    session.folder_id = folder_id
                    session.folder_spreadsheets = spreadsheets
                    session.selected_spreadsheets = []

                    folder_summary = (
                        f"✅ <b>Google Drive papka ulandi!</b>\n\n"
                        f"📊 <b>{len(spreadsheets)} ta spreadsheet topildi:</b>\n\n"
                    )
                    keyboard_buttons = []
                    for idx, sheet in enumerate(spreadsheets[:20]):
                        sname = sheet["name"][:27] + "..." if len(sheet["name"]) > 30 else sheet["name"]
                        folder_summary += f"{idx+1}. 📋 {sheet['name']}\n"
                        keyboard_buttons.append([
                            InlineKeyboardButton(
                                text=f"📋 {sname}",
                                callback_data=f"select_sheet:{idx}",
                            )
                        ])
                    if len(spreadsheets) > 20:
                        folder_summary += f"\n... va yana {len(spreadsheets)-20} ta fayl"
                    keyboard_buttons.append([
                        InlineKeyboardButton(text="✅ Barchasini yuklash", callback_data="load_folder_sheets")
                    ])
                    keyboard_buttons.append([
                        InlineKeyboardButton(text="📍 Qayta yuborish", callback_data="folder")
                    ])

                    await message.answer(
                        folder_summary,
                        parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons),
                    )
                    session.step = "selecting_folder_sheets"

            else:
                await message.answer(
                    "❌ Bu Google Drive papka linki emas.\n\n"
                    "📁 Quyidagi formatda yuboring:\n"
                    "• <code>https://drive.google.com/drive/folders/XXXX</code>\n"
                    "• <code>https://drive.google.com/folders/XXXX</code>\n"
                    "• <code>https://drive.google.com/open?id=XXXX</code>",
                    parse_mode="HTML",
                    reply_markup=build_retry_keyboard("folder"),
                )


        # ====== IN CHAT MODE ======

        elif session.step == "in_chat":

            try:

                user_message = message.text.strip()

                

                if not user_message:

                    await message.answer("❌ Xabar bo'sh bo'is not allowed.")

                    return

                

                # Show typing indicator

                if ctx.bot:

                    await ctx.bot.send_chat_action(message.chat.id, "typing")

                

                # Send instant "waiting" message so user knows bot received the request

                waiting_msg = await message.answer(

                    "⏳ <b>Biroz kuting, javob tayyorlanmoqda...</b>",

                    parse_mode="HTML"

                )

                

                logger.info(f"💬 Chat message from {telegram_id}: {user_message[:50]}")

                logger.info(f"📊 Session data check: sheet_id={session.sheet_id}, "

                           f"all_sheets_data={len(session.all_sheets_data) if session.all_sheets_data else 0} sheets, "

                           f"all_folder_sheets_data={len(session.all_folder_sheets_data) if session.all_folder_sheets_data else 0} spreadsheets, "

                           f"excel_data={len(session.excel_data) if session.excel_data else 0} rows")

                

                # ===== NEW: Check if indexing_service is available =====

                if session.indexing_service:

                    logger.info(f"🤖 Using DataIndexingService for user {telegram_id}")

                    

                    # Query from indexed data

                    success, answer = await session.indexing_service.query_index(user_message)

                    

                    if success:

                        response_text = f"💬 <b>AI Javob (from Indexed Data)</b>\n\n{html_escape(answer)}"

                        

                        # Split response if too long

                        try:

                            if len(response_text) > 4000:

                                parts = [response_text[i:i+4000] for i in range(0, len(response_text), 4000)]

                                for i, part in enumerate(parts):

                                    if i == len(parts) - 1:

                                        await message.answer(part, parse_mode="HTML", reply_markup=build_chat_response_keyboard())

                                    else:

                                        await message.answer(part, parse_mode="HTML")

                            else:

                                await message.answer(response_text, parse_mode="HTML", reply_markup=build_chat_response_keyboard())

                        except Exception:

                            # Fallback: send without HTML if parsing fails

                            plain_text = response_text.replace("<b>", "").replace("</b>", "")

                            await message.answer(plain_text, reply_markup=build_chat_response_keyboard())

                        

                        return

                    else:

                        # Fallback to regular response if indexing fails

                        logger.warning(f"⚠️ Indexing query failed: {answer}")

                        await message.answer(f"⚠️ Indeksdan javob olishda xatolik: {html_escape(str(answer))}")

                        return

                # ===== END: DataIndexingService =====

                

                # Check if we have local spreadsheet data (from folder or single sheet)

                local_context = None

                

                # If user explicitly chose web search mode — skip spreadsheet entirely

                if session.web_search_mode:

                    logger.info(f"🌐 Web search mode active for user {telegram_id} — skipping spreadsheet")

                    local_context = None

                else:

                    # Priority 1: Folder sheets data

                    if session.all_folder_sheets_data:

                        local_context = session.all_folder_sheets_data

                        logger.info(f"📁 Using folder sheets data with {len(local_context)} spreadsheets")

                    # Priority 2: Single sheet data

                    elif session.all_sheets_data:

                        local_context = {session.sheet_id or "sheet": session.all_sheets_data}

                        logger.info(f"📊 Using single sheet data: {list(session.all_sheets_data.keys())}")

                    # Priority 3: excel_files dict (multi-file from Drive SA folder)
                    elif session.excel_files:
                        # Combine ALL loaded files into one context
                        combined = {}
                        for fname, frows in session.excel_files.items():
                            combined[fname] = frows
                        local_context = {"excel_files": {k: {"Sheet1": v} for k, v in combined.items()}}
                        logger.info(f"📁 Using excel_files dict: {len(combined)} files")

                    # Priority 4: Single legacy Excel data
                    elif session.excel_data:
                        local_context = {"excel": {"Sheet1": session.excel_data}}
                        logger.info(f"📄 Using legacy excel_data: {len(session.excel_data)} rows")

                    else:

                        logger.info(f"⚠️ No spreadsheet data in session. sheet_id={session.sheet_id}, all_sheets_data={bool(session.all_sheets_data)}, all_folder_sheets_data={bool(session.all_folder_sheets_data)}, excel_data={bool(session.excel_data)}")

                

                # If we have local data, use it for context

                if local_context:

                    try:

                        # Build context from local spreadsheets — read EVERYTHING

                        context_text = ""

                        

                        for sheet_id, sheets in local_context.items():

                            # Get sheet name from folder_spreadsheets if available

                            sheet_name = next(

                                (s['name'] for s in session.folder_spreadsheets if s['id'] == sheet_id),

                                sheet_id

                            ) if session.folder_spreadsheets else (session.sheet_name or "Spreadsheet")

                            

                            context_text += f"=== Spreadsheet: {sheet_name} ===\n"

                            

                            for sheet_title, rows in sheets.items():

                                context_text += f"\n--- Sheet: {sheet_title} ---\n"

                                if not rows:

                                    context_text += "(empty)\n"

                                    continue

                                

                                # Read ALL rows up to limit, with FULL cell values

                                for i, row in enumerate(rows[:MAX_ROWS_FOR_CONTEXT]):

                                    # Filter out completely empty cells at the end

                                    while row and str(row[-1]).strip() == "":

                                        row = row[:-1]

                                    if not row:

                                        continue  # skip completely empty rows

                                    

                                    # Full cell values — no per-cell truncation

                                    cells = [str(x).strip() for x in row[:MAX_COLS_FOR_CONTEXT]]

                                    context_text += f"Row {i+1}: {' | '.join(cells)}\n"

                                

                                total_rows = len(rows)

                                if total_rows > MAX_ROWS_FOR_CONTEXT:

                                    context_text += f"... and {total_rows - MAX_ROWS_FOR_CONTEXT} more rows\n"

                                context_text += "\n"

                        

                        # Limit context size for API calls

                        context_text = context_text[:MAX_CHARS_CONTEXT]

                        

                        logger.info(f"🧠 Built local context: {len(context_text)} chars")

                        # DEBUG: Log first 500 chars of context to verify data is actually there

                        logger.info(f"🧠 Context preview (first 500 chars):\n{context_text[:500]}")

                        logger.info(f"🧠 Context preview (last 300 chars):\n{context_text[-300:]}")

                        

                        # ▶▶ Use Grok AI (xAI) to answer based on spreadsheet data ▶▶

                        

                        grok_api_key = os.getenv("GROK_API_KEY", "")

                        

                        if grok_api_key:

                            try:

                                logger.info(f"💬 Sending to Grok AI for spreadsheet Q&A")

                                

                                system_prompt = (

                                                                        "You are a data assistant that reads spreadsheet data and answers questions in Uzbek. RULES:\n"

                                    "1. Answer ONLY from the data provided. Do NOT use outside knowledge.\n"

                                    "2. UZBEK SUFFIX STRIPPING: Questions are in Uzbek and names may have grammatical suffixes. Strip these suffixes before searching: -ni, -ning, -ga, -da, -dan, -lar, -larni, -larning. Example: 'Yodgor ni' -> search for 'Yodgor'. 'Moxizoda ning' -> search for 'Moxizoda'.\n"

                                    "3. NAME MATCHING RULES:\n"

                                    "   a) EXACT SUBSTRING: The searched name (after suffix stripping) must appear as a substring inside the cell value. Example: 'Jasur' matches 'Jasurbek' because 'Jasur' is inside 'Jasurbek'.\n"

                                    "   b) NO PHONETIC GUESSING: Do NOT match names that merely sound similar. 'Zio', 'Ziyo', 'Muhammadziyo' and 'Moxizoda' are completely different people — never substitute one for another. EXCEPTION: Uzbek vowel swap (a=o): 'Yadgar'='Yodgor' if first 3 chars match.\n"

                                    "   c) SHORT FORMS ONLY: Only allow prefix/suffix shortening within the SAME name. 'Yodgor' can match 'Yodgorbek'. 'Jasur' can match 'Jasurbek'. But 'Mox' does NOT match 'Muhammad'. 'Zio' does NOT match 'Moxizoda'.\n"

                                    "   d) CYRILLIC/LATIN: Treat Cyrillic and Latin spellings as equivalent where applicable.\n"

                                    "4. FATHER'S NAME EXCEPTION: Do NOT match a name that only appears as a father's name in a patronymic suffix like 'O\'G\'LI' or 'QIZI' (meaning son/daughter of). Example: 'BAHRIDDIN ILYOSBEK O\'G\'LI' is NOT a match for 'Ilyosbek' because Ilyosbek is the father, not the person.\n"

                                    "5. MULTI-PERSON QUERIES: If the user asks about 2 or more people:\n"

                                    "   - Answer EACH person separately.\n"

                                    "   - If a person IS found in the data: give their information.\n"

                                    "   - If a person is NOT found: explicitly state 'X jadvalda topilmadi' for that person only.\n"

                                    "   - NEVER skip a person silently or substitute another person's data.\n"

                                    "6. If data IS found: give a direct answer. Mention the sheet name.\n"

                                    "7. Format numbers: 2500000 -> 2,500,000\n"

                                    "8. If truly not found after thorough search: respond 'Bu ma\'lumot jadvalda mavjud emas.'\n"

                                    "9. Answer in Uzbek language.\n"

                                    "10. Check ALL rows and ALL columns — data layout may vary."

                                )

                                

                                user_prompt = (

                                    f"Spreadsheet data:\n\n{context_text}\n\n"

                                    f"Question: {user_message}\n\n"

                                    f"IMPORTANT: Answer ONLY from the data above. "

                                    f"If the requested information is not present in the data, respond with: 'Bu ma'lumot jadvalda mavjud emas.' "

                                    f"Do NOT invent or guess any answer."

                                )

                                

                                logger.info(f"📤 Grok request: system_prompt={len(system_prompt)} chars, user_prompt={len(user_prompt)} chars (context={len(context_text)} chars)")

                                

                                # Try grok-3-mini-fast first, fallback to other models

                                grok_models = ["grok-3-mini-fast", "grok-2-latest"]

                                ai_answer = None

                                last_error = ""

                                

                                for model_name in grok_models:

                                    try:

                                        logger.info(f"💬 Trying Grok model: {model_name}")

                                        

                                        # Build messages with conversation history for follow-up questions
                                        grok_messages = [{"role": "system", "content": system_prompt}]
                                        # Include last 10 conversation turns for context
                                        if session.chat_history:
                                            grok_messages.extend(session.chat_history[-10:])
                                        grok_messages.append({"role": "user", "content": user_prompt})

                                        grok_payload = {

                                            "model": model_name,

                                            "messages": grok_messages,

                                            "temperature": 0.3,

                                            "max_tokens": 2000,

                                        }

                                        grok_headers = {

                                            "Authorization": f"Bearer {grok_api_key}",

                                            "Content-Type": "application/json",

                                        }

                                        

                                        async with aiohttp.ClientSession() as _grok_http:

                                            async with _grok_http.post(

                                                "https://api.x.ai/v1/chat/completions",

                                                headers=grok_headers,

                                                json=grok_payload,

                                                timeout=aiohttp.ClientTimeout(total=45),

                                            ) as grok_resp:

                                                if grok_resp.status == 200:

                                                    grok_data = await grok_resp.json()

                                                    ai_answer = grok_data["choices"][0]["message"]["content"]

                                                    logger.info(f"✅ Grok AI ({model_name}) answer: {ai_answer[:100]}...")

                                                    break

                                                else:

                                                    resp_text = await grok_resp.text()

                                                    last_error = f"{model_name}: {grok_resp.status} - {resp_text[:200]}"

                                                    logger.warning(f"⚠️ Grok model {model_name} failed: {last_error}")

                                    except Exception as model_err:

                                        last_error = f"{model_name}: {str(model_err)[:200]}"

                                        logger.warning(f"⚠️ Grok model {model_name} error: {model_err}")

                                

                                if ai_answer:

                                    # Save conversation for follow-up questions
                                    session.chat_history.append({"role": "user", "content": user_message})
                                    session.chat_history.append({"role": "assistant", "content": ai_answer})
                                    # Keep only last 20 messages to avoid token overflow
                                    if len(session.chat_history) > 20:
                                        session.chat_history = session.chat_history[-20:]

                                    response_text = f"💬 AI Javob\n\n{ai_answer}"

                                else:

                                    logger.error(f"❌ All Grok models failed. Last error: {last_error}")

                                    response_text = (

                                        f"💬 AI Javob\n\n"

                                        f"⚠️ AI xizmatida vaqtinchalik xatolik ({last_error[:100]})\n"

                                        f"Ma'lumotlar:\n{context_text[:2000]}"

                                    )

                            except Exception as grok_err:

                                logger.error(f"❌ Grok AI error: {grok_err}")

                                # Fallback: show raw data summary

                                response_text = (

                                    f"💬 AI Javob\n\n"

                                    f"⚠️ AI service is temporarily unavailable.\n"

                                    f"Data:\n{context_text[:2000]}"

                                )

                        else:

                            # No Grok key — show raw data as before

                            logger.warning("⚠️ GROK_API_KEY not set, showing raw spreadsheet data")

                            response_text = (

                                f"💬 AI Javob\n\n"

                                f"⚠️ AI key not configured. Data:\n{context_text[:2000]}"

                            )

                        

                        # Delete the waiting message now that answer is ready

                        try:

                            await waiting_msg.delete()

                        except Exception:

                            pass

                        

                        # Send response to user

                        try:

                            if len(response_text) > 4000:

                                parts = [response_text[i:i+4000] for i in range(0, len(response_text), 4000)]

                                for i, part in enumerate(parts):

                                    if i == len(parts) - 1:

                                        await message.answer(part, reply_markup=build_chat_response_keyboard())

                                    else:

                                        await message.answer(part)

                            else:

                                await message.answer(response_text, reply_markup=build_chat_response_keyboard())

                        

                        except Exception as ai_error:

                            logger.error(f"❌ Response error: {ai_error}")

                            await message.answer("❌ Javob yaratishda xatolik. Qaytadan urinib ko'ring")

                        

                        return

                    

                    except Exception as context_error:

                        logger.error(f"❌ Context building error: {context_error}")

                        # Fall through to web search

                

                # Fall back to web search if no local data

                logger.info(f"🔗 Using web search (no local spreadsheet data)")

                

                # Show waiting message for Tavily web search

                try:

                    waiting_msg = await message.answer(

                        "🌐 <b>Internet qidirilmoqda, biroz kuting...</b>",

                        parse_mode="HTML"

                    )

                except Exception:

                    waiting_msg = None

                

                # Get response using Tavily API (web search + AI synthesis)

                try:

                    import html

                    

                    tavily_api_key = os.getenv("TAVILY_API_KEY")

                    

                    if not tavily_api_key:

                        await message.answer("❌ Tavily API kaliti topilmadi!")

                        return

                    

                    logger.info(f"🔍 Using Tavily API for user {telegram_id}")

                    

                    # Use Tavily API to search and get AI-synthesized answer

                    tavily_response = requests.post(

                        "https://api.tavily.com/search",

                        json={

                            "api_key": tavily_api_key,

                            "query": user_message,

                            "include_answer": True,

                            "max_results": 5,

                            "include_images": False,

                        },

                        timeout=10

                    )

                    

                    logger.info(f"📊 Tavily response status: {tavily_response.status_code}")

                    

                    if tavily_response.status_code == 200:

                        search_results = tavily_response.json()

                        

                        # Get Tavily's AI-synthesized answer

                        ai_answer = search_results.get("answer")

                        

                        if ai_answer:

                            logger.info(f"✅ Tavily answer received for: {user_message[:30]}")

                            

                            # Grok already answers in Uzbek — no translation needed

                            uzbek_answer = ai_answer


                            # Decode HTML entities (like &#39; to ')

                            uzbek_answer = html.unescape(uzbek_answer)

                            

                            # Format response with sources

                            response_text = f"💬 AI Javob\n\n{uzbek_answer}"

                            

                            # Add sources if available

                            if search_results.get("results"):

                                response_text += "\n\n📚 Sources:\n"

                                for i, result in enumerate(search_results["results"][:3], 1):

                                    if result.get("title"):

                                        title = html.unescape(result['title'])

                                        response_text += f"{i}. {title}\n"

                            

                            logger.info(f"✅ Response sent to {telegram_id}: {uzbek_answer[:50]}...")

                            

                            # Delete the "searching..." waiting message

                            if waiting_msg:

                                try:

                                    await waiting_msg.delete()

                                except Exception:

                                    pass

                            

                            exit_keyboard = InlineKeyboardMarkup(

                                inline_keyboard=[

                                    [InlineKeyboardButton(text="🚪 Chat-ni tugatish", callback_data="exit_chat")]

                                ]

                            )

                            

                            # Split response if too long (Telegram limit is 4096)

                            # Send as plain text to avoid Markdown parse errors from

                            # translated text / web content containing special chars

                            try:

                                if len(response_text) > 4000:

                                    parts = [response_text[i:i+4000] for i in range(0, len(response_text), 4000)]

                                    for i, part in enumerate(parts):

                                        if i == len(parts) - 1:

                                            await message.answer(part, reply_markup=exit_keyboard)

                                        else:

                                            await message.answer(part)

                                else:

                                    await message.answer(response_text, reply_markup=exit_keyboard)

                            except Exception as send_err:

                                logger.warning("AI response send failed: %s", send_err)

                                await message.answer("❌ Javobni yuborishda xatolik. Qaytadan urinib ko'ring")

                        else:

                            await message.answer("❌ Tavily javob bera olmadi. Keyinroq urinib ko'ring!")

                            logger.warning(f"⚠️  Tavily returned no answer for: {user_message[:30]}")

                    else:

                        error_msg = tavily_response.text

                        logger.error(f"❌ Tavily error {tavily_response.status_code}: {error_msg[:100]}")

                        await message.answer(f"❌ Javob olishda xatolik: {tavily_response.status_code}")

                    

                except Exception as e:

                    error_msg = str(e)

                    logger.error(f"❌ Tavily error: {error_msg}")

                    await message.answer("❌ AI javob xatoligi. Qaytadan urinib ko'ring")

                

                return

            except Exception as exc:

                logger.exception(f"❌ Chat handler error: {exc}")

                await message.answer("❌ Xatolik yuz berdi. Qaytadan urinib ko'ring")

                return

        

        # ====== WAITING FOR NAME ======

        if session.step == "waiting_first_name":

            try:

                full_name = input_validator.sanitize_string(message.text.strip())

                

                # ========== SECURITY: Validate Name ==========

                if not input_validator.validate_name(full_name):

                    await message.answer("❌ Ism noto'g'ri formatda. Qaytadan urinib ko'ring.")

                    return

                

                if len(full_name) < 2:

                    await message.answer("❌ Kamida 2 belgidan iborat ism kiriting.")

                    return

                

                session.full_name = full_name

                session.step = "waiting_contact"

                

                logger.info(f"✅ User {telegram_id} entered name: {full_name}")

                

                await message.answer(

                    "📱 <b>Telefon raqamingizni ulashing:</b>\n\n"

                    "Pastdagi tugmani bosing 👇",

                    reply_markup=ReplyKeyboardMarkup(

                        keyboard=[

                            [KeyboardButton(text="📱 Kontaktni ulashish", request_contact=True)],

                        ],

                        resize_keyboard=True,

                        one_time_keyboard=True,

                    ),

                    parse_mode="HTML"

                )

                

            except Exception as exc:

                logger.exception(f"❌ Ism kiritishda xato: {exc}")

                await message.answer("❌ Xatolik yuz berdi. Qaytadan urinib ko'ring")

            return  # IMPORTANT: Return to prevent further processing

        

        # ====== WAITING FOR LAST NAME ======

        if session.step == "waiting_last_name":

            try:

                last_name = input_validator.sanitize_string(message.text.strip())

                

                # ========== SECURITY: Validate Name ==========

                if not input_validator.validate_name(last_name):

                    await message.answer("❌ Familiya faqat haqiqiy belgilardan iborat bo'lishi kerak. Qaytadan urinib ko'ring.")

                    return

                

                if len(last_name) < 2:

                    await message.answer("❌ Kamida 2 belgidan iborat familiya kiriting.")

                    return

                

                session.full_name = f"{session.full_name} {last_name}"

                session.step = "waiting_contact"

                

                logger.info(f"✅ User {telegram_id} entered last name: {last_name}")

                

                await message.answer(

                    "📱 <b>Endi kontaktingizni ulashing:</b>\n\n"

                    "Pastdagi tugmani bosing 👇",

                    reply_markup=ReplyKeyboardMarkup(

                        keyboard=[

                            [KeyboardButton(text="📱 Kontaktni ulashish", request_contact=True)],

                        ],

                        resize_keyboard=True,

                        one_time_keyboard=True,

                    ),

                    parse_mode="HTML"

                )

                

            except Exception as exc:

                logger.exception(f"❌ Error entering last name: {exc}")

                await message.answer("❌ Xatolik yuz berdi. Qaytadan urinib ko'ring")

            return  # IMPORTANT: Return to prevent further processing

        

        # ====== OTHER TEXT MESSAGES (MENU HANDLING) ======

        # Davom etish with existing menu handlers

        # (This handler will now delegate to other handlers if step != waiting_first_name/last_name)




    # ============================================================
    # VOICE MESSAGE HANDLER — Groq Whisper (Uzbek transcription)
    # ============================================================

    @dp.message(F.voice)

    async def voice_handler(message: Message, bot: Bot) -> None:

        """Handles voice messages: transcribes via OpenAI Whisper then routes as text."""

        telegram_id = message.from_user.id

        

        # Rate limiting

        if not rate_limiter.is_allowed(telegram_id):

            await message.answer(

                "⚙️⚠️ <b>Juda ko'p so'rovlar!</b>\n\nBiroz kuting va qaytadan urinib ko'ring.",

                parse_mode="HTML"

            )

            return

        

        # Check OpenAI Whisper availability

        openai_whisper_key = ctx.config.openai_whisper_key or os.getenv("OPENAI_API_KEY", "").strip()

        if not OPENAI_WHISPER_AVAILABLE or not openai_whisper_key:

            await message.answer(

                "🎤 <b>Ovozli xabar qabul qilindi</b>\n\n"

                "❌ Ovozni matnга aylantirish hozircha mavjud emas.\n"

                "Iltimos, savolingizni yozma holda yuboring.",

                parse_mode="HTML"

            )

            return

        

        session = ctx.sessions.get(telegram_id)

        

        # Make sure user is registered

        if session.step not in ("in_chat", "ready", "waiting_question"):

            await message.answer(

                "Iltimos, avval jadval ulang (Google Sheets, Excel yoki Folder), keyin ovozli savol yuboring."

            )

            return

        

        # Show typing indicator while processing

        processing_msg = await message.answer(

            "🎤 <b>Ovozingiz tinglanmoqda...</b>",

            parse_mode="HTML"

        )

        

        try:

            # 1. Download voice file from Telegram

            voice = message.voice

            file_info = await bot.get_file(voice.file_id)

            file_bytes = await bot.download_file(file_info.file_path)

            audio_bytes = file_bytes.read() if hasattr(file_bytes, "read") else bytes(file_bytes)

            

            # 2. Transcribe via production audio pipeline
            #    (OGG→WAV, silence-based chunking, Whisper with retry)

            logger.info(
                f"🔑 OpenAI key: {bool(openai_whisper_key)}, "
                f"audio: {len(audio_bytes):,}B, "
                f"pipeline: {'audio_processor' if AUDIO_PROCESSOR_AVAILABLE else 'inline'}"
            )

            if AUDIO_PROCESSOR_AVAILABLE:
                # Full pipeline: OGG→WAV conversion, chunking, retry
                # Build dynamic vocabulary from loaded sheet data so Whisper
                # can spell student names and column names correctly
                try:
                    from audio_processor import build_sheet_vocabulary as _build_vocab
                    _sheet_vocab = _build_vocab(session)
                except Exception:
                    _sheet_vocab = ""
                transcribed_text, error_reason = await _transcribe_audio(
                    audio_bytes, openai_whisper_key, sheet_vocabulary=_sheet_vocab
                )
            else:
                # Fallback: direct Whisper call without preprocessing
                logger.warning('⚠️ audio_processor unavailable — using inline Whisper')
                openai_client = _AsyncOpenAI(api_key=openai_whisper_key)
                raw = await openai_client.audio.transcriptions.create(
                    model='whisper-1',
                    file=('voice.ogg', io.BytesIO(audio_bytes), 'audio/ogg'),
                    response_format='text',
                    prompt='O\'zbek tilida so\'zlashuv. Ismlar, fanlar, ball, umumiy ball.',
                )
                transcribed_text = raw.strip() if isinstance(raw, str) else str(raw).strip()
                error_reason = '' if transcribed_text else 'empty_transcription'

            if not transcribed_text:
                _err_msgs = {
                    'all_chunks_failed':   '❌ Ovozni matnga aylantirish muvaffaqiyatsiz. Qayta urinib ko\'ring.',
                    'no_speech_detected':  '🎤 Ovozda nutq aniqlanmadi. Iltimos, aniqroq gapiring.',
                    'empty_transcription': '🎤 Ovozingiz tushunilmadi. Yozma holda yuboring.',
                    'openai_not_installed':'❌ OpenAI kutubxonasi o\'rnatilmagan.',
                }
                msg = _err_msgs.get(error_reason, '🎤 Ovozingiz tushunilmadi. Qayta urinib ko\'ring.')
                await processing_msg.edit_text(msg)
                return

            logger.info(f"🎤 Voice transcribed for {telegram_id}: {transcribed_text[:80]}")

            # 3. Edit the processing message to show what was heard

            await processing_msg.edit_text(

                f"🎤 <b>Eshitildi:</b> <i>{transcribed_text}</i>\n\n"

                f"⏳ Javob tayyorlanmoqda...",

                parse_mode="HTML"

            )

            

            # 4. Create a fake message-like object and route through text_handler logic

            # We do this by sending the transcribed text as a new message directly

            # But since we can't fake a Message object, we duplicate the core text processing inline

            # Build a simple forwarding: re-use the session's existing data

            await processing_msg.delete()

            

            # Send the transcribed text back to user as their "typed" message, then trigger answer

            fake_text_msg = None  # removed visible echo — transcription feeds directly into AI
            logger.info(f"🎤 Voice question from {telegram_id}: {transcribed_text[:80]}")

            

            # 5. Now simulate the text_handler call with the transcribed text

            # We directly copy the message and override text

            # (message.text assignment removed — Message is immutable in aiogram)

            

            # Call the registered text handler logic by dispatching manually

            # The cleanest way: import the inner function and call with modified message

            # Since handlers are registered in dp, easiest is to build Answer directly here

            # using same Grok call as text_handler does for "ready" session

            

            # Get session data

            user_message = transcribed_text

            

            # Determine data source — identical priority chain to text_handler
            local_context = None
            if session.all_folder_sheets_data:
                local_context = session.all_folder_sheets_data
            elif session.all_sheets_data:
                local_context = {session.sheet_id or "sheet": session.all_sheets_data}
            elif getattr(session, 'excel_files', None):
                combined = {}
                for fname, frows in session.excel_files.items():
                    combined[fname] = frows
                local_context = {"excel_files": {k: {"Sheet1": v} for k, v in combined.items()}}
            elif session.excel_data:
                local_context = {"excel": {"Sheet1": session.excel_data}}

            if not local_context:
                await message.answer(
                    "📊 Hali hech qanday jadval yuklanmagan.\n\n"
                    "Avval Google Sheets, Excel yoki Folder ulang.",
                    parse_mode="HTML"
                )
                return

            # Build context — identical Row N: col | col format to text_handler
            context_text = ""

            def _render_rows(rows_list: list, label: str) -> str:
                """Render a flat list of rows into context lines."""
                out = f"\n--- Sheet: {label} ---\n"
                if not rows_list:
                    return out + "(empty)\n"
                for idx, row in enumerate(rows_list[:MAX_ROWS_FOR_CONTEXT]):
                    row = list(row)  # ensure list (not tuple)
                    while row and str(row[-1]).strip() == "":
                        row.pop()
                    if not row:
                        continue
                    cells = [str(x).strip() for x in row[:MAX_COLS_FOR_CONTEXT]]
                    out += f"Row {idx+1}: {' | '.join(cells)}\n"
                total = len(rows_list)
                if total > MAX_ROWS_FOR_CONTEXT:
                    out += f"... and {total - MAX_ROWS_FOR_CONTEXT} more rows\n"
                return out + "\n"

            for sheet_id, sheets in local_context.items():
                sheet_name = next(
                    (s['name'] for s in session.folder_spreadsheets if s['id'] == sheet_id),
                    sheet_id
                ) if session.folder_spreadsheets else (session.sheet_name or sheet_id)
                context_text += f"=== Spreadsheet: {sheet_name} ===\n"
                for sheet_title, rows in sheets.items():
                    # rows can be a list (normal) or dict {tab: [rows]} (excel_files path)
                    if isinstance(rows, dict):
                        for tab_name, tab_rows in rows.items():
                            context_text += _render_rows(tab_rows, f"{sheet_title} / {tab_name}")
                    else:
                        context_text += _render_rows(rows, sheet_title)

            context_text = context_text[:MAX_CHARS_CONTEXT]

            if not context_text.strip():
                await message.answer("📊 Jadvalda ma'lumot topilmadi.")
                return

            

            # Grok API key

            grok_api_key_val = os.getenv("GROK_API_KEY", "")

            if not grok_api_key_val:

                await message.answer(f"📋 <b>Ma'lumot:</b>\n{context_text[:1000]}", parse_mode="HTML")

                return

            

            # Build Grok system prompt (same as text_handler)

            system_prompt = (

                                "You are a data assistant that reads spreadsheet data and answers questions in Uzbek. IMPORTANT: This question came from a VOICE MESSAGE transcribed by speech recognition. The name in the question may be slightly misspelled due to transcription errors. Use the transcribed name as-is for matching — do NOT substitute a completely different name.\n"

                "RULES:\n"

                "1. Answer ONLY from the data provided. Do NOT use outside knowledge.\n"

                "2. UZBEK SUFFIX STRIPPING: Questions are in Uzbek and names may have grammatical suffixes. Strip these suffixes before searching: -ni, -ning, -ga, -da, -dan, -lar, -larni, -larning. Example: 'Yodgor ni' -> search for 'Yodgor'. 'Moxizoda ning' -> search for 'Moxizoda'.\n"

                "3. NAME MATCHING RULES:\n"

                "   a) EXACT SUBSTRING: The searched name (after suffix stripping) must appear as a substring inside the cell value. Example: 'Jasur' matches 'Jasurbek' because 'Jasur' is inside 'Jasurbek'.\n"

                "   b) NO PHONETIC GUESSING: Do NOT match names that merely sound similar. 'Zio', 'Ziyo', 'Muhammadziyo' and 'Moxizoda' are completely different people — never substitute one for another. If 'Muhammadziyo' is asked, look for rows containing 'Muhammadziyo' or 'Muhammad Ziyo' — NOT 'Moxizoda'.\n"

                "   c) SHORT FORMS ONLY: Only allow prefix shortening within the SAME name. 'Yodgor' can match 'Yodgorbek'. 'Jasur' can match 'Jasurbek'. But 'Zio' does NOT match 'Moxizoda' or any other unrelated name.\n"

                "   d) CYRILLIC/LATIN: Treat Cyrillic and Latin spellings as equivalent where applicable.\n"

                "4. FATHER'S NAME EXCEPTION: Do NOT match a name that only appears as a father's name in a patronymic suffix like 'O\'G\'LI' or 'QIZI' (meaning son/daughter of).\n"

                "5. MULTI-PERSON QUERIES: If the user asks about 2 or more people:\n"

                "   - Answer EACH person separately.\n"

                "   - If a person IS found in the data: give their information.\n"

                "   - If a person is NOT found: explicitly state 'X jadvalda topilmadi' for that person only.\n"

                "   - NEVER skip a person silently or substitute another person's data.\n"

                "6. If data IS found: give a direct answer. Mention the sheet name.\n"

                "7. Format numbers: 2500000 -> 2,500,000\n"

                "8. If truly not found after thorough search: respond 'Bu ma\'lumot jadvalda mavjud emas.'\n"

                "9. Answer in Uzbek language.\n"

                "10. Check ALL rows and ALL columns — data layout may vary."

            )

            

            user_prompt = (

                f"Spreadsheet data:\n\n{context_text}\n\n"

                f"Question: {user_message}\n\n"

                f"IMPORTANT: Answer ONLY from the data above. "

                f"If the requested information is not present in the data, respond with: 'Bu ma'lumot jadvalda mavjud emas.' "

                f"Do NOT invent or guess any answer."

            )

            

            # Build messages with conversation history

            grok_messages = [{"role": "system", "content": system_prompt}]

            if session.chat_history:

                grok_messages.extend(session.chat_history[-10:])

            grok_messages.append({"role": "user", "content": user_prompt})

            

            thinking_msg = await message.answer("⏳ <b>AI javob tayyorlanmoqda...</b>", parse_mode="HTML")

            

            try:

                async with httpx.AsyncClient(timeout=45.0) as client:

                    grok_resp = await client.post(

                        "https://api.x.ai/v1/chat/completions",

                        headers={

                            "Content-Type": "application/json",

                            "Authorization": f"Bearer {grok_api_key_val}",

                        },

                        json={

                            "model": "grok-3-mini-fast",

                            "messages": grok_messages,

                            "temperature": 0.3,

                            "max_tokens": 2000,

                        }

                    )

                

                if grok_resp.status_code == 200:

                    grok_data = grok_resp.json()

                    ai_answer = grok_data["choices"][0]["message"]["content"].strip()

                else:

                    ai_answer = None

            

            except Exception as grok_exc:

                logger.warning(f"Voice handler Grok error: {grok_exc}")

                ai_answer = None

            

            await thinking_msg.delete()

            

            if ai_answer:

                # Save to conversation history

                session.chat_history.append({"role": "user", "content": user_message})

                session.chat_history.append({"role": "assistant", "content": ai_answer})

                if len(session.chat_history) > 20:

                    session.chat_history = session.chat_history[-20:]

                

                

                await message.answer(

                    f"💬 <b>AI Javob</b>\n\n{ai_answer}",

                    parse_mode="HTML",

                    reply_markup=build_chat_response_keyboard()

                )


            else:

                await message.answer(

                    "❌ AI javob bera olmadi. Iltimos, qaytadan urinib ko'ring."

                )

        

        except Exception as exc:

            logger.exception(f"❌ Voice handler error for {telegram_id}: {exc}")

            try:

                await processing_msg.delete()

            except Exception:

                pass

            await message.answer(
                f"❌ Xatolik: <code>{type(exc).__name__}: {str(exc)[:200]}</code>\n\n"
                "Iltimos, yozma holda savol yuboring yoki qaytadan urinib ko'ring.",
                parse_mode="HTML"
            )

    

    @dp.message(F.contact)

    async def contact_handler(message: Message) -> None:

        """Handle contact sharing during registration"""

        telegram_id = message.from_user.id

        session = ctx.sessions.get(telegram_id)

        

        logger.info(f"🚀▒ Contact received from user {telegram_id}, step={session.step}")

        

        if session.step != "waiting_contact":

            logger.warning(f"⚠️ User {telegram_id} sent contact at wrong step: {session.step}")

            await message.answer(

                "❌ Kontakt tugmasi bu vaqtda mumkin emas. /start buyrug'ini yuboring.",

                reply_markup=ReplyKeyboardRemove(),

            )

            return

        

        try:

            # Extract phone number from contact

            phone_number = message.contact.phone_number

            

            if not phone_number:

                logger.warning(f"⚠️ User {telegram_id} sent contact without phone number")

                await message.answer(

                    "❌ Telefon raqami topilmadi. Kontaktingizni qaytadan ulashing.",

                    reply_markup=ReplyKeyboardMarkup(

                        keyboard=[

                            [KeyboardButton(text="📱 Kontaktni ulashish", request_contact=True)],

                        ],

                        resize_keyboard=True,

                        one_time_keyboard=True,

                    ),

                )

                return

            

            # ========== SECURITY: Validate Phone Number ==========

            if not input_validator.validate_phone(phone_number):

                logger.warning(f"⚠️ Invalid phone format from user {telegram_id}: {phone_number}")

                await message.answer("❌ Telefon raqami noto'g'ri formatda. Qaytadan urinib ko'ring.")

                return

            

            session.phone_number = phone_number

            user_email = f"{telegram_id}@telegram.local"

            session.email = user_email

            

            logger.info(f"✅ Creating user: {telegram_id}, name={session.full_name}")

            

            # Create user in Supabase

            result = await ctx.supabase_service.create_user(

                telegram_id=telegram_id,

                full_name=session.full_name,

                email=user_email,

                phone_number=phone_number,

            )

            

            if result:

                session.step = "ready"

                logger.info(f"✅ User {telegram_id} registered successfully!")

                

                # Send confirmation

                await message.answer(

                    f"✅ <b>Ro'yxatdan o'tish yakunlandi!</b>\n\n"

                    f"👤 Ism: {session.full_name}\n"

                    f"📱 Telefon: {phone_number}\n"

                    f"🎉 Xush kelibsiz!",

                    reply_markup=ReplyKeyboardRemove(),

                    parse_mode="HTML"

                )

                

                # Show main menu

                await message.answer(

                    "📋 Select the section you need from the menu:",

                    reply_markup=build_main_menu(),

                )

            else:

                raise Exception("User creation failed - result is False")

            

        except Exception as exc:

            logger.exception(f"❌ Contact handler error for {telegram_id}: {exc}")

            session.step = "idle"

            await message.answer(

                f"❌ An error occurred during registration:\n\n{str(exc)}\n\n"

                "/start buyrug'ini yuboring.",

                reply_markup=ReplyKeyboardRemove(),

            )



    @dp.message(F.text == "Cancel")

    async def cancel_registration_handler(message: Message) -> None:

        """Handle cancel during registration"""

        telegram_id = message.from_user.id

        session = ctx.sessions.get(telegram_id)

        

        if session.step in {"waiting_first_name", "waiting_contact"}:

            session.step = "idle"

            session.full_name = None

            session.phone_number = None

            await message.answer(

                "Ro'yxatdan o'tish bekor qilindi.\n\n"

                "Qaytadan ro'yxatdan o'tish uchun /start buyrug'ini yuboring.",

                reply_markup=ReplyKeyboardRemove(),

            )

        else:

            await message.answer("Buyruq rad etildi.", reply_markup=ReplyKeyboardRemove())



    @dp.callback_query(F.data.startswith("sheet:"))

    async def select_sheet_handler(callback: CallbackQuery) -> None:

        telegram_id = callback.from_user.id

        session = ctx.sessions.get(telegram_id)

        key = callback.data.split("sheet:", 1)[1]

        if key not in session.pending_sheets:

            await callback.answer("Jadval havolasi eskirdi. Qayta ulaning", show_alert=True)

            return

        selected_sheet_name = session.pending_sheets[key]

        selected_sheet_id = key.split(":", 1)[1]







        try:

            await callback.message.edit_text(

                UIAnimations.loading_message("loading_sheets", "Jadvalingiz yuklanmoqda"),

                parse_mode="HTML"

            )



            # Use public CSV — no credentials needed for shared sheets

            all_sheets_data = await _fetch_all_public_sheets(selected_sheet_id)



            session.sheet_id = selected_sheet_id

            session.sheet_name = selected_sheet_name

            session.all_sheets_data = all_sheets_data  # Store ALL sheets

            session.sheet_data = []  # Legacy support

            session.excel_data = []

            session.step = "in_chat"

            

            await ctx.supabase_service.save_integration(

                telegram_id, selected_sheet_id, selected_sheet_name

            )



            # ---- Persist workspace to SQLite so it survives restarts ----

            company_id = _workspace_store.save_workspace(

                telegram_id,

                mode="sheets",

                sheet_id=selected_sheet_id,

                sheet_name=selected_sheet_name,

            )

            if company_id > 0:

                for sheet_title, rows in all_sheets_data.items():

                    cache_key = f"sheet:{selected_sheet_id}:{sheet_title}"

                    _workspace_store.save_cache(company_id, cache_key, sheet_title, qator, telegram_id=telegram_id)

                logger.info(f"💾 Sheet workspace persisted to SQLite+Supabase for user {telegram_id}")

            # ---------------------------------------------------------------

            

            # Show summary of all sheets read

            sheet_summary = "📊 Barcha jadvallar o'qildi:\n\n"

            for sheet_name, rows in all_sheets_data.items():

                row_count = len(rows)

                col_count = len(rows[0]) if rows else 0

                sheet_summary += f"📋 {html_escape(sheet_name)}: {row_count} qator, {col_count} ustun\n"

            

            sheet_summary += f"\n✅ Connected: {html_escape(selected_sheet_name)}\n"

            sheet_summary += "💬 Savolingizni yozing, jadval ma'lumotlari asosida javob beraman."

            

            await callback.message.edit_text(sheet_summary, reply_markup=build_chat_response_keyboard())

            await callback.answer("Google Sheet muvaffaqiyatli ulandi.")

        except Exception as exc:

            logger.exception("Error selecting sheet: %s", exc)

            await callback.answer(

                "Error connecting the spreadsheet. Qaytadan urinib ko'ring",

                show_alert=True,

            )



    @dp.callback_query(F.data.startswith("select_sheet:"))

    async def select_folder_sheet_handler(callback: CallbackQuery) -> None:

        """Handle selecting individual sheets from a folder"""

        telegram_id = callback.from_user.id

        session = ctx.sessions.get(telegram_id)

        

        try:

            idx_str = callback.data.split("select_sheet:", 1)[1]

            idx = int(idx_str)

            

            if idx >= len(session.folder_spreadsheets):

                await callback.answer("Jadval topilmadi. Qayta ulaning", show_alert=True)

                return

            

            sheet_id = session.folder_spreadsheets[idx]['id']

            

            # Toggle selection

            if sheet_id in session.selected_spreadsheets:

                session.selected_spreadsheets.remove(sheet_id)

                status = "❌ Olib tashlandi"

            else:

                session.selected_spreadsheets.append(sheet_id)

                status = "✅ Tanlandi"

            

            await callback.answer(f"{status}: {session.folder_spreadsheets[idx]['name']}")

            

        except Exception as exc:

            logger.exception(f"❌ Folder sheet selection error: {exc}")

            await callback.answer("❌ Xatolik yuz berdi!", show_alert=True)



    @dp.callback_query(F.data == "load_folder_sheets")

    async def load_folder_sheets_handler(callback: CallbackQuery) -> None:

        """Load all selected sheets from the folder"""

        telegram_id = callback.from_user.id

        session = ctx.sessions.get(telegram_id)

        

        try:

            if not session.selected_spreadsheets:

                await callback.answer("Jadval tanlanmagan. Birma-bir tanlang!", show_alert=True)

                return

            

            await callback.message.edit_text(

                UIAnimations.loading_message(

                    "processing_files", 

                    f"Processing {len(session.selected_spreadsheets)} files"

                ),

                parse_mode="HTML"

            )

            

            # Get credentials

            try:

                creds = credentials_from_json(session.google_credentials_json, telegram_id=telegram_id)

            except ValueError as scope_error:

                logger.info(f"Old scopes detected for user {telegram_id}: {scope_error}")

                await callback.message.edit_text(

                    "🔒 <b>Re-authentication Required</b>\n\n"

                    "Your Google permissions have expired.\n\n"

                    "Please tap the <b>🔒 Google Sheets</b> button to reconnect.",

                    parse_mode="HTML",

                    reply_markup=build_main_menu()

                )

                session.google_credentials_json = None

                return

            

            # Import the Google Drive service

            from google_drive_service import GoogleDriveService

            

            drive_service = GoogleDriveService(creds)

            

            # Read all selected spreadsheets

            all_folder_sheets_data = {}

            failed_sheets = []

            

            for idx, sheet_id in enumerate(session.selected_spreadsheets, 1):

                try:

                    logger.info(f"📊 Reading spreadsheet {idx}/{len(session.selected_spreadsheets)}: {sheet_id}")

                    

                    sheets_data = await drive_service.read_spreadsheet(sheet_id)

                    all_folder_sheets_data[sheet_id] = sheets_data

                    

                    # Find the name from folder_spreadsheets

                    sheet_name = next(

                        (s['name'] for s in session.folder_spreadsheets if s['id'] == sheet_id),

                        sheet_id

                    )

                    logger.info(f"✅ Successfully read: {sheet_name}")

                    

                except Exception as e:

                    logger.error(f"❌ Failed to read spreadsheet {sheet_id}: {e}")

                    sheet_name = next(

                        (s['name'] for s in session.folder_spreadsheets if s['id'] == sheet_id),

                        sheet_id

                    )

                    failed_sheets.append(sheet_name)

            

            if not all_folder_sheets_data:

                await callback.message.edit_text(

                    "❌ Hech qanday spreadsheet o'qilmadi.\n\n"

                    "💡 Fix the folder link and try again",

                    reply_markup=build_retry_keyboard("folder")

                )

                session.step = "ready"

                return

            

            # Store the data

            session.all_folder_sheets_data = all_folder_sheets_data

            session.step = "in_chat"



            # ---- Persist workspace to SQLite so it survives restarts ----

            folder_url = None

            if session.folder_id:

                folder_url = f"https://drive.google.com/drive/folders/{session.folder_id}"

            company_id = _workspace_store.save_workspace(

                telegram_id,

                mode="folder",

                folder_id=session.folder_id,

                folder_url=folder_url,

                folder_spreadsheets=session.folder_spreadsheets,

                selected_spreadsheets=session.selected_spreadsheets,

            )

            if company_id > 0:

                for sid, sheets_dict in all_folder_sheets_data.items():

                    for sheet_title, rows in sheets_dict.items():

                        cache_key = f"folder:{sid}:{sheet_title}"

                        _workspace_store.save_cache(company_id, cache_key, sheet_title, qator, telegram_id=telegram_id)

                logger.info(f"💾 Folder workspace persisted to SQLite+Supabase for user {telegram_id}")

            # ---------------------------------------------------------------

            

            # Create summary

            summary = "✅ <b>Google Drive spreadsheets connected successfully!</b>\n\n"

            summary += f"📊 <b>{len(all_folder_sheets_data)} ta spreadsheet o'qildi:</b>\n\n"

            

            for sheet_id, sheets in all_folder_sheets_data.items():

                sheet_name = next(

                    (s['name'] for s in session.folder_spreadsheets if s['id'] == sheet_id),

                    "Unknown"

                )

                summary += f"📁 <b>{html_escape(sheet_name)}</b>\n"

                for sheet_title, rows in sheets.items():

                    row_count = len(rows)

                    col_count = len(rows[0]) if rows else 0

                    summary += f"   📊▶ 📋 {html_escape(sheet_title)}: {row_count} qator, {col_count} ustun\n"

                summary += "\n"

            

            if failed_sheets:

                summary += f"\n⚠️ <b>O'qilmagan spreadsheetlar:</b>\n"

                for name in failed_sheets:

                    summary += f"   ❌ {html_escape(name)}\n"

            

            summary += "\n💬 Now type your question, I'll answer based on all spreadsheet data."

            

            try:

                await callback.message.edit_text(

                    summary,

                    parse_mode="HTML",

                    reply_markup=build_chat_response_keyboard()

                )

            except Exception:

                # Fallback: send without HTML if parsing fails

                plain_summary = summary.replace("<b>", "").replace("</b>", "")

                await callback.message.edit_text(

                    plain_summary,

                    reply_markup=build_chat_response_keyboard()

                )

            

            logger.info(f"✅ Successfully loaded {len(all_folder_sheets_data)} spreadsheets for user {telegram_id}")

            

        except Exception as exc:

            logger.exception(f"❌ Error loading folder sheets: {exc}")

            await callback.message.edit_text(

                "❌ Error loading spreadsheets.\n\n"

                "💡 Qaytadan urinib ko'ring",

                reply_markup=build_retry_keyboard("folder")

            )

            session.step = "ready"



    @dp.message(F.document)

    async def document_handler(message: Message, bot: Bot) -> None:

        telegram_id = message.from_user.id

        

        # ========== SECURITY: Rate Limiting ==========

        if not rate_limiter.is_allowed(telegram_id):

            await message.answer(

                "⚙️⚠️ <b>Juda ko'p so'requests!</b>\n\n"

                "Biroz kuting va qaytadan urinib ko'ring'",

                parse_mode="HTML"

            )

            return

        

        session = ctx.sessions.get(telegram_id)

        # Auto-accept Excel/CSV files at any session step.
        # No need to force users to press a button first — just process the file.
        if session.step not in ("waiting_excel", "in_chat", "ready", "waiting_excel_file"):
            session.step = "waiting_excel"



        doc = message.document

        file_name = doc.file_name or ""

        

        # ========== SECURITY: Validate File ==========

        if not file_name:

            await message.answer("❌ Fayl nomi topilmadi.")

            return

        

        # Sanitize filename

        file_name = FileValidator.sanitize_filename(file_name)

        

        # Check file extension

        if not file_name.lower().endswith((".xlsx", ".xls", ".xlsm")):

            await message.answer(

                "❌ Invalid file type.\n\n"

                "Faqat quyidagi formatlar ruxsat:\n"

                "• .xlsx\n"

                "• .xls\n"

                "• .xlsm"

            )

            return



        try:

            # Download file

            telegram_file = await bot.get_file(doc.file_id)

            buffer = io.BytesIO()

            await bot.download_file(telegram_file.file_path, destination=buffer)

            file_content = buffer.getvalue()

            

            # ========== SECURITY: Validate Excel File ==========

            is_valid, error_msg = FileValidator.validate_excel_file(file_name, file_content)

            if not is_valid:

                logger.warning(f"⚠️ File validation failed for user {telegram_id}: {error_msg}")

                await message.answer(error_msg)

                session.step = "ready"

                return

            

            # Parse Excel

            excel_rows = await asyncio.to_thread(parse_excel_bytes, file_name, file_content)

            if not excel_rows:

                await message.answer("❌ Fayl bo'sh ko'rinmoqda. Boshqa fayl yuboring")

                return

            

            session.excel_data = excel_rows

            session.sheet_data = []

            session.sheet_id = None

            session.sheet_name = file_name

            session.step = "in_chat"

            # ── Multi-file support: store in named dict ──────────────────
            session.excel_files[file_name] = excel_rows
            session.active_excel_name = file_name

            # ── Persist to SQLite so data survives session expiry ────────
            ctx.sessions.save_excel_to_db(telegram_id, file_name, excel_rows)

            

            logger.info(f"✅ Excel file loaded for user {telegram_id}: {len(excel_rows)} rows")

            

            await message.answer(

                f"✅ <b>Excel file uploaded successfully!</b>\n\n"

                f"📊 Qatorlar: {len(excel_rows)}\n"

                f"📄 Fayl: {file_name}\n\n"

                f"💬 Savolingizni yozing, jadval ma'lumotlari asosida javob beraman.",

                parse_mode="HTML",

                reply_markup=build_chat_response_keyboard()

            )

        except Exception as exc:

            logger.exception(f"❌ Excel file processing error for user {telegram_id}: {exc}")

            await message.answer(

                "❌ Error reading the Excel file.\n\n"

                "Sabablari:\n"

                "• Fayl korruptsiyalangan\n"

                "• Fayl turi to'g'ri emas\n"

                "• Fayl juda katta\n\n"

                "Boshqa fayl yuboring yoki /start buyrug'ini yuboring."

            )

            session.step = "ready"











async def main() -> None:

    config = Config.from_env()

    context = AppContext(config)

    

    # Initialize oauth_server early so it can be safely accessed in finally block

    oauth_server = None

    

    # Create bot with default settings

    bot = Bot(

        token=config.bot_token,

        default=DefaultBotProperties(parse_mode=ParseMode.HTML),

    )

    context.bot = bot

    

    # Ensure any previous bot sessions are closed

    logger.info("📍 Cleaning up any previous bot sessions...")

    await asyncio.sleep(2)  # Give Telegram time to register session termination

    

    # Test the bot token first

    try:

        me = await bot.get_me()

        logger.info(f"Bot authenticated: {me.first_name} (@{me.username})")

    except Exception as e:

        logger.error(f"Failed to authenticate bot: {e}")

        raise

    

    # Register bot commands (shown at the bottom left)

    try:

        from aiogram.types import BotCommand

        commands = [

            BotCommand(command="start", description="🏠 Asosiy menyu - Boshlanish"),

            BotCommand(command="chat", description="💬 Chat - Assistant bilan suhbat"),

            BotCommand(command="help", description="📋 Yordam - Qo'llanma"),

        ]

        await bot.set_my_commands(commands)

        logger.info("✅ Bot commands registered")

    except Exception as e:

        logger.warning(f"⚠️  Could not register bot commands: {e}")

    

    # Force-close any existing polling session by deleting webhook + dropping updates

    # This kills any old bot instance that's still polling

    try:

        logger.info("📍 Forcefully stopping any existing bot sessions...")

        await bot.delete_webhook(drop_pending_updates=True)

        logger.info("   ✅ Webhook deleted, pending updates dropped")

        # Wait longer for Telegram to fully release the old polling session

        # (needed when migrating from another server like Render)

        await asyncio.sleep(10)

    except Exception as e:

        logger.warning(f"⚠️  Could not clean webhook: {e}")

        await asyncio.sleep(5)

    

    dp = Dispatcher()

    register_handlers(dp, context)



    # OAuth server - REQUIRED for Google Sheets integration

    oauth_server = OAuthServer(context)

    await oauth_server.start()

    

    # Give the HTTP server time to fully bind to the port

    # This prevents "HTTP health check failed" errors on platform restarts

    await asyncio.sleep(2)

    logger.info("✅ HTTP server ready, OAuth callback endpoint available")

    

    logger.info("Starting bot polling loop...")

    polling_task = None

    async def _session_cleanup_loop() -> None:
        """Background task: cleans expired sessions every 5 min
        and sends 30-min expiry warnings to active users."""
        while True:
            await asyncio.sleep(300)  # run every 5 minutes
            try:
                # Send expiry warnings (30 min before session ends)
                warn_ids = context.sessions.get_expiry_warnings()
                for uid in warn_ids:
                    try:
                        await bot.send_message(
                            uid,
                            "⏰ <b>Eslatma:</b> Sizning sessiyangiz <b>30 daqiqa</b> ichida tugaydi.\n\n"
                            "Davom etish uchun fayl qayta yuboring yoki /start bosing.",
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass
                # Remove expired sessions from memory
                context.sessions.cleanup_expired()
            except Exception as e:
                logger.warning(f"⚠️ Session cleanup error: {e}")

    try:

        # Start polling - aiogram handles reconnection automatically

        cleanup_task = asyncio.create_task(_session_cleanup_loop())

        polling_task = asyncio.create_task(

            dp.start_polling(

                bot,

                allowed_updates=dp.resolve_used_update_types(),

                relax_timeout=60.0,

                long_poll_timeout=60.0,

            )

        )

        await polling_task

    except (asyncio.CancelledError, KeyboardInterrupt):

        logger.info("Polling cancelled")

        if polling_task and not polling_task.done():

            polling_task.cancel()

            try:

                await polling_task

            except asyncio.CancelledError:

                pass

    except Exception as e:

        logger.error(f"Polling error: {type(e).__name__}: {e}")

    finally:

        if oauth_server is not None:

            try:

                await oauth_server.stop()  # Stop OAuth server

            except Exception as e:

                logger.warning(f"⚠️ Error stopping OAuth server: {e}")

        try:

            await bot.session.close()

        except:

            pass





if __name__ == "__main__":

    logger.info("🤖 OnBrain AI Bot Starting...")

    while True:

        try:

            asyncio.run(main())

            logger.info("Bot run completed normally")

        except KeyboardInterrupt:

            logger.info("Bot stopped by user (Ctrl+C)")

            break

        except asyncio.CancelledError:

            logger.warning("Bot was cancelled, restarting...")

            time.sleep(10)

        except Exception as e:

            error_name = type(e).__name__

            logger.error(f"Bot error ({error_name}): {e}")

            if "conflict" in str(e).lower() or "terminated by other" in str(e).lower():

                logger.info("⚠️ Another bot instance detected. Waiting 15s for it to stop...")

                time.sleep(15)

            else:

                logger.info("Restarting bot in 5 seconds...")

                time.sleep(5)

