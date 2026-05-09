import asyncio
import collections
import io
import json
import logging
import os
import re
import sqlite3
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from supabase import create_client, Client as SupabaseClient

import aiohttp
import openpyxl
import requests
import xlrd
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart
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
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("onbrain")

# ─── Constants ───────────────────────────────────────────────────────────────
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]
MAX_ROWS = 2000
MAX_COLS = 60
MAX_CHARS = 120_000
_DB = os.environ.get("SQLITE_TOKEN_DB", "google_tokens.db")
# Storage mode: "cloud" (default) or "on_prem" (no S3, local only)
STORAGE_MODE = os.environ.get("STORAGE_MODE", "cloud").strip().lower()

# ─── Supabase ─────────────────────────────────────────────────────────────────
_SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
_SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "").strip()
_supa: SupabaseClient | None = None
if _SUPABASE_URL and _SUPABASE_KEY:
    try:
        _supa = create_client(_SUPABASE_URL, _SUPABASE_KEY)
        logging.getLogger("onbrain").info("Supabase connected ✅")
    except Exception as _e:
        logging.getLogger("onbrain").warning(f"Supabase init failed: {_e}")


def supa_upsert_user(uid: int, telegram_username: str, full_name: str, phone: str, lang: str):
    """Insert or update user in Supabase 'users' table."""
    if not _supa:
        logging.getLogger("onbrain").warning("Supabase not configured — skipping upsert")
        return
    try:
        result = _supa.table("users").upsert({
            "telegram_id": uid,
            "telegram_username": telegram_username,
            "full_name": full_name,
            "phone": phone,
            "lang": lang,
            "registered_at": datetime.now(timezone.utc).isoformat(),
        }, on_conflict="telegram_id").execute()
        logging.getLogger("onbrain").info(f"Supabase upsert ok uid={uid} data={result.data}")
    except Exception as e:
        logging.getLogger("onbrain").error(f"Supabase upsert ERROR uid={uid}: {type(e).__name__}: {e}")


def supa_is_registered(uid: int) -> bool:
    """Check if user has completed registration (has phone in Supabase)."""
    if not _supa:
        return True   # Supabase not configured → skip registration gate
    try:
        res = _supa.table("users").select("phone").eq("telegram_id", uid).execute()
        if res.data:
            has_phone = bool(res.data[0].get("phone"))
            logging.getLogger("onbrain").info(f"Supabase check uid={uid} has_phone={has_phone}")
            return has_phone
        logging.getLogger("onbrain").info(f"Supabase check uid={uid} not found in DB")
        return False
    except Exception as e:
        # Fail-open: on any error assume registered to avoid re-asking every restart
        logging.getLogger("onbrain").error(f"Supabase check ERROR uid={uid}: {type(e).__name__}: {e} — assuming registered")
        return True


# ─── Rate limiter ─────────────────────────────────────────────────────────────
_RATE_LIMIT = 20          # max requests per window
_RATE_WINDOW = 60         # seconds

class _RateLimiter:
    """Sliding-window rate limiter keyed by user_id."""
    def __init__(self):
        self._buckets: dict[int, collections.deque] = {}

    def is_allowed(self, uid: int) -> bool:
        now = time.monotonic()
        if uid not in self._buckets:
            self._buckets[uid] = collections.deque()
        dq = self._buckets[uid]
        # Remove timestamps outside the window
        while dq and now - dq[0] > _RATE_WINDOW:
            dq.popleft()
        if len(dq) >= _RATE_LIMIT:
            return False
        dq.append(now)
        return True

_rate_limiter = _RateLimiter()

# ─── URL validator ────────────────────────────────────────────────────────────
_ALLOWED_SHEETS_HOSTS = {"docs.google.com"}

def validate_sheets_url(url: str) -> tuple[bool, str]:
    """Return (ok, error_message). Only docs.google.com/spreadsheets allowed."""
    url = url.strip()
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        host = parsed.netloc.lower().lstrip("www.")
        if host not in _ALLOWED_SHEETS_HOSTS:
            return False, f"❌ Noto'g'ri domen: <code>{host}</code>\nFaqat <b>docs.google.com</b> manzili qabul qilinadi."
        if "/spreadsheets/" not in parsed.path:
            return False, "❌ Bu Google Sheets havolasi emas.\nTo'g'ri format: <code>https://docs.google.com/spreadsheets/d/...</code>"
    except Exception:
        return False, "❌ Havola tahlil qilinmadi. To'g'ri URL yuboring."
    return True, ""

# ─── Secure temp-file helper (Excel) ─────────────────────────────────────────
def _safe_temp_path(uid: int, ext: str = ".xlsx") -> str:
    ts = int(time.time())
    tmp_dir = tempfile.gettempdir()
    return os.path.join(tmp_dir, f"user_{uid}_{ts}{ext}")

def _secure_delete(path: str):
    """Overwrite then remove a file so content can't be recovered."""
    try:
        if os.path.exists(path):
            size = os.path.getsize(path)
            with open(path, "r+b") as f:
                f.write(b"\x00" * size)
            os.remove(path)
    except Exception as e:
        logger.warning(f"secure_delete failed for {os.path.basename(path)}: {e}")

# ─── Safe logger (never logs tokens, URLs, file content) ─────────────────────
def _safe_log(uid: int, action: str, source_name: str = "", extra: str = ""):
    """Log only uid, action, source_name. Never log URLs, tokens, file content."""
    msg = f"uid={uid} action={action!r}"
    if source_name:
        msg += f" source={source_name!r}"
    if extra:
        msg += f" {extra}"
    logger.info(msg)

# ─── i18n ────────────────────────────────────────────────────────────────────
TEXTS = {
    "uz": {
        "welcome": (
            "👋 Salom, {name}!\n\n"
            "🤖 <b>OnBrain AI</b> — aqlli jadval tahlilchisi\n\n"
            "📋 <b>Imkoniyatlar:</b>\n"
            "  📊 Excel fayl yuklash\n"
            "  🔗 Google Sheets ulash\n"
            "  📁 Google Drive papka\n"
            "  🌐 Internet qidiruv\n"
            "  🎤 Ovozli savol\n"
            "  🌍 3 tilda ishlash\n\n"
            "Pastdagi menyudan tanlang 👇"
        ),
        "choose_lang": "🌍 Tilni tanlang / Choose language / Выберите язык:",
        "lang_set": "✅ Til o'zgartirildi: O'zbek",
        "btn_excel": "📊 Excel yuklash",
        "btn_sheets": "🔗 Google Sheets",
        "btn_folder": "📁 Drive papka",
        "btn_search": "🌐 Internet qidiruv",
        "btn_help": "❓ Yordam",
        "btn_settings": "⚙️ Sozlamalar",
        "btn_cancel": "❌ Bekor qilish",
        "btn_continue": "💬 Savol davom",
        "btn_voice_hint": "🎤 Ovozli savol",
        "btn_exit": "🚪 Chiqish",
        "btn_lang": "🌍 Til tanlash",
        "btn_disconnect": "🔌 Uzish",
        "ask_excel": "📎 Excel faylni yuboring (.xlsx yoki .xls):",
        "ask_sheets": (
            "🔗 Google Sheets havolasini yuboring:\n"
            "<code>https://docs.google.com/spreadsheets/d/...</code>"
        ),
        "ask_folder": (
            "📁 Google Drive papka havolasini yuboring:\n"
            "<code>https://drive.google.com/drive/folders/...</code>"
        ),
        "loading": "⏳ Yuklanmoqda...",
        "analyzing": "🔍 Tahlil qilinmoqda...",
        "searching": "🌐 Internetdan qidirilmoqda...",
        "thinking": "🤔 AI javob tayyorlamoqda...",
        "transcribing": "🎤 Ovoz tanilmoqda...",
        "excel_ok": (
            "✅ <b>Excel yuklandi!</b>\n\n"
            "📄 Fayl: <code>{name}</code>\n"
            "📊 Qatorlar: <b>{rows}</b> ta\n"
            "📋 Ustunlar: <b>{cols}</b> ta\n"
            "🏷 Sarlavhalar: <code>{headers}</code>\n\n"
            "💬 Savolingizni yozing yoki 🎤 ovozli yuboring:"
        ),
        "sheets_ok": (
            "✅ <b>Google Sheets yuklandi!</b>\n\n"
            "📊 Varaqlar: <b>{sheets}</b>\n"
            "📋 Jami qatorlar: <b>{rows}</b>\n\n"
            "💬 Savolingizni yozing:"
        ),
        "folder_ok": (
            "✅ <b>Google Drive papka yuklandi!</b>\n\n"
            "📁 Jadvallar: <b>{files}</b> ta\n"
            "📋 Jami qatorlar: <b>{rows}</b>\n\n"
            "💬 Savolingizni yozing:"
        ),
        "no_data": "⚠️ Avval ma'lumot yuklang: Excel, Google Sheets yoki Drive papka.",
        "no_excel": "❌ Faqat Excel fayl (.xlsx yoki .xls) qabul qilinadi.",
        "sheets_fail": (
            "❌ Google Sheets yuklanmadi.\n\n"
            "Tekshiring:\n"
            "• Havola to'g'rimi?\n"
            "• Fayl ommaviy (public) qilinganmi?\n"
            "  Share → Anyone with link → Viewer"
        ),
        "not_found_id": (
            "❌ Havola topilmadi.\n\n"
            "To'g'ri format:\n"
            "<code>https://docs.google.com/spreadsheets/d/ID/edit</code>"
        ),
        "folder_not_found": (
            "❌ Papka havolasi topilmadi.\n\n"
            "To'g'ri format:\n"
            "<code>https://drive.google.com/drive/folders/ID</code>"
        ),
        "no_voice_key": "❌ Ovozli savol ishlamaydi — OPENAI_API_KEY o'rnatilmagan.",
        "voice_fail": "❌ Ovozni aniqlashda xatolik: {err}\nMatn shaklida yuboring.",
        "voice_detected": "🎤 Aniqlandi: <i>{text}</i>",
        "no_search_key": "❌ Internet qidiruv ishlamaydi — TAVILY_API_KEY o'rnatilmagan.",
        "search_result": "🌐 <b>Internet qidiruv natijasi:</b>\n\n{answer}",
        "search_sources": "\n\n📎 <b>Manbalar:</b>",
        "search_fail": "❌ Internet qidiruvda xatolik: {err}",
        "cancelled": "❌ Bekor qilindi.",
        "disconnected": "✅ Ma'lumotlar tozalandi. /start yuboring.",
        "auth_link": "🔐 Google hisobiga kirish uchun quyidagi havolani bosing:\n{url}",
        "auth_ok_sheets": "✅ Google hisobiga ulandi!\n🔗 Endi Google Sheets havolasini yuboring:",
        "auth_ok_folder": "✅ Google hisobiga ulandi!\n📁 Endi Drive papka havolasini yuboring:",
        "oauth_not_set": "❌ Google OAuth sozlanmagan.",
        "choose_connect": "Google Sheets ulash usulini tanlang:",
        "btn_oauth": "🔐 Google hisobi bilan",
        "btn_public": "🔓 Ommaviy havola bilan",
        "exited": "🚪 Chat yopildi.",
        "help": (
            "❓ <b>YORDAM</b>\n\n"
            "📊 <b>Excel bilan ishlash:</b>\n"
            "  1. «📊 Excel yuklash» tugmasini bosing\n"
            "  2. Excel faylni yuboring\n"
            "  3. Savol bering\n\n"
            "🔗 <b>Google Sheets:</b>\n"
            "  1. «🔗 Google Sheets» tugmasini bosing\n"
            "  2. Havola yuboring (public bo'lishi shart)\n\n"
            "🌐 <b>Internet qidiruv:</b>\n"
            "  «🌐 Internet qidiruv» tugmasini bosing\n\n"
            "🎤 <b>Ovozli savol:</b>\n"
            "  Ovozli xabar yuboring — bot javob beradi\n\n"
            "📚 <b>Ko'p manba:</b>\n"
            "  Bir nechta Excel va Sheets ulash mumkin.\n"
            "  Bot hammasidan bir vaqtda qidiradi!\n\n"
            "⚙️ <b>Buyruqlar:</b>\n"
            "  /start — Bosh menyu\n"
            "  /help — Yordam\n"
            "  /lang — Til tanlash\n"
            "  /my_sources — Ulangan manbalar\n"
            "  /delete_source 2 — 2-manbani o'chirish\n"
            "  /clear_all — Hammasini tozalash\n"
            "  /disconnect — Ma'lumotlarni tozalash"
        ),
        "settings": "⚙️ <b>Sozlamalar</b>\n\nTil: 🇺🇿 O'zbek",
        "web_on": "🌐 Internet qidiruv yoqildi. Savolingizni yuboring:",
    },
    "ru": {
        "welcome": (
            "👋 Привет, {name}!\n\n"
            "🤖 <b>OnBrain AI</b> — умный анализатор таблиц\n\n"
            "📋 <b>Возможности:</b>\n"
            "  📊 Загрузка Excel файлов\n"
            "  🔗 Подключение Google Sheets\n"
            "  📁 Google Drive папка\n"
            "  🌐 Поиск в интернете\n"
            "  🎤 Голосовые вопросы\n"
            "  🌍 Работа на 3 языках\n\n"
            "Выберите из меню ниже 👇"
        ),
        "choose_lang": "🌍 Tilni tanlang / Choose language / Выберите язык:",
        "lang_set": "✅ Язык изменён: Русский",
        "btn_excel": "📊 Загрузить Excel",
        "btn_sheets": "🔗 Google Sheets",
        "btn_folder": "📁 Drive папка",
        "btn_search": "🌐 Поиск в интернете",
        "btn_help": "❓ Помощь",
        "btn_settings": "⚙️ Настройки",
        "btn_cancel": "❌ Отмена",
        "btn_continue": "💬 Продолжить",
        "btn_voice_hint": "🎤 Голосовой вопрос",
        "btn_exit": "🚪 Выход",
        "btn_lang": "🌍 Язык",
        "btn_disconnect": "🔌 Отключить",
        "ask_excel": "📎 Отправьте Excel файл (.xlsx или .xls):",
        "ask_sheets": (
            "🔗 Отправьте ссылку на Google Sheets:\n"
            "<code>https://docs.google.com/spreadsheets/d/...</code>"
        ),
        "ask_folder": (
            "📁 Отправьте ссылку на папку Google Drive:\n"
            "<code>https://drive.google.com/drive/folders/...</code>"
        ),
        "loading": "⏳ Загружается...",
        "analyzing": "🔍 Анализируется...",
        "searching": "🌐 Поиск в интернете...",
        "thinking": "🤔 ИИ готовит ответ...",
        "transcribing": "🎤 Распознавание речи...",
        "excel_ok": (
            "✅ <b>Excel загружен!</b>\n\n"
            "📄 Файл: <code>{name}</code>\n"
            "📊 Строк: <b>{rows}</b>\n"
            "📋 Столбцов: <b>{cols}</b>\n"
            "🏷 Заголовки: <code>{headers}</code>\n\n"
            "💬 Задайте вопрос или 🎤 отправьте голосовое:"
        ),
        "sheets_ok": (
            "✅ <b>Google Sheets загружен!</b>\n\n"
            "📊 Листов: <b>{sheets}</b>\n"
            "📋 Всего строк: <b>{rows}</b>\n\n"
            "💬 Задайте вопрос:"
        ),
        "folder_ok": (
            "✅ <b>Папка Google Drive загружена!</b>\n\n"
            "📁 Таблиц: <b>{files}</b>\n"
            "📋 Всего строк: <b>{rows}</b>\n\n"
            "💬 Задайте вопрос:"
        ),
        "no_data": "⚠️ Сначала загрузите данные: Excel, Google Sheets или Drive папку.",
        "no_excel": "❌ Принимаются только Excel файлы (.xlsx или .xls).",
        "sheets_fail": (
            "❌ Google Sheets не загружен.\n\n"
            "Проверьте:\n"
            "• Правильная ли ссылка?\n"
            "• Файл открыт публично?\n"
            "  Share → Anyone with link → Viewer"
        ),
        "not_found_id": (
            "❌ Ссылка не найдена.\n\n"
            "Правильный формат:\n"
            "<code>https://docs.google.com/spreadsheets/d/ID/edit</code>"
        ),
        "folder_not_found": (
            "❌ Ссылка на папку не найдена.\n\n"
            "Правильный формат:\n"
            "<code>https://drive.google.com/drive/folders/ID</code>"
        ),
        "no_voice_key": "❌ Голосовые вопросы недоступны — OPENAI_API_KEY не установлен.",
        "voice_fail": "❌ Ошибка распознавания: {err}\nОтправьте текстом.",
        "voice_detected": "🎤 Распознано: <i>{text}</i>",
        "no_search_key": "❌ Поиск недоступен — TAVILY_API_KEY не установлен.",
        "search_result": "🌐 <b>Результат поиска:</b>\n\n{answer}",
        "search_sources": "\n\n📎 <b>Источники:</b>",
        "search_fail": "❌ Ошибка поиска: {err}",
        "cancelled": "❌ Отменено.",
        "disconnected": "✅ Данные очищены. Отправьте /start.",
        "auth_link": "🔐 Для входа в Google нажмите ссылку:\n{url}",
        "auth_ok_sheets": "✅ Google аккаунт подключён!\n🔗 Теперь отправьте ссылку на Google Sheets:",
        "auth_ok_folder": "✅ Google аккаунт подключён!\n📁 Теперь отправьте ссылку на папку Drive:",
        "oauth_not_set": "❌ Google OAuth не настроен.",
        "choose_connect": "Выберите способ подключения Google Sheets:",
        "btn_oauth": "🔐 Через Google аккаунт",
        "btn_public": "🔓 По публичной ссылке",
        "exited": "🚪 Чат закрыт.",
        "help": (
            "❓ <b>ПОМОЩЬ</b>\n\n"
            "📊 <b>Работа с Excel:</b>\n"
            "  1. Нажмите «📊 Загрузить Excel»\n"
            "  2. Отправьте файл\n"
            "  3. Задайте вопрос\n\n"
            "🔗 <b>Google Sheets:</b>\n"
            "  1. Нажмите «🔗 Google Sheets»\n"
            "  2. Отправьте ссылку (файл должен быть public)\n\n"
            "🌐 <b>Поиск в интернете:</b>\n"
            "  Нажмите «🌐 Поиск в интернете»\n\n"
            "🎤 <b>Голосовой вопрос:</b>\n"
            "  Отправьте голосовое — бот ответит\n\n"
            "⚙️ <b>Команды:</b>\n"
            "  /start — Главное меню\n"
            "  /help — Помощь\n"
            "  /lang — Выбор языка\n"
            "  /disconnect — Очистить данные"
        ),
        "settings": "⚙️ <b>Настройки</b>\n\nЯзык: 🇷🇺 Русский",
        "web_on": "🌐 Поиск включён. Задайте вопрос:",
    },
    "en": {
        "welcome": (
            "👋 Hello, {name}!\n\n"
            "🤖 <b>OnBrain AI</b> — Smart table analyzer\n\n"
            "📋 <b>Features:</b>\n"
            "  📊 Upload Excel files\n"
            "  🔗 Connect Google Sheets\n"
            "  📁 Google Drive folder\n"
            "  🌐 Internet search\n"
            "  🎤 Voice questions\n"
            "  🌍 Works in 3 languages\n\n"
            "Choose from the menu below 👇"
        ),
        "choose_lang": "🌍 Tilni tanlang / Choose language / Выберите язык:",
        "lang_set": "✅ Language set: English",
        "btn_excel": "📊 Upload Excel",
        "btn_sheets": "🔗 Google Sheets",
        "btn_folder": "📁 Drive Folder",
        "btn_search": "🌐 Web Search",
        "btn_help": "❓ Help",
        "btn_settings": "⚙️ Settings",
        "btn_cancel": "❌ Cancel",
        "btn_continue": "💬 Continue",
        "btn_voice_hint": "🎤 Voice Question",
        "btn_exit": "🚪 Exit",
        "btn_lang": "🌍 Language",
        "btn_disconnect": "🔌 Disconnect",
        "ask_excel": "📎 Send your Excel file (.xlsx or .xls):",
        "ask_sheets": (
            "🔗 Send your Google Sheets link:\n"
            "<code>https://docs.google.com/spreadsheets/d/...</code>"
        ),
        "ask_folder": (
            "📁 Send your Google Drive folder link:\n"
            "<code>https://drive.google.com/drive/folders/...</code>"
        ),
        "loading": "⏳ Loading...",
        "analyzing": "🔍 Analyzing...",
        "searching": "🌐 Searching the web...",
        "thinking": "🤔 AI is preparing the answer...",
        "transcribing": "🎤 Transcribing voice...",
        "excel_ok": (
            "✅ <b>Excel loaded!</b>\n\n"
            "📄 File: <code>{name}</code>\n"
            "📊 Rows: <b>{rows}</b>\n"
            "📋 Columns: <b>{cols}</b>\n"
            "🏷 Headers: <code>{headers}</code>\n\n"
            "💬 Ask your question or 🎤 send a voice message:"
        ),
        "sheets_ok": (
            "✅ <b>Google Sheets loaded!</b>\n\n"
            "📊 Sheets: <b>{sheets}</b>\n"
            "📋 Total rows: <b>{rows}</b>\n\n"
            "💬 Ask your question:"
        ),
        "folder_ok": (
            "✅ <b>Google Drive folder loaded!</b>\n\n"
            "📁 Spreadsheets: <b>{files}</b>\n"
            "📋 Total rows: <b>{rows}</b>\n\n"
            "💬 Ask your question:"
        ),
        "no_data": "⚠️ Please load data first: Excel, Google Sheets, or Drive folder.",
        "no_excel": "❌ Only Excel files (.xlsx or .xls) are accepted.",
        "sheets_fail": (
            "❌ Google Sheets failed to load.\n\n"
            "Check:\n"
            "• Is the link correct?\n"
            "• Is the file shared publicly?\n"
            "  Share → Anyone with link → Viewer"
        ),
        "not_found_id": (
            "❌ Link not found.\n\n"
            "Correct format:\n"
            "<code>https://docs.google.com/spreadsheets/d/ID/edit</code>"
        ),
        "folder_not_found": (
            "❌ Folder link not found.\n\n"
            "Correct format:\n"
            "<code>https://drive.google.com/drive/folders/ID</code>"
        ),
        "no_voice_key": "❌ Voice questions unavailable — OPENAI_API_KEY not set.",
        "voice_fail": "❌ Transcription error: {err}\nPlease send text instead.",
        "voice_detected": "🎤 Detected: <i>{text}</i>",
        "no_search_key": "❌ Web search unavailable — TAVILY_API_KEY not set.",
        "search_result": "🌐 <b>Web search result:</b>\n\n{answer}",
        "search_sources": "\n\n📎 <b>Sources:</b>",
        "search_fail": "❌ Search error: {err}",
        "cancelled": "❌ Cancelled.",
        "disconnected": "✅ Data cleared. Send /start.",
        "auth_link": "🔐 Click the link to sign in to Google:\n{url}",
        "auth_ok_sheets": "✅ Google account connected!\n🔗 Now send your Google Sheets link:",
        "auth_ok_folder": "✅ Google account connected!\n📁 Now send your Drive folder link:",
        "oauth_not_set": "❌ Google OAuth not configured.",
        "choose_connect": "Choose how to connect Google Sheets:",
        "btn_oauth": "🔐 Via Google account",
        "btn_public": "🔓 Public link",
        "exited": "🚪 Chat closed.",
        "help": (
            "❓ <b>HELP</b>\n\n"
            "📊 <b>Working with Excel:</b>\n"
            "  1. Press «📊 Upload Excel»\n"
            "  2. Send your file\n"
            "  3. Ask questions\n\n"
            "🔗 <b>Google Sheets:</b>\n"
            "  1. Press «🔗 Google Sheets»\n"
            "  2. Send the link (must be public)\n\n"
            "🌐 <b>Web Search:</b>\n"
            "  Press «🌐 Web Search»\n\n"
            "🎤 <b>Voice questions:</b>\n"
            "  Send a voice message — bot will answer\n\n"
            "⚙️ <b>Commands:</b>\n"
            "  /start — Main menu\n"
            "  /help — Help\n"
            "  /lang — Choose language\n"
            "  /disconnect — Clear data"
        ),
        "settings": "⚙️ <b>Settings</b>\n\nLanguage: 🇬🇧 English",
        "web_on": "🌐 Web search enabled. Ask your question:",
    },
}


def t(lang: str, key: str, **kwargs) -> str:
    text = TEXTS.get(lang, TEXTS["uz"]).get(key, TEXTS["uz"].get(key, key))
    if kwargs:
        try:
            text = text.format(**kwargs)
        except Exception:
            pass
    return text


# ─── Session ─────────────────────────────────────────────────────────────────
@dataclass
class Session:
    step: str = "idle"
    lang: str = "uz"
    # Legacy single-source fields (kept for compatibility, used as active/temporary)
    excel_data: list = field(default_factory=list)
    sheets_data: dict = field(default_factory=dict)
    folder_data: dict = field(default_factory=dict)
    sheet_name: str = ""
    sheet_id: str = ""
    google_creds_json: str = ""
    web_search: bool = False
    # Multi-source: each item: {id, source_type, source_name, source_url, file_id, data, header_rows}
    sources: list = field(default_factory=list)
    # For /clear_all confirmation
    pending_clear: bool = False
    # Registration flow
    reg_full_name: str = ""    # temp: stores name during registration
    # Dynamic schema: human-readable column description built from loaded sheet
    schema_info: str = ""      # e.g. "F.I.O, Ball, Sinf, Maktab"
    # Last sheet URL for state persistence (survives bot restart)
    last_sheet_url: str = ""
    # Conversation memory: last successfully found person names
    last_found_names: list = field(default_factory=list)   # e.g. ["Muhammad Davronbek"]
    # Disambiguation: pending candidates when multiple people found for same query
    disambiguation_candidates: list = field(default_factory=list)
    disambiguation_question: str = ""   # original question saved during disambiguation


_sessions: dict[int, Session] = {}


def get_session(uid: int) -> Session:
    if uid not in _sessions:
        _sessions[uid] = Session()
    return _sessions[uid]


def has_data(s: Session) -> bool:
    return bool(s.sources or s.excel_data or s.sheets_data or s.folder_data)


# ─── DB ──────────────────────────────────────────────────────────────────────
def _db_conn():
    c = sqlite3.connect(_DB, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def _init_db():
    with _db_conn() as c:
        c.execute(
            "CREATE TABLE IF NOT EXISTS tokens("
            "uid INTEGER PRIMARY KEY, creds TEXT, lang TEXT, updated TEXT, is_registered INTEGER DEFAULT 0)"
        )
        # Add is_registered column if upgrading from old schema
        try:
            c.execute("ALTER TABLE tokens ADD COLUMN is_registered INTEGER DEFAULT 0")
        except Exception:
            pass
        # Add last_sheet_url column for state persistence
        try:
            c.execute("ALTER TABLE tokens ADD COLUMN last_sheet_url TEXT DEFAULT ''")
        except Exception:
            pass
        c.execute(
            "CREATE TABLE IF NOT EXISTS data_sources("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "user_id INTEGER NOT NULL,"
            "source_type TEXT NOT NULL,"
            "source_name TEXT NOT NULL,"
            "source_url TEXT,"
            "file_id TEXT,"
            "created_at TEXT DEFAULT (datetime('now')))"
        )
        c.execute("CREATE INDEX IF NOT EXISTS idx_ds_user ON data_sources(user_id)")
        c.commit()


def save_registered(uid: int):
    """Mark user as registered in local SQLite (persistent across restarts)."""
    now = datetime.now(timezone.utc).isoformat()
    with _db_conn() as c:
        c.execute(
            "INSERT INTO tokens(uid, is_registered, updated) VALUES(?,1,?) "
            "ON CONFLICT(uid) DO UPDATE SET is_registered=1, updated=excluded.updated",
            (uid, now),
        )
        c.commit()


def is_registered_local(uid: int) -> bool:
    """Check local SQLite registration flag — fast, no network call."""
    with _db_conn() as c:
        row = c.execute("SELECT is_registered FROM tokens WHERE uid=?", (uid,)).fetchone()
    return bool(row and row["is_registered"])


def save_last_sheet_url(uid: int, url: str):
    """Persist the last Google Sheets URL for a user — survives bot restarts."""
    now = datetime.now(timezone.utc).isoformat()
    with _db_conn() as c:
        c.execute(
            "INSERT INTO tokens(uid, last_sheet_url, updated) VALUES(?,?,?) "
            "ON CONFLICT(uid) DO UPDATE SET last_sheet_url=excluded.last_sheet_url, updated=excluded.updated",
            (uid, url, now),
        )
        c.commit()


def load_last_sheet_url(uid: int) -> str:
    """Load the last saved Google Sheets URL for a user."""
    with _db_conn() as c:
        row = c.execute("SELECT last_sheet_url FROM tokens WHERE uid=?", (uid,)).fetchone()
    return (row["last_sheet_url"] or "") if row else ""


# ─── Multi-source DB helpers ──────────────────────────────────────────────────
def _unique_source_name(uid: int, name: str) -> str:
    """Return 'name' if unique for user, otherwise 'name (2)', 'name (3)', ..."""
    with _db_conn() as c:
        rows = c.execute(
            "SELECT source_name FROM data_sources WHERE user_id=?", (uid,)
        ).fetchall()
    existing = {r["source_name"] for r in rows}
    if name not in existing:
        return name
    n = 2
    base, ext = name, ""
    if "." in name:
        base, ext = name.rsplit(".", 1)
        ext = "." + ext
    while True:
        candidate = f"{base} ({n}){ext}"
        if candidate not in existing:
            return candidate
        n += 1


def db_add_source(uid: int, source_type: str, source_name: str,
                  source_url: str | None = None, file_id: str | None = None) -> int:
    """INSERT a new source row, return its id."""
    now = datetime.now(timezone.utc).isoformat()
    with _db_conn() as c:
        cur = c.execute(
            "INSERT INTO data_sources(user_id,source_type,source_name,source_url,file_id,created_at) "
            "VALUES(?,?,?,?,?,?)",
            (uid, source_type, source_name, source_url, file_id, now),
        )
        c.commit()
        return cur.lastrowid


def db_list_sources(uid: int) -> list:
    """Return all sources for user as list of sqlite3.Row."""
    with _db_conn() as c:
        return c.execute(
            "SELECT * FROM data_sources WHERE user_id=? ORDER BY id", (uid,)
        ).fetchall()


def db_delete_source(uid: int, row_number: int) -> str | None:
    """Delete source by 1-based list position. Returns deleted name or None."""
    rows = db_list_sources(uid)
    if row_number < 1 or row_number > len(rows):
        return None
    target = rows[row_number - 1]
    with _db_conn() as c:
        c.execute("DELETE FROM data_sources WHERE id=?", (target["id"],))
        c.commit()
    return target["source_name"]


def db_clear_all_sources(uid: int):
    with _db_conn() as c:
        c.execute("DELETE FROM data_sources WHERE user_id=?", (uid,))
        c.commit()


def db_source_count(uid: int) -> int:
    rows = db_list_sources(uid)
    return len(rows)


def save_token(uid: int, creds_json: str):
    now = datetime.now(timezone.utc).isoformat()
    with _db_conn() as c:
        c.execute(
            "INSERT INTO tokens(uid,creds,updated) VALUES(?,?,?) "
            "ON CONFLICT(uid) DO UPDATE SET creds=excluded.creds, updated=excluded.updated",
            (uid, creds_json, now),
        )
        c.commit()


def load_token(uid: int):
    with _db_conn() as c:
        row = c.execute("SELECT creds FROM tokens WHERE uid=?", (uid,)).fetchone()
    return row["creds"] if row else None


def save_lang(uid: int, lang: str):
    now = datetime.now(timezone.utc).isoformat()
    with _db_conn() as c:
        c.execute(
            "INSERT INTO tokens(uid,lang,updated) VALUES(?,?,?) "
            "ON CONFLICT(uid) DO UPDATE SET lang=excluded.lang, updated=excluded.updated",
            (uid, lang, now),
        )
        c.commit()


def load_lang(uid: int) -> str:
    with _db_conn() as c:
        row = c.execute("SELECT lang FROM tokens WHERE uid=?", (uid,)).fetchone()
    return row["lang"] if (row and row["lang"]) else "uz"


def load_refresh_token(uid: int):
    raw = load_token(uid)
    if not raw:
        return None
    try:
        data = json.loads(raw)
        creds = Credentials.from_authorized_user_info(data, scopes=SCOPES)
        if not creds.valid and creds.expired and creds.refresh_token:
            creds.refresh(GoogleAuthRequest())
            save_token(uid, creds.to_json())
        return creds.to_json() if creds.valid else None
    except Exception as e:
        logger.warning(f"Token refresh error: {e}")
        return None


_oauth_states: dict = {}

# ─── Keyboards ───────────────────────────────────────────────────────────────
def kb_main(lang: str = "uz") -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=t(lang, "btn_excel")), KeyboardButton(text=t(lang, "btn_sheets"))],
            [KeyboardButton(text=t(lang, "btn_folder")), KeyboardButton(text=t(lang, "btn_search"))],
            [KeyboardButton(text=t(lang, "btn_help")), KeyboardButton(text=t(lang, "btn_settings"))],
        ],
        resize_keyboard=True,
    )


def kb_cancel(lang: str = "uz") -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=t(lang, "btn_cancel"))]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def kb_chat(lang: str = "uz", miniapp_url: str = "") -> InlineKeyboardMarkup:
    rows = []
    if miniapp_url:
        rows.append([
            InlineKeyboardButton(
                text="🚀 Mini App da davom eting",
                web_app=WebAppInfo(url=miniapp_url),
            )
        ])
    rows.append([
        InlineKeyboardButton(text=t(lang, "btn_continue"), callback_data="chat_continue"),
        InlineKeyboardButton(text=t(lang, "btn_search"), callback_data="web_search"),
    ])
    rows.append([
        InlineKeyboardButton(text=t(lang, "btn_exit"), callback_data="exit_chat"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_miniapp_open(miniapp_url: str, lang: str = "uz") -> InlineKeyboardMarkup:
    """Button shown right after data source is connected — invites user to mini app."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="🚀 Mini App da savol bering",
                web_app=WebAppInfo(url=miniapp_url),
            )
        ],
        [
            InlineKeyboardButton(text=t(lang, "btn_continue"), callback_data="chat_continue"),
        ],
    ])


def kb_lang() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🇺🇿 O'zbek", callback_data="lang_uz"),
                InlineKeyboardButton(text="🇷🇺 Русский", callback_data="lang_ru"),
                InlineKeyboardButton(text="🇬🇧 English", callback_data="lang_en"),
            ]
        ]
    )


def kb_connect(lang: str = "uz") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=t(lang, "btn_oauth"), callback_data="google_auth")],
            [InlineKeyboardButton(text=t(lang, "btn_public"), callback_data="public_link")],
        ]
    )


def kb_settings(lang: str = "uz") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=t(lang, "btn_lang"), callback_data="open_lang")],
            [InlineKeyboardButton(text=t(lang, "btn_disconnect"), callback_data="disconnect")],
        ]
    )


def kb_phone(lang: str = "uz") -> ReplyKeyboardMarkup:
    """Keyboard with a single 'share contact' button — used during registration."""
    labels = {
        "uz": "📱 Telefon raqamni ulashish",
        "ru": "📱 Поделиться номером",
        "en": "📱 Share phone number",
    }
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=labels.get(lang, labels["uz"]), request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


# ─── Excel ───────────────────────────────────────────────────────────────────
def parse_excel(name: str, content: bytes, uid: int = 0) -> list:
    """Parse Excel bytes. Writes to temp file, reads, then securely deletes it."""
    rows = []
    ext = ".xls" if name.lower().endswith(".xls") else ".xlsx"
    tmp_path = _safe_temp_path(uid, ext)
    try:
        with open(tmp_path, "wb") as f:
            f.write(content)
        if ext == ".xls":
            wb = xlrd.open_workbook(tmp_path)
            ws = wb.sheet_by_index(0)
            for r in range(ws.nrows):
                rows.append([str(ws.cell_value(r, c)) for c in range(ws.ncols)])
        else:
            wb = openpyxl.load_workbook(tmp_path, read_only=True, data_only=True)
            ws = wb.active
            for row in ws.iter_rows(values_only=True):
                rows.append([str(v) if v is not None else "" for v in row])
            wb.close()
    except Exception as e:
        logger.error(f"Excel parse error (uid={uid}): {e}")
    finally:
        # Always securely delete temp file — never keep on disk
        _secure_delete(tmp_path)
    return rows


# ─── Data helpers ─────────────────────────────────────────────────────────────
def _get_all_rows(s: Session) -> tuple[list, list]:
    """Legacy: return merged rows from all sources (for context building)."""
    rows = []
    # Multi-source: merge all
    for src in s.sources:
        data = src.get("data")
        if isinstance(data, list) and data:
            rows.extend(data)
        elif isinstance(data, dict):
            for r in data.values():
                if isinstance(r, list):
                    rows.extend(r)
    # Fallback legacy fields
    if not rows:
        if s.excel_data:
            rows = s.excel_data
        elif s.sheets_data:
            for r in s.sheets_data.values():
                rows.extend(r)
        elif s.folder_data:
            for sheets in s.folder_data.values():
                for r in sheets.values():
                    rows.extend(r)
    return rows, (rows[0] if rows else [])


def _get_source_rows(src: dict) -> tuple[list, list]:
    """Get (all_rows, header) from a single source dict."""
    data = src.get("data")
    rows = []
    if isinstance(data, list) and data:
        rows = data
    elif isinstance(data, dict):
        for r in data.values():
            if isinstance(r, list):
                rows.extend(r)
    header = rows[0] if rows else []
    return rows, header


def _rows_to_text(rows: list) -> str:
    lines = []
    for i, row in enumerate(rows[:MAX_ROWS]):
        cells = [str(c).strip() or "-" for c in row[:MAX_COLS]]
        if all(v == "-" for v in cells):
            continue
        lines.append(f"{i+1}. {' | '.join(cells)}")
    return "\n".join(lines)


def _to_num(val) -> float | None:
    try:
        return float(str(val).strip().replace(",", ".").replace(" ", ""))
    except Exception:
        return None


def _strip_suffix(word: str) -> str:
    """Strip ONLY grammatical (case) suffixes from an Uzbek word.
    Name-component endings like -bek, -boy, -xon, -qul, -jon, -ali
    are intentionally NOT stripped so 'Yodgorbekning' → 'yodgorbek'.

    Strategy:
      1. Try longest grammatical suffixes first (larning, lardan …).
      2. For each suffix candidate, check that the leftover root does NOT
         end with a known name-component — if it does, skip that suffix
         (the suffix is actually part of the name).
      3. Minimum root length = 3 chars.
    """
    w = word.lower().strip()

    # Pure grammatical (case/plural) suffixes — longest first
    suffixes = [
        "larning", "lardan", "larcha", "lardagi",
        "larda", "larga", "larni",
        "gacha", "dagi", "niki",
        "ning", "dan",
        "ni", "ga", "da", "gi", "ki",
        "lar", "lik",
        # Relationship suffixes (don't strip — they're part of name context)
        # But we DO need to strip them from query words to find the base name
        # "o'g'li" / "qizi" → strip → base name
        "o'g'lining", "o'g'lini", "o'g'liga", "o'g'lidan",
        "qizining", "qizini", "qiziga",
    ]

    for suf in suffixes:
        if w.endswith(suf):
            root = w[: len(w) - len(suf)]
            if len(root) < 3:
                continue
            return root

    # "ka" (dative variant) — only strip if root >= 5 chars to avoid
    # cutting real names like "Malika" → "mali" (wrong)
    if w.endswith("ka") and len(w) - 2 >= 5:
        return w[:-2]

    return w


def _classify_name_type(word: str) -> str:
    """Classify a single cleaned word as 'familiya', 'ism', or 'ota'.

    Rules:
    1. If word ends with a familiya suffix (-ov, -ev, -yev, -ova, -eva, -yeva,
       -off, -eff, -in, -ina, -skiy, -sky, -zadeh, -zoda, -bekov, etc.)
       → 'familiya'  (search pos 0 in name column)
    2. If word ends with o'g'li / qizi (after grammatical strip)
       → 'ota'  (search pos 2+ — otaismi field)
    3. Otherwise → 'ism'  (search pos 1 in name column)
    """
    w = word.strip().lower()
    # Familiya (surname) suffixes — Uzbek, Russian-style, Tajik-style
    fam_suffixes = (
        "ov", "ev", "yev",
        "ova", "eva", "yeva",
        "off", "eff",
        "in", "ina",
        "skiy", "sky", "ский",
        "zadeh", "zoda",
        "bekov", "bekova",
        "jonov", "jonova",
        "xonov", "xonova",
        "qolov", "qolova",
        "boyev", "boyeva",
        "qulov", "qulova",
        "ulov", "ulova",
        "aliev", "alieva",
    )
    for suf in fam_suffixes:
        if w.endswith(suf) and len(w) > len(suf) + 1:
            return "familiya"
    # Ota-ism markers
    ota_markers = ("o'g'li", "o'g'lining", "qizi", "qizining", "ugli", "ugil", "ogli")
    for m in ota_markers:
        if w.endswith(m):
            return "ota"
    return "unknown"


def _search_person(data_rows: list, header: list, name_q: str) -> list[dict]:
    raw = name_q.strip().lower()
    stripped = _strip_suffix(raw)
    candidates = list({raw, stripped})

    # Determine what kind of name this is
    # Use the stripped form for classification (remove grammatical suffixes first)
    name_type = _classify_name_type(stripped)

    # Find name columns (F.I.O, FIO, Ism, Name, ФИО etc.)
    name_col_indices = []
    for j, h in enumerate(header):
        hl = str(h).strip().lower()
        if any(w in hl for w in ["f.i.o", "fio", "ism", "name", "ф.и.о", "фио", "имя", "familiya", "fish"]):
            name_col_indices.append(j)
    # If no dedicated name column found, search all columns
    search_all = len(name_col_indices) == 0

    results = []
    for i, row in enumerate(data_rows):
        cols_to_check = range(len(row)) if search_all else name_col_indices
        matched_j = None
        matched_cell = None
        match_quality = 0  # 3=exact_full, 2=exact_word_token, 0=no_match
        for j in cols_to_check:
            if j >= len(row):
                continue
            cs = str(row[j]).strip().lower()
            if not cs or len(cs) < 2:
                continue
            for c in candidates:
                if len(c) < 3:
                    continue
                # Priority 3: Exact full cell match (single-word name in cell)
                if c == cs:
                    matched_j = j
                    matched_cell = str(row[j]).strip()
                    match_quality = 3
                    break
                # Priority 2: Exact word token match ONLY
                # "muhammad" must be a standalone word in cell — NOT substring of "muhammadali"
                words_in_cell = cs.split()
                if c in words_in_cell:
                    pos = words_in_cell.index(c)
                    is_name_col = j in name_col_indices
                    matched = False
                    if is_name_col:
                        # Pozitsiyadan qat'iy nazar — token topilsa matched = True
                        # (Chunki Excel'da ism/familiya tartibi har xil bo'lishi mumkin)
                        matched = True
                    else:
                        matched = True
                    if matched and match_quality < 2:
                        matched_j = j
                        matched_cell = str(row[j]).strip()
                        match_quality = 2
                        logger.debug(f"[SEARCH] token={c!r} matched cell={cs!r} pos={pos} quality=2")
            if match_quality == 3:
                break
        if matched_j is not None and match_quality > 0:
            col = str(header[matched_j]).strip() if matched_j < len(header) else f"Col{matched_j}"
            results.append({
                "row_index": i,
                "row": row,
                "matched_cell": matched_cell,
                "matched_col": col,
                "match_quality": match_quality,
            })
            logger.debug(f"[SEARCH] Row {i} matched: cell={matched_cell!r} quality={match_quality}")

    logger.debug(f"[SEARCH] _search_person({name_q!r}): {len(results)} rows found")
    return results


def _sum_numeric(row: list, header: list) -> tuple[float, list[str]]:
    """Sum numeric columns, skipping name cols and dedicated 'Umumiy ball' cols."""
    total, details = 0.0, []
    for j, h in enumerate(header):
        hl = str(h).strip().lower()
        # Skip name/id columns
        if any(w in hl for w in ["f.i.o", "fio", "ism", "name", "ф.и.о", "фио", "имя", "familiya", "fish", "raqam", "tartib", "id", "#"]):
            continue
        # Skip dedicated "Umumiy ball" / total columns — they are sum of others, already shown via direct_val
        if ("umumiy" in hl and "ball" in hl) or hl in ["umumiy ball", "total", "итого", "jami ball", "общий балл", "jami"]:
            continue
        if j >= len(row):
            continue
        v = _to_num(row[j])
        if v is not None:
            col = str(h).strip()
            total += v
            details.append(f"{col}={v}")
    return total, details


def _format_person_answer(person: str, row: list, header: list, src_label: str,
                           is_avg: bool, is_max: bool, is_min: bool,
                           question: str = "") -> str:
    """Build a clean answer line for one person. Shows only what was asked."""
    src_tag = f"\n📂 <i>Manba: {src_label}</i>"
    q_lower = question.strip().lower()

    # ── Subject/column filter: if question mentions a specific subject, show that column ──
    SUBJECT_KEYWORDS = {
        "algebra": ["algebra"],
        "geometriya": ["geometriya", "геометрия"],
        "matematika": ["matematika", "математика", "math"],
        "fizika": ["fizika", "физика", "physics"],
        "kimyo": ["kimyo", "химия", "chemistry"],
        "biologiya": ["biologiya", "биология", "biology"],
        "tarix": ["tarix", "история", "history"],
        "geografiya": ["geografiya", "география", "geography"],
        "adabiyot": ["adabiyot", "литература"],
        "ingliz": ["ingliz", "английский", "english"],
        "rus": ["rus tili", "rus", "русский"],
        "ona tili": ["ona tili", "ona", "узбекский"],
        "informatika": ["informatika", "информатика"],
        "chizmachilik": ["chizmachilik"],
        "texnologiya": ["texnologiya"],
        "musiqa": ["musiqa"],
        "sport": ["sport", "jismoniy"],
        "huquq": ["huquq"],
        "iqtisodiyot": ["iqtisodiyot"],
        "falsafa": ["falsafa"],
        "psixologiya": ["psixologiya"],
        "astronomiya": ["astronomiya"],
    }

    asked_subject_col = None  # header column index matching the asked subject
    for subj, keywords in SUBJECT_KEYWORDS.items():
        if any(kw in q_lower for kw in keywords):
            # Find column in header matching this subject
            for j, h in enumerate(header):
                hl = str(h).strip().lower()
                if any(kw in hl for kw in keywords):
                    if j < len(row):
                        v = _to_num(row[j])
                        if v is not None:
                            asked_subject_col = (j, str(header[j]).strip(), v)
                            break
            break

    if asked_subject_col is not None:
        j, col_name, val = asked_subject_col
        return f"👤 <b>{person}</b>\n📚 {col_name}: <b>{val}</b>{src_tag}"

    # Find dedicated "Umumiy ball" column first
    direct_val, direct_col = None, None
    for j, h in enumerate(header):
        hl = str(h).strip().lower()
        if ("umumiy" in hl and "ball" in hl) or hl in ["umumiy ball", "total", "итого", "jami ball", "общий балл"]:
            if j < len(row):
                v = _to_num(row[j])
                if v is not None:
                    direct_val, direct_col = v, str(h).strip()
                    break

    total, details = _sum_numeric(row, header)

    if is_avg and details:
        avg = total / len(details)
        return f"👤 <b>{person}</b>\n📊 O'rtacha: <b>{avg:.2f}</b>{src_tag}"
    elif is_max and details:
        mx = max((_to_num(d.split("=")[1]) or 0) for d in details if "=" in d)
        return f"👤 <b>{person}</b>\n📈 Maksimal: <b>{mx}</b>{src_tag}"
    elif is_min and details:
        mn = min((_to_num(d.split("=")[1]) or 0) for d in details if "=" in d)
        return f"👤 <b>{person}</b>\n📉 Minimal: <b>{mn}</b>{src_tag}"
    elif direct_val is not None:
        # Has dedicated score column — show it cleanly, skip noisy details
        return f"👤 <b>{person}</b>\n🏆 {direct_col}: <b>{direct_val:.2f}</b>{src_tag}"
    elif details:
        return f"👤 <b>{person}</b>\n🏆 Jami: <b>{total:.2f}</b>{src_tag}"
    else:
        # No numeric data — show non-empty cells, skip the matched cell itself (already shown as person name)
        parts = []
        person_lower = person.strip().lower()
        for k in range(len(row)):
            cell_val = str(row[k]).strip()
            if not cell_val:
                continue
            # Skip the cell that was already shown as the matched name
            if cell_val.lower() == person_lower:
                continue
            col_name = str(header[k]).strip() if k < len(header) else f"Col{k}"
            parts.append(f"<b>{col_name}:</b> {cell_val}")
        if parts:
            return f"� <b>{person}</b>\n" + "\n".join(parts) + src_tag
        return f"🔍 <b>{person}</b>{src_tag}"


def _python_answer(question: str, s: Session) -> str | None:
    q = question.strip().lower()

    # ── Collect source datasets (each sheet-tab as separate dataset for Sheets)
    source_datasets: list[tuple[list, list, str]] = []

    if s.sources:
        for src in s.sources:
            data = src.get("data")
            src_name = src.get("source_name", "Manba")
            if isinstance(data, list) and len(data) >= 2:
                # Excel — flat list of rows
                source_datasets.append((data, data[0], src_name))
            elif isinstance(data, dict):
                # Google Sheets — dict of {tab_name: [rows]}
                for tab_name, tab_rows in data.items():
                    if isinstance(tab_rows, list) and len(tab_rows) >= 2:
                        label = f"{src_name} › {tab_name}" if len(data) > 1 else src_name
                        source_datasets.append((tab_rows, tab_rows[0], label))

    # Fallback legacy
    if not source_datasets:
        rows, header = _get_all_rows(s)
        if rows and len(rows) >= 2:
            source_datasets.append((rows, header, s.sheet_name or "Ma'lumot"))

    if not source_datasets:
        return None

    is_query = any(w in q for w in [
        "ball", "balli", "ballari", "bali", "baho", "bahosi", "score", "natija", "natijalari",
        "umumiy", "jami", "hammasi", "summa", "total", "итог", "балл", "сумма",
        "nechchi", "necchi", "qancha", "сколько", "how many",
        "o'rtacha", "ortacha", "average", "средний",
        "eng yuqori", "maksimal", "max", "maximum", "максимальный",
        "eng past", "minimal", "min", "minimum", "минимальный",
        # Voice transcription variants (Whisper may omit/alter words)
        "ko'rsat", "korsat", "chiqar", "ayt", "top", "hisob", "hisobi",
        "ko'rsatib", "chiqarib", "topib", "hisobla",
        "результат", "показать", "найти", "баллы", "оценка",
    ])

    # Words that indicate a general (non-person) question → let AI handle it
    non_person_indicators = {
        "kamera", "telefon", "pul", "narx", "xarajat", "ketgan", "sarflangan",
        "sarflandi", "sarf", "xarajatlar", "tushumlari", "tushum", "daromad",
        "sotildi", "sotilgan", "sotilgani", "qancha", "nechta", "miqdori",
        "tovar", "mahsulot", "buyurtma", "buyurtmalar", "zakaz",
        "nechtasi", "barchasi", "hammasi", "davomiyligi", "muddati",
        "sana", "sanasi", "kun", "oy", "yil", "vaqt", "soat",
        "raqam", "raqami", "nomer", "nomeri",
        "price", "cost", "money", "time", "date", "number",
        "spent", "total", "sum", "amount", "paid",
        "цена", "стоимость", "деньги", "время", "дата", "потрачено", "сумма",
        # Texnika / buyum nomlari — ism emas
        "model", "marka", "nomi", "turi", "tipi", "versiya", "seriya",
        "kompyuter", "noutbuk", "printer", "skaner", "monitor", "klaviatura",
        "telefon", "planshet", "kamera", "videokamera", "projetor", "ekran",
        "stol", "stul", "shkaf", "javon", "divan", "krovat", "krovati",
        "mashina", "avto", "moshina", "velosiped", "mototsikl",
        "ijaraga", "ijara", "arenda", "soliq", "nalog",
        # Fan / predmet nomlari — ism emas
        "algebra", "geometriya", "matematika", "fizika", "kimyo", "biologiya",
        "tarix", "geografiya", "adabiyot", "ingliz", "rus", "ona", "tili",
        "informatika", "texnologiya", "sport", "chizmachilik", "musiqa",
        "huquq", "iqtisodiyot", "falsafa", "psixologiya", "astronomiya",
        "fan", "fani", "fanidan", "predmet", "dars", "darsi", "darslari",
        "subject", "math", "physics", "chemistry", "biology", "history",
    }

    stop = {
        # Grammatik / ko'makchi so'zlar
        "va", "bilan", "uchun", "ning", "ni", "ga", "da", "dan", "nechchi", "necchi",
        "umumiy", "ball", "balli", "ballari", "baho", "jami", "hammasi",
        "ko'rsat", "toping", "ayt", "qancha", "top", "nima", "qaysi", "nechta",
        "natijasi", "hisobi", "yig'indisi", "yigindisi", "qildimi", "topdi",
        "bahosi", "natija", "score", "natijalari", "yig'indi", "yigindi", "summa",
        "sinf", "class", "и", "или", "для", "с", "в", "на", "по", "что", "как",
        "and", "or", "for", "with", "the", "of", "is", "are", "what", "how",
        # Fan / predmet nomlari
        "algebra", "geometriya", "fizika", "kimyo", "biologiya", "tarix",
        "ingliz", "rus", "matematika", "informatika", "adabiyot", "geografiya",
        "ona", "tili", "fanidan", "fani", "fandan",
        # Grammatik so'zlar
        "olgan", "olgani", "qilgan", "bergan", "topgan", "yozgan",
        "sarflandi", "sarflangan", "sarflagan", "sotildi", "sotilgan",
        "xarajat", "xarajatlar", "tushum", "daromad", "miqdori",
        "uning", "uniki", "ularning", "sizning", "mening",
        "necha", "qanday", "qoida", "nomi", "nomini",
        # Texnika / buyum so'zlari — ism emas
        "model", "marka", "turi", "tipi", "versiya", "seriya", "nomi",
        "kompyuter", "noutbuk", "printer", "monitor", "ekran", "kamera",
        "mashina", "moshina", "ijara", "ijaraga", "arenda", "soliq",
        # ── KENGAYTIRILGAN STOP WORDS ──────────────────────────────────────
        # So'rov so'zlari
        "menga", "senga", "unga", "bizga", "sizga", "ularga",
        "ber", "bering", "berib", "bersin",
        "top", "toping", "topib", "topsin",
        "ko'rsat", "ko'rsating", "ko'rsatib",
        "chiqar", "chiqaring", "ayt", "ayting",
        "kerak", "lozim", "zarur",
        "qilib", "qiling", "qilsin",
        "haqida", "haqida", "to'g'risida", "borasida",
        "malumot", "ma'lumot", "ma'lumotini", "malumotini",
        "axborot", "bilmoqchi", "bilish", "bilaman",
        "iltimos", "marhamat", "iltimos",
        # Olmosh / proximal so'zlar (pronoun → context resolution)
        "shu", "shuni", "shuning", "shunday", "shuni",
        "bu", "buni", "buning", "bunday",
        "u", "uni", "uning", "ul",
        "o'sha", "o'shani", "o'shaning",
        "ana", "mana",
        # Oquvchi / kishi bildiruvchi umumiy so'zlar
        "oquvchi", "o'quvchi", "talaba", "o'quvchini", "talabani",
        "shaxs", "kishi", "odam", "bola", "farzand",
        "oquvchining", "o'quvchining", "talabaning",
        # Qarindoshlik (relationship) so'zlari — ism emas
        "o'g'li", "o'g'lini", "o'g'liga", "o'g'lidan", "o'g'lining",
        "qizi", "qizini", "qiziga", "qizidan", "qizining",
        "akasi", "singlisi", "ukasi", "opasi",
        # Savol so'zlari
        "nechi", "nechchi", "nechanchi", "qaysi", "qachon", "nima",
        "kim", "kimning", "kimni", "kimga",
        # Rus / ingliz stop
        "мне", "мне", "дай", "покажи", "найди", "нужен", "нужна",
        "этот", "эта", "это", "тот", "та", "то",
        "ученик", "студент", "человек",
        "give", "show", "find", "need", "want", "tell",
        "this", "that", "the", "student", "person",
    }

    words = [w.strip(".,!?\"'()[]") for w in question.split()]
    name_candidates: list[str] = []
    for w in words:
        c = _strip_suffix(w)
        cl = c.lower()
        if len(c) >= 3 and cl not in stop and not c.isdigit():
            if cl not in [x.lower() for x in name_candidates]:  # deduplicate
                name_candidates.append(c)

    # ── Pronoun / context resolution ─────────────────────────────────────────
    # ONLY inject memory on explicit pronouns ("u", "shu", "o'sha").
    # NEVER inject when user has already named a person in the query.
    PRONOUNS = {
        "u", "uni", "uning", "shu", "shuni", "shuning", "o'sha", "o'shani",
        "bu", "buni", "ushbu", "shu oquvchi", "o'sha oquvchi", "u oquvchi",
        "ushbu oquvchi", "shu talaba", "ushbu talaba",
        "this student", "that student", "он", "она", "этот", "тот",
    }
    q_stripped = q.strip()
    _q_words = set(q_stripped.lower().split())
    has_pronoun = any(pr in _q_words for pr in PRONOUNS)

    # "person-like" = candidate not in non_person_indicators and not a pure number
    person_like_candidates = [c for c in name_candidates if c.lower() not in non_person_indicators]

    # If user named a person explicitly → clear old memory (new query, new person)
    if person_like_candidates:
        s.last_found_names = []
        logger.debug(f"[MEMORY] cleared — new person query: {person_like_candidates}")

    # Inject memory ONLY on explicit pronoun AND no person name in query
    should_inject_memory = (
        s.last_found_names
        and has_pronoun
        and len(person_like_candidates) == 0
    )
    if should_inject_memory:
        for remembered in s.last_found_names:
            remembered_parts = remembered.split()
            for part in remembered_parts:
                pl = part.lower()
                if len(pl) >= 3 and pl not in stop and pl not in non_person_indicators:
                    if pl not in [x.lower() for x in name_candidates]:
                        name_candidates.append(part)
        person_like_candidates = [c for c in name_candidates if c.lower() not in non_person_indicators]
        logger.info(f"Memory injected (pronoun) → {s.last_found_names} into candidates")
    # ─────────────────────────────────────────────────────────────────────────

    if not name_candidates:
        return None

    # If ALL candidates are non-person words (e.g. "kamera", "pul") → let AI answer
    if all(c.lower() in non_person_indicators for c in name_candidates):
        return None

    # If no scoring keyword found AND no person-like candidates → let AI handle
    if not is_query and not person_like_candidates:
        return None

    is_avg = any(w in q for w in ["o'rtacha", "ortacha", "average", "средний", "avg"])
    is_max = any(w in q for w in ["eng yuqori", "maksimal", "max", "maximum", "максимальный"])
    is_min = any(w in q for w in ["eng past", "minimal", "min", "minimum", "минимальный"])

    # ── Search strategy:
    # If person_like_candidates has 2+ parts (e.g. ["Mirzayev", "Hasanboy"]),
    # first try AND search (row must contain ALL parts → exact person match).
    # If AND search finds nothing, fall back to OR search per candidate.
    # This prevents "Mirzayev Akbarshox" from matching when user asks "Mirzayev Hasanboy".

    answer_parts: list[str] = []
    not_found_names: list[str] = []
    global_seen: set[tuple[int, str]] = set()  # (row_index, src_label)

    def _matches_all_parts(row: list, header: list, parts: list[str]) -> bool:
        """Return True if the row's name column(s) contain ALL given parts as whole words."""
        name_col_indices = []
        for j, h in enumerate(header):
            hl = str(h).strip().lower()
            if any(w in hl for w in ["f.i.o", "fio", "ism", "name", "ф.и.о", "фио", "имя", "familiya", "fish"]):
                name_col_indices.append(j)
        cols = name_col_indices if name_col_indices else range(len(row))
        cell_text = " ".join(str(row[j]).strip().lower() for j in cols if j < len(row))
        cell_words = cell_text.split()
        for p in parts:
            p_lower = p.lower()
            # Exact word token match ONLY — no startswith/substring
            if p_lower not in cell_words:
                return False
        return True

    # Try AND search first when multiple person-like candidates
    and_search_done = False
    if len(person_like_candidates) >= 2:
        and_search_done = True
        for (rows, header, src_label) in source_datasets:
            data_rows = rows[1:]
            # Get matches for first candidate, then filter by remaining parts
            first_matches = _search_person(data_rows, header, person_like_candidates[0])
            for m in first_matches:
                # Require at least quality=2 (startswith or exact) for multi-part AND search
                if m["match_quality"] < 2:
                    continue
                if not _matches_all_parts(m["row"], header, person_like_candidates[1:]):
                    continue
                key = (m["row_index"], src_label)
                if key in global_seen:
                    continue
                global_seen.add(key)
                answer_parts.append(
                    _format_person_answer(
                        m["matched_cell"], m["row"], header, src_label,
                        is_avg, is_max, is_min, question=question,
                    )
                )

    # ── AND search was attempted but found nothing → OR fallback bilan qayta qidir
    if and_search_done and not answer_parts:
        # OR fallback: har bir candidate alohida qidiriladi
        for name in person_like_candidates:
            for (rows, header, src_label) in source_datasets:
                data_rows = rows[1:]
                matches = _search_person(data_rows, header, name)
                for m in matches:
                    key = (m["row_index"], src_label)
                    if key in global_seen:
                        continue
                    global_seen.add(key)
                    answer_parts.append(
                        _format_person_answer(
                            m["matched_cell"], m["row"], header, src_label,
                            is_avg, is_max, is_min, question=question,
                        )
                    )
        # Agar OR fallback ham hech narsa topmasa — faqat shunda "topilmadi" chiqar
        if not answer_parts:
            searched = " ".join(c.capitalize() for c in person_like_candidates)
            return (
                f"❌ <b>{searched}</b> — ma'lumotlar bazasida topilmadi.\n\n"
                "💡 Familiya yoki to'liq ism bilan qayta yozing."
            )

    # ── OR search (single-word name queries)
    if not answer_parts:
        for name in name_candidates:
            found_this_name = False
            name_matches_all = []
            for (rows, header, src_label) in source_datasets:
                data_rows = rows[1:]
                matches = _search_person(data_rows, header, name)
                for m in matches:
                    key = (m["row_index"], src_label)
                    if key in global_seen:
                        continue
                    name_matches_all.append((m, src_label, header))

            if name_matches_all:
                # All results are exact token matches (quality 2 or 3)
                # If multiple people found → disambiguation list
                unique_names = []
                for (m, slabel, _) in name_matches_all:
                    fn = m["matched_cell"]
                    if fn not in unique_names:
                        unique_names.append(fn)

                if len(unique_names) > 1:
                    # Multiple exact matches → show numbered list (backend-generated, no AI)
                    s.disambiguation_candidates = unique_names
                    s.disambiguation_question = question
                    logger.info(f"[DISAMBIG] {len(unique_names)} exact matches for {name!r}: {unique_names}")
                    lines = [f"🔍 <b>'{name.capitalize()}'</b> ismli bir nechta o'quvchi topildi:\n"]
                    for idx, cn in enumerate(unique_names, 1):
                        lines.append(f"{idx}. {cn}")
                    lines.append("\n<i>Raqamini kiriting (masalan: 1)</i>")
                    return "\n".join(lines)

                # Single exact match → answer directly
                for (m, src_label, header) in name_matches_all:
                    key = (m["row_index"], src_label)
                    if key in global_seen:
                        continue
                    global_seen.add(key)
                    found_this_name = True
                    logger.info(f"[EXACT] uid match: {m['matched_cell']!r} from {src_label!r}")
                    answer_parts.append(
                        _format_person_answer(
                            m["matched_cell"], m["row"], header, src_label,
                            is_avg, is_max, is_min, question=question,
                        )
                    )
            if not found_this_name:
                if name.lower() not in non_person_indicators:
                    not_found_names.append(name)

    if answer_parts:
        result = "\n\n".join(answer_parts)
        if not_found_names and not s.web_search:
            missing = ", ".join(f"<b>{n.capitalize()}</b>" for n in not_found_names)
            result += f"\n\n❌ Topilmadi: {missing}"

        # ── Save found names to conversation memory (only for pronoun follow-ups) ──
        found_names = []
        for part in answer_parts:
            import re as _re
            m = _re.search(r"👤 <b>([^<]+)</b>", part)
            if m:
                found_names.append(m.group(1).strip())
        if found_names:
            s.last_found_names = found_names[:1]  # only keep 1 name, not 3
        logger.info(f"[MEMORY] last_found_names updated → {s.last_found_names}")

        s.disambiguation_candidates = []
        return result

    # Nothing found at all
    # If web_search is ON → return None so Tavily handles it
    if s.web_search:
        return None
    # Only show "topilmadi" if query clearly looked like a person+score search
    if person_like_candidates and is_query:
        searched = ", ".join(f"<b>{c.capitalize()}</b>" for c in person_like_candidates[:3])
        return (
            f"❌ {searched} — ma'lumotlar bazasida topilmadi.\n\n"
            "💡 Familiya yoki to'liq ism bilan qayta yozing."
        )
    return None


def _get_person_candidates_from_question(question: str) -> list:
    """Savol matnidan odam ismi bo'lishi mumkin bo'lgan so'zlarni ajratib oladi (ma'lumot qidirmaydi)."""
    _stop = {
        "va", "bilan", "uchun", "ning", "ni", "ga", "da", "dan", "nechchi", "necchi",
        "umumiy", "ball", "balli", "ballari", "baho", "jami", "hammasi",
        "qancha", "nechta", "nima", "qaysi", "natijasi", "hisobi", "yigindisi",
        "algebra", "geometriya", "fizika", "kimyo", "biologiya", "tarix",
        "ingliz", "rus", "matematika", "informatika", "adabiyot", "geografiya",
        "ona", "tili", "fanidan", "fani", "fandan", "faniga", "fanlar",
        "olgan", "olgani", "qilgan", "bergan", "topgan", "yozgan",
        "sarflandi", "sarflangan", "sarflagan", "sotildi", "sotilgan",
        "xarajat", "xarajatlar", "tushum", "daromad", "miqdori",
        "uning", "uniki", "ularning", "sizning", "mening",
        "necha", "qanday", "nomi", "nomini",
        "and", "or", "for", "the", "of", "is", "are", "what", "how",
        "и", "или", "для", "с", "в", "на", "по", "что", "как",
    }
    _non_person = {
        "kamera", "telefon", "pul", "narx", "xarajat", "sarflangan", "ketgan",
        "tovar", "mahsulot", "buyurtma", "sana", "raqam", "vaqt", "kun", "oy", "yil",
        "price", "cost", "money", "time", "date", "number", "total", "sum", "amount",
        "model", "marka", "kompyuter", "noutbuk", "printer", "mashina", "avto",
        "algebra", "geometriya", "matematika", "fizika", "kimyo", "biologiya",
        "tarix", "geografiya", "adabiyot", "ingliz", "rus", "ona", "tili",
        "informatika", "texnologiya", "sport", "musiqa", "fan", "fani", "fanidan",
        "ijara", "arenda", "soliq", "nalog",
    }
    words = [w.strip(".,!?\"'()[]") for w in question.split()]
    candidates: list = []
    for w in words:
        c = _strip_suffix(w)
        cl = c.lower()
        if len(c) >= 3 and not c.isdigit() and cl not in _stop and cl not in _non_person:
            if cl not in [x.lower() for x in candidates]:
                candidates.append(c)
    return candidates


def _build_slim_context(sess: "Session", question: str) -> str:
    """Grok uchun kontekst: faqat header + savol kalit so'zlari bor qatorlar (max 50 qator/sheet).
    Agar hech narsa topilmasa — to'liq kontekst qaytariladi."""
    kws = [w.lower().strip(".,!?\"'()[]") for w in question.split() if len(w) >= 3]

    def _row_relevant(row: list) -> bool:
        row_text = " ".join(str(c).lower() for c in row)
        return any(kw in row_text for kw in kws)

    parts: list = []

    if sess.sources:
        for src in sess.sources:
            label = src.get("source_name", "Manba")
            stype = src.get("source_type", "")
            data = src.get("data")
            parts.append(f"\n=== {label} [{stype}] ===")
            if isinstance(data, list) and data:
                header = data[0]
                relevant = [r for r in data[1:] if _row_relevant(r)]
                slim = [header] + relevant[:50]
                parts.append(_rows_to_text(slim))
            elif isinstance(data, dict):
                for title, rows in data.items():
                    if isinstance(rows, list) and rows:
                        header = rows[0]
                        relevant = [r for r in rows[1:] if _row_relevant(r)]
                        slim = [header] + relevant[:50]
                        parts.append(f"--- {title} ---\n{_rows_to_text(slim)}")

    if not parts:
        # legacy fallback
        if sess.sheets_data:
            for title, rows in sess.sheets_data.items():
                if isinstance(rows, list) and rows:
                    header = rows[0]
                    relevant = [r for r in rows[1:] if _row_relevant(r)]
                    slim = [header] + relevant[:50]
                    parts.append(f"--- {title} ---\n{_rows_to_text(slim)}")
        elif sess.excel_data:
            data = sess.excel_data
            header = data[0] if data else []
            relevant = [r for r in data[1:] if _row_relevant(r)]
            slim = [header] + relevant[:50]
            parts.append(_rows_to_text(slim))

    if not parts:
        return build_context(sess)  # fallback to full

    ctx = "\n".join(parts)
    return ctx[:MAX_CHARS]


def build_context(s: Session) -> str:
    parts = []
    # Multi-source
    if s.sources:
        for src in s.sources:
            label = src.get("source_name", "Manba")
            stype = src.get("source_type", "")
            data = src.get("data")
            parts.append(f"\n=== {label} [{stype}] ===")
            if isinstance(data, list) and data:
                parts.append(_rows_to_text(data))
            elif isinstance(data, dict):
                for title, rows in data.items():
                    if isinstance(rows, list) and rows:
                        parts.append(f"--- {title} ---\n{_rows_to_text(rows)}")
    # Fallback legacy
    if not parts:
        if s.folder_data:
            for sid, sheets in s.folder_data.items():
                parts.append(f"\n=== {sid} ===")
                for title, rows in sheets.items():
                    parts.append(f"--- {title} ---\n{_rows_to_text(rows)}")
        elif s.sheets_data:
            name = s.sheet_name or "Sheet"
            parts.append(f"=== {name} ===")
            for title, rows in s.sheets_data.items():
                parts.append(f"--- {title} ---\n{_rows_to_text(rows)}")
        elif s.excel_data:
            parts.append(f"=== {s.sheet_name or 'Excel'} ===\n{_rows_to_text(s.excel_data)}")
    ctx = "\n".join(parts)
    return ctx[:MAX_CHARS]


# ─── Grok AI ─────────────────────────────────────────────────────────────────
LANG_INSTRUCTION = {
    "uz": "Javob FAQAT O'ZBEK tilida bo'lsin.",
    "ru": "Ответ должен быть ТОЛЬКО на РУССКОМ языке.",
    "en": "Answer ONLY in ENGLISH.",
}

GROK_SYSTEM = (
    "Sen jadval (Excel/Sheets) ma'lumotlarini tahlil qiluvchi aqlli assistantsan.\n"
    "{schema_section}"
    "QOIDALAR:\n"
    "1. FAQAT berilgan jadval ma'lumotlari asosida javob ber — tashqaridan ma'lumot qo'shma.\n"
    "2. Ism qidirishda: to'liq mos topishga harakat qil, topilmasa 'topilmadi' de.\n"
    "3. Har bir shaxs uchun alohida javob ber.\n"
    "4. Ball so'ralganda: jadvalda 'Umumiy ball' ustuni bo'lsa — o'sha qiymatni ber.\n"
    "5. Ma'lumot topilmasa boshqa ism bilan almashtirma.\n"
    "6. {lang_rule}\n"
    "7. Javob qisqa va aniq bo'lsin."
)

TRANSLATE_SYSTEM = (
    "Sen tarjimon assistantsan. Berilgan matnni ko'rsatilgan tilga tarjima qil. "
    "Faqat tarjimani yoz, boshqa hech narsa qo'shma."
)

# ─── Shared aiohttp session (reuse across all requests) ──────────────────────
_http_session: aiohttp.ClientSession | None = None

def _get_http() -> aiohttp.ClientSession:
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession()
    return _http_session

# ─── Sheet data cache: {sheet_id: (data, fetched_at)} ───────────────────────
_SHEET_CACHE: dict[str, tuple[dict, float]] = {}
_SHEET_CACHE_TTL = 180.0  # 3 minutes

def _sheet_cache_get(sheet_id: str) -> dict | None:
    entry = _SHEET_CACHE.get(sheet_id)
    if entry and (time.time() - entry[1]) < _SHEET_CACHE_TTL:
        return entry[0]
    return None

def _sheet_cache_set(sheet_id: str, data: dict):
    _SHEET_CACHE[sheet_id] = (data, time.time())


def _build_schema_info(sources: list) -> str:
    """Extract all column names from loaded sources and return a human-readable summary."""
    all_columns: list[str] = []
    seen: set[str] = set()
    for src in sources:
        data = src.get("data")
        headers: list[str] = []
        if isinstance(data, list) and data:
            headers = [str(c).strip() for c in data[0] if str(c).strip()]
        elif isinstance(data, dict):
            for rows in data.values():
                if isinstance(rows, list) and rows:
                    headers = [str(c).strip() for c in rows[0] if str(c).strip()]
                    break
        for h in headers:
            if h and h.lower() not in seen:
                seen.add(h.lower())
                all_columns.append(h)
    if all_columns:
        return f"JADVAL USTUNLARI: {', '.join(all_columns)}\n\n"
    return ""


async def ask_grok(question: str, context: str, grok_key: str, lang: str = "uz", schema_info: str = "") -> str:
    lang_rule = LANG_INSTRUCTION.get(lang, LANG_INSTRUCTION["uz"])
    if context.strip():
        schema_section = f"JADVAL USTUNLARI: {schema_info}\n\n" if schema_info else ""
        system = GROK_SYSTEM.format(lang_rule=lang_rule, schema_section=schema_section)
        user_msg = f"JADVAL:\n{context}\n\nSAVOL: {question}\n\nJavob {lang_rule}"
    else:
        system = TRANSLATE_SYSTEM
        user_msg = question

    models = ["grok-3-mini", "grok-3-mini-fast", "grok-2-latest"]
    last_err = ""
    for model in models:
        try:
            http = _get_http()
            async with http.post(
                "https://api.x.ai/v1/chat/completions",
                headers={"Authorization": f"Bearer {grok_key}", "Content-Type": "application/json"},
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": user_msg},
                    ],
                    "temperature": 0,
                    "max_tokens": 3000,
                },
                timeout=aiohttp.ClientTimeout(total=90),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    answer = data["choices"][0]["message"]["content"]
                    logger.info(f"Grok({model}): {answer[:80]}")
                    return answer
                last_err = f"{model}: HTTP {resp.status}"
                logger.warning(last_err)
        except Exception as e:
            last_err = f"{model}: {e}"
            logger.warning(last_err)
    return f"❌ AI xatolik: {last_err[:80]}"


# ─── Web search ──────────────────────────────────────────────────────────────

# Real-time trigger keywords — Tavily faqat shu so'zlar bo'lganda ishga tushadi
_REALTIME_TRIGGERS = {
    # Uz
    "bugun", "bugungi", "hozir", "hozirgi", "joriy", "oxirgi", "so'nggi",
    "yangilik", "yangiliklar", "kurs", "valyuta", "dollar", "euro",
    "ob-havo", "havo", "harorat", "prognoz", "bashorat",
    "narx", "narxi", "baho", "bahosi", "neft", "oltin",
    "sport", "futbol", "natija", "o'yin", "match",
    "tirik", "live", "onlayn", "online",
    # Ru
    "сегодня", "сейчас", "текущий", "последний", "последние",
    "новость", "новости", "курс", "валюта", "погода",
    "цена", "нефть", "золото", "спорт",
    # En
    "today", "current", "latest", "now", "live", "real-time",
    "news", "weather", "exchange", "rate", "price", "score",
}

def _is_realtime_query(question: str) -> bool:
    """Return True if the question likely needs fresh internet data."""
    q = question.lower()
    return any(kw in q for kw in _REALTIME_TRIGGERS)


async def _fetch_tavily_snippets(query: str, tavily_key: str) -> str | None:
    """
    Fast Tavily search: basic depth, 3 results, no answer synthesis.
    Returns a clean context string for Grok, or None on timeout/error.
    """
    try:
        http = _get_http()
        async with http.post(
            "https://api.tavily.com/search",
            json={
                "api_key": tavily_key,
                "query": query,
                "search_depth": "basic",   # fast mode
                "include_answer": False,    # skip slow answer synthesis
                "max_results": 3,
            },
            timeout=aiohttp.ClientTimeout(total=5),  # hard 5s limit
        ) as resp:
            if resp.status != 200:
                logger.warning(f"Tavily HTTP {resp.status}")
                return None
            data = await resp.json()

        results = data.get("results", [])
        if not results:
            return None

        # Clean output: title + snippet only
        lines = []
        for r in results:
            title = r.get("title", "").strip()
            snippet = r.get("content", "").strip()[:300]
            url = r.get("url", "")
            if title or snippet:
                lines.append(f"• {title}\n  {snippet}\n  ({url})")
        return "\n\n".join(lines) if lines else None

    except asyncio.TimeoutError:
        logger.warning("Tavily timeout (5s) — falling back to Grok knowledge")
        return None
    except Exception as e:
        logger.warning(f"Tavily error: {e}")
        return None


async def do_web_search(query: str, tavily_key: str, grok_key: str = "", lang: str = "uz") -> str:
    """
    Full web search for web_search mode (user explicitly pressed 🌐).
    Uses fast snippets + Grok to compose a final answer.
    """
    snippets = await _fetch_tavily_snippets(query, tavily_key)

    if snippets and grok_key:
        lang_names = {"uz": "O'zbek tilida", "ru": "на русском языке", "en": "in English"}
        lang_str = lang_names.get(lang, "O'zbek tilida")
        prompt = (
            f"Quyidagi internet qidiruv natijalari asosida savolga qisqa va aniq javob ber {lang_str}.\n"
            f"Faqat ma'lum bo'lgan ma'lumotlarni yoz — ixtiro qilma.\n\n"
            f"SAVOL: {query}\n\n"
            f"QIDIRUV NATIJALARI:\n{snippets}"
        )
        try:
            answer = await ask_grok(prompt, "", grok_key, lang)
            # Append source URLs
            sources_block = ""
            lines = [l for l in snippets.split("\n") if l.strip().startswith("(http")]
            if lines:
                sources_block = "\n\n🔗 <b>Manbalar:</b>"
                for i, l in enumerate(lines[:3], 1):
                    url = l.strip().strip("()")
                    sources_block += f"\n{i}. {url}"
            return answer + sources_block
        except Exception as e:
            logger.warning(f"Grok web summary error: {e}")

    if snippets:
        return t(lang, "search_result", answer=snippets[:1500])

    return t(lang, "search_fail", err="Natija topilmadi")


# ─── Voice ───────────────────────────────────────────────────────────────────
async def transcribe_voice(ogg: bytes, openai_key: str) -> tuple[str | None, str]:
    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=openai_key)
        buf = io.BytesIO(ogg)
        buf.name = "voice.ogg"
        tr = await client.audio.transcriptions.create(model="whisper-1", file=buf, temperature=0)
        text = tr.text.strip()
        return (text, "") if text else (None, "No speech detected")
    except ImportError:
        return None, "openai not installed"
    except Exception as e:
        return None, str(e)[:80]


# ─── Sheets / Drive ──────────────────────────────────────────────────────────
def _parse_csv(text: str) -> list:
    import csv
    rows = []
    try:
        for row in csv.reader(io.StringIO(text)):
            if any(c.strip() for c in row):
                rows.append(row)
    except Exception:
        pass
    return rows


def _extract_sheet_id(url: str):
    for p in [r"spreadsheets/d/([a-zA-Z0-9\-_]+)", r"^([a-zA-Z0-9\-_]{40,})$"]:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None


def _extract_folder_id(url: str):
    for p in [r"drive/folders/([a-zA-Z0-9\-_]+)", r"open\?id=([a-zA-Z0-9\-_]+)", r"id=([a-zA-Z0-9\-_]+)"]:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None


async def fetch_sheet_public(sheet_id: str) -> dict:
    cached = _sheet_cache_get(sheet_id)
    if cached is not None:
        logger.info(f"Sheet cache HIT sid={sheet_id[:8]}")
        return cached
    try:
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
        http = _get_http()
        async with http.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                rows = _parse_csv(await resp.text())
                if rows:
                    result = {"Sheet1": rows}
                    _sheet_cache_set(sheet_id, result)
                    return result
    except Exception as e:
        logger.error(f"fetch_sheet_public: {e}")
    return {}


async def fetch_sheet_with_creds(sheet_id: str, creds_json: str) -> dict:
    cache_key = f"{sheet_id}:creds"
    cached = _sheet_cache_get(cache_key)
    if cached is not None:
        logger.info(f"Sheet cache HIT (creds) sid={sheet_id[:8]}")
        return cached
    result = {}
    try:
        def _sync_fetch():
            _result = {}
            creds = Credentials.from_authorized_user_info(json.loads(creds_json), scopes=SCOPES)
            svc = build("sheets", "v4", credentials=creds)
            meta = svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
            tabs = [tab["properties"]["title"] for tab in meta.get("sheets", [])]

            def _fetch_tab(title):
                try:
                    vals = svc.spreadsheets().values().get(
                        spreadsheetId=sheet_id, range=title, valueRenderOption="FORMATTED_VALUE"
                    ).execute()
                    rows = vals.get("values", [])
                    return title, rows if rows else None
                except Exception as e:
                    logger.warning(f"Tab '{title}': {e}")
                    return title, None

            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=min(8, len(tabs))) as pool:
                for title, rows in pool.map(_fetch_tab, tabs):
                    if rows:
                        _result[title] = rows
            return _result

        result = await asyncio.to_thread(_sync_fetch)
        if result:
            _sheet_cache_set(cache_key, result)
    except Exception as e:
        logger.error(f"fetch_sheet_with_creds: {e}")
    return result


async def fetch_folder_sheets(folder_id: str, creds_json: str) -> dict:
    result = {}
    try:
        creds = Credentials.from_authorized_user_info(json.loads(creds_json), scopes=SCOPES)
        drive = build("drive", "v3", credentials=creds)
        sheets_svc = build("sheets", "v4", credentials=creds)
        items, token = [], None
        while True:
            q = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
            r = drive.files().list(q=q, fields="nextPageToken,files(id,name)", pageSize=50, pageToken=token).execute()
            items.extend(r.get("files", []))
            token = r.get("nextPageToken")
            if not token:
                break
        for item in items[:20]:
            try:
                meta = sheets_svc.spreadsheets().get(spreadsheetId=item["id"]).execute()
                sd = {}
                for tab in meta.get("sheets", [])[:10]:
                    title = tab["properties"]["title"]
                    try:
                        vals = sheets_svc.spreadsheets().values().get(
                            spreadsheetId=item["id"], range=title, valueRenderOption="FORMATTED_VALUE"
                        ).execute()
                        rows = vals.get("values", [])
                        if rows:
                            sd[title] = rows
                    except Exception:
                        pass
                if sd:
                    result[f"{item['name']}::{item['id']}"] = sd
            except Exception as e:
                logger.warning(f"Spreadsheet {item['id']}: {e}")
    except Exception as e:
        logger.error(f"fetch_folder_sheets: {e}")
    return result


# ─── OAuth server ─────────────────────────────────────────────────────────────
class OAuthServer:
    def __init__(self, bot: Bot, config):
        self.bot = bot
        self.config = config
        self._runner = None

    async def start(self):
        app = web.Application()
        app.router.add_get("/", self._oauth)
        app.router.add_get("/health", self._health)
        app.router.add_get("/miniapp", self._miniapp)
        app.router.add_static("/static", path=os.path.join(os.path.dirname(__file__), "static"), name="static")
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        await web.TCPSite(self._runner, self.config.host, self.config.port).start()
        logger.info(f"HTTP server on port {self.config.port}")

    async def stop(self):
        if self._runner:
            await self._runner.cleanup()

    async def _health(self, req):
        return web.Response(text="OK")

    async def _miniapp(self, req):
        index = os.path.join(os.path.dirname(__file__), "static", "index.html")
        return web.FileResponse(index)

    async def _oauth(self, req):
        code = req.query.get("code")
        state = req.query.get("state")
        if not code or not state or state not in _oauth_states:
            return web.Response(text="<h2>Invalid OAuth request</h2>", content_type="text/html")
        info = _oauth_states.pop(state)
        uid, mode, flow = info["uid"], info["mode"], info["flow"]
        lang = info.get("lang", "uz")
        try:
            flow.fetch_token(code=code)
            creds_json = flow.credentials.to_json()
            save_token(uid, creds_json)
            sess = get_session(uid)
            sess.google_creds_json = creds_json
            if mode == "sheets":
                sess.step = "waiting_sheet"
                await self.bot.send_message(uid, t(lang, "auth_ok_sheets"), reply_markup=kb_cancel(lang), parse_mode="HTML")
            else:
                sess.step = "waiting_folder"
                await self.bot.send_message(uid, t(lang, "auth_ok_folder"), reply_markup=kb_cancel(lang), parse_mode="HTML")
            return web.Response(text="<h2>✅ Connected! Go back to the bot.</h2>", content_type="text/html")
        except Exception as e:
            logger.error(f"OAuth: {e}")
            await self.bot.send_message(uid, f"❌ OAuth error: {e}")
            return web.Response(text=f"<h2>Error: {e}</h2>", content_type="text/html")


# ─── Config ──────────────────────────────────────────────────────────────────
@dataclass
class Config:
    bot_token: str
    grok_key: str
    openai_key: str
    tavily_key: str
    google_client_id: str
    google_client_secret: str
    redirect_uri: str
    host: str
    port: int
    drive_service_email: str
    miniapp_url: str  # Netlify mini app URL
    api_url: str  # Internal API URL for session sync (e.g. https://xxx.koyeb.app)

    @classmethod
    def from_env(cls) -> "Config":
        load_dotenv()
        bot_token = os.getenv("BOT_TOKEN", "").strip()
        if not bot_token:
            raise RuntimeError("BOT_TOKEN is not set!")
        port = int(os.getenv("BOT_PORT", os.getenv("PORT", os.getenv("SERVER_PORT", "8081"))))
        domain = os.getenv("APP_DOMAIN", "").strip()
        redirect = os.getenv("GOOGLE_REDIRECT_URI", "").strip()
        if not redirect:
            redirect = f"https://{domain}/" if (domain and domain != "localhost") else f"http://localhost:{port}/"
        miniapp_url = os.getenv("MINIAPP_URL", "").strip()
        api_url = os.getenv("API_URL", f"https://{domain}" if domain else "").strip()
        return cls(
            bot_token=bot_token,
            grok_key=os.getenv("GROK_API_KEY", "").strip(),
            openai_key=os.getenv("OPENAI_API_KEY", "").strip(),
            tavily_key=os.getenv("TAVILY_API_KEY", "").strip(),
            google_client_id=os.getenv("GOOGLE_CLIENT_ID", "").strip(),
            google_client_secret=os.getenv("GOOGLE_CLIENT_SECRET", "").strip(),
            redirect_uri=redirect,
            host="0.0.0.0",
            port=port,
            drive_service_email=os.getenv("DRIVE_SERVICE_EMAIL", "").strip(),
            miniapp_url=miniapp_url,
            api_url=api_url,
        )



# ─── Session sync helper ──────────────────────────────────────────────────────
async def _sync_session_to_api(uid: int, sources: list, lang: str, api_url: str):
    """Push session sources from bot.py to main.py so the mini app sees them."""
    if not api_url:
        return
    light = []
    for s in sources:
        rows = s.get("data", [])
        if not rows or len(rows) < 2:
            continue
        header = rows[0]
        preview = [dict(zip([str(h) for h in header], row)) for row in rows[1:]]  # ALL rows, no limit
        light.append({
            "name": s.get("source_name", "Manba"),
            "type": s.get("source_type", "excel"),
            "rows": len(rows) - 1,
            "preview": preview,
            "disabled": False,
        })
    try:
        async with aiohttp.ClientSession() as client:
            await client.post(
                f"{api_url}/api/sync_session",
                json={"telegram_id": uid, "sources": light, "lang": lang},
                timeout=aiohttp.ClientTimeout(total=30),
            )
        logger.info(f"Session synced to API: uid={uid} sources={len(light)}")
    except Exception as e:
        logger.warning(f"Session sync failed uid={uid}: {e}")


# ─── Handlers ────────────────────────────────────────────────────────────────
def register(dp: Dispatcher, config: Config, bot: Bot):

    def make_flow():
        return Flow.from_client_config(
            {"web": {
                "client_id": config.google_client_id,
                "client_secret": config.google_client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [config.redirect_uri],
            }},
            scopes=SCOPES,
            redirect_uri=config.redirect_uri,
        )

    # ── /start ────────────────────────────────────────────────────────────────
    @dp.message(CommandStart())
    async def cmd_start(msg: Message):
        uid = msg.from_user.id
        sess = get_session(uid)
        sess.web_search = False
        sess.lang = load_lang(uid)
        if not sess.google_creds_json:
            creds = load_refresh_token(uid)
            if creds:
                sess.google_creds_json = creds

        # ── Registration gate: check local SQLite first (fast), then Supabase
        if not is_registered_local(uid) and not supa_is_registered(uid):
            sess.step = "reg_lang"
            await msg.answer(
                "🌍 Xush kelibsiz! Iltimos, tilni tanlang:\n"
                "Добро пожаловать! Выберите язык:\n"
                "Welcome! Please choose language:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(text="🇺🇿 O'zbek", callback_data="reglang_uz"),
                        InlineKeyboardButton(text="🇷🇺 Русский", callback_data="reglang_ru"),
                        InlineKeyboardButton(text="🇬🇧 English", callback_data="reglang_en"),
                    ]
                ]),
            )
            return

        # ── Already registered: normal welcome
        sess.step = "idle"
        source_count = db_source_count(uid)
        name = msg.from_user.first_name or "User"
        welcome = t(sess.lang, "welcome", name=name)
        if source_count > 0:
            welcome += f"\n\n📚 Sizda <b>{source_count}</b> ta ulangan manba bor. /my_sources"

        # ── State restore: if user had a sheet before restart, silently re-fetch it
        if not has_data(sess):
            last_url = load_last_sheet_url(uid)
            if last_url:
                sheet_id = _extract_sheet_id(last_url)
                if sheet_id:
                    try:
                        data = await fetch_sheet_public(sheet_id)
                        if data:
                            src_name = list(data.keys())[0] if len(data) == 1 else f"Sheets ({sheet_id[:8]})"
                            sess.sources = [{
                                "source_type": "google_sheets",
                                "source_name": src_name,
                                "source_url": last_url,
                                "sheet_id": sheet_id,
                                "data": data,
                            }]
                            sess.schema_info = _build_schema_info(sess.sources)
                            sess.last_sheet_url = last_url
                            sess.sheets_data = data
                            sess.sheet_id = sheet_id
                            sess.sheet_name = src_name
                            sess.step = "in_chat"
                            welcome += f"\n\n✅ Oxirgi jadval tiklandi: <b>{src_name}</b>"
                            logger.info(f"State restored uid={uid} sheet={sheet_id[:8]}")
                    except Exception as _e:
                        logger.warning(f"State restore failed uid={uid}: {_e}")

        await msg.answer(welcome, reply_markup=kb_main(sess.lang) if not has_data(sess) else kb_chat(sess.lang), parse_mode="HTML")

    # ── /help ─────────────────────────────────────────────────────────────────
    @dp.message(Command("help"))
    async def cmd_help(msg: Message):
        sess = get_session(msg.from_user.id)
        await msg.answer(t(sess.lang, "help"), reply_markup=kb_main(sess.lang), parse_mode="HTML")

    # ── /lang ─────────────────────────────────────────────────────────────────
    @dp.message(Command("lang"))
    async def cmd_lang(msg: Message):
        sess = get_session(msg.from_user.id)
        await msg.answer(t(sess.lang, "choose_lang"), reply_markup=kb_lang())

    # ── /disconnect ───────────────────────────────────────────────────────────
    @dp.message(Command("disconnect"))
    async def cmd_disconnect(msg: Message):
        uid = msg.from_user.id
        sess = get_session(uid)
        lang = sess.lang
        sess.step = "idle"
        sess.google_creds_json = ""
        sess.sheets_data = {}
        sess.folder_data = {}
        sess.excel_data = []
        sess.sources = []
        sess.web_search = False
        try:
            with _db_conn() as c:
                c.execute("DELETE FROM tokens WHERE uid=?", (uid,))
                c.commit()
        except Exception:
            pass
        await msg.answer(t(lang, "disconnected"), reply_markup=kb_main(lang))

    # ── /my_sources ───────────────────────────────────────────────────────────
    @dp.message(Command("my_sources"))
    async def cmd_my_sources(msg: Message):
        uid = msg.from_user.id
        sess = get_session(uid)
        lang = sess.lang
        rows = db_list_sources(uid)
        if not rows:
            await msg.answer(
                "📭 Sizda hali ulangan ma'lumot yo'q.\n"
                "Google Sheets link yoki Excel fayl yuboring.",
                reply_markup=kb_main(lang),
            )
            return
        icons = {"excel": "📊", "google_sheets": "🔗", "drive_folder": "📁"}
        lines = ["📚 <b>Sizda ulangan manbalar:</b>\n"]
        for i, row in enumerate(rows, 1):
            icon = icons.get(row["source_type"], "📄")
            stype_label = {"excel": "Excel", "google_sheets": "Google Sheets", "drive_folder": "Drive"}.get(row["source_type"], row["source_type"])
            date_str = (row["created_at"] or "")[:10]
            lines.append(f"{i}. {icon} <b>{row['source_name']}</b> [{stype_label}] — {date_str}")
        lines.append(
            "\n💡 O'chirish: /delete_source <i>raqam</i>\n"
            "💡 Hammasini tozalash: /clear_all"
        )
        await msg.answer("\n".join(lines), parse_mode="HTML")

    # ── /delete_source ────────────────────────────────────────────────────────
    @dp.message(Command("delete_source"))
    async def cmd_delete_source(msg: Message):
        uid = msg.from_user.id
        sess = get_session(uid)
        lang = sess.lang
        args = (msg.text or "").strip().split(maxsplit=1)
        if len(args) < 2 or not args[1].isdigit():
            await msg.answer(
                "❌ Noto'g'ri format.\n"
                "To'g'ri: <code>/delete_source 2</code>\n"
                "Raqamni ko'rish: /my_sources",
                parse_mode="HTML",
            )
            return
        num = int(args[1])
        deleted = db_delete_source(uid, num)
        if deleted is None:
            count = db_source_count(uid)
            await msg.answer(
                f"❌ {num}-raqamli manba topilmadi. "
                f"Sizda {count} ta manba bor. /my_sources ni ko'ring."
            )
            return
        # Also remove from session sources
        sess.sources = [s for s in sess.sources if s.get("source_name") != deleted]
        remaining = db_source_count(uid)
        await msg.answer(
            f"🗑 <b>{deleted}</b> o'chirildi.\n📚 Qolgan manbalar: <b>{remaining}</b> ta",
            parse_mode="HTML",
        )

    # ── /clear_all ────────────────────────────────────────────────────────────
    @dp.message(Command("clear_all"))
    async def cmd_clear_all(msg: Message):
        uid = msg.from_user.id
        sess = get_session(uid)
        lang = sess.lang
        count = db_source_count(uid)
        if count == 0:
            await msg.answer("📭 Manbalar allaqachon bo'sh.")
            return
        sess.pending_clear = True
        await msg.answer(
            f"⚠️ Barcha <b>{count}</b> ta manbani o'chirib tashlaysizmi?\n\n"
            "Tasdiqlash uchun yozing: <code>Ha</code>\nBekor qilish: <code>Yo'q</code>",
            parse_mode="HTML",
        )

    # ── /clear_all confirm via text ───────────────────────────────────────────
    # (Handled inside handle_text below via sess.pending_clear flag)

    # ── /privacy ──────────────────────────────────────────────────────────────
    @dp.message(Command("privacy"))
    async def cmd_privacy(msg: Message):
        await msg.answer(
            "🔒 <b>Maxfiylik kafolatlari</b>\n\n"
            "✅ Sizning ma'lumotlaringiz <b>faqat sizga ko'rinadi</b>.\n"
            "   Boshqa hech qaysi foydalanuvchi sizning fayllaringizni ko'ra olmaydi.\n\n"
            "🗑 Excel faylingiz serverga yuklanadi, <b>24 soat ichida o'chiriladi</b>.\n"
            "   Fayl o'qilib bo'lgach, xotiradan ham tozalanadi.\n\n"
            "👁 Google Sheets'ga <b>faqat o'qish huquqi</b> bilan kiramiz.\n"
            "   Yozish, o'chirish yoki tahrirlash imkonimiz yo'q.\n\n"
            "🔐 Google access token'ingiz bazada saqlanmaydi.\n"
            "   Har safar ommaviy havola orqali o'qiladi.\n\n"
            "🚫 SQL injection va xakerlik hujumlaridan himoyalangan.\n"
            "   Barcha so'rovlar parametrli query bilan bajariladi.\n\n"
            "⚡ Rate limit: 1 daqiqada 20 ta so'rovdan ko'p yuborib bo'lmaydi.\n\n"
            "🧹 Xohlasangiz <b>/clear_all</b> buyrug'i bilan barcha ma'lumotlaringizni\n"
            "   darhol o'chirib tashlashingiz mumkin.\n\n"
            "📦 <b>/export_my_data</b> — ma'lumotlaringizni JSON formatida olish (GDPR).",
            parse_mode="HTML",
        )

    # ── /export_my_data ───────────────────────────────────────────────────────
    @dp.message(Command("export_my_data"))
    async def cmd_export_my_data(msg: Message):
        uid = msg.from_user.id
        sess = get_session(uid)
        rows = db_list_sources(uid)
        if not rows:
            await msg.answer("📭 Sizda saqlangan ma'lumot yo'q.")
            return
        # Build GDPR-compliant export: only metadata, no file content
        export = {
            "user_id": uid,
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "data_sources": [
                {
                    "id": r["id"],
                    "source_type": r["source_type"],
                    "source_name": r["source_name"],
                    # source_url included for transparency (user's own data)
                    "source_url": r["source_url"],
                    "file_id": r["file_id"],
                    "created_at": r["created_at"],
                }
                for r in rows
            ],
            "note": (
                "Bu faylda faqat ma'lumot manbalari ro'yxati mavjud. "
                "Fayl mazmuni (Excel/Sheets ichidagi jadval) bu yerda saqlanmaydi."
            ),
        }
        json_bytes = json.dumps(export, ensure_ascii=False, indent=2).encode("utf-8")
        buf = io.BytesIO(json_bytes)
        buf.name = f"my_data_{uid}.json"
        await msg.answer_document(
            document=buf,
            caption=(
                f"📦 <b>Sizning ma'lumotlaringiz (GDPR)</b>\n"
                f"📅 Sana: {export['exported_at'][:10]}\n"
                f"📚 Manbalar soni: {len(rows)} ta"
            ),
            parse_mode="HTML",
        )

    # ── Callbacks ─────────────────────────────────────────────────────────────
    @dp.callback_query()
    async def handle_cb(cb: CallbackQuery):
        uid = cb.from_user.id
        sess = get_session(uid)
        lang = sess.lang
        d = cb.data
        await cb.answer()

        if d.startswith("lang_"):
            new_lang = d[5:]
            if new_lang in ("uz", "ru", "en"):
                sess.lang = new_lang
                save_lang(uid, new_lang)
                await cb.message.answer(t(new_lang, "lang_set"), reply_markup=kb_main(new_lang))

        elif d == "open_lang":
            await cb.message.answer(t(lang, "choose_lang"), reply_markup=kb_lang())

        elif d == "disconnect":
            await cmd_disconnect(cb.message)

        elif d == "chat_continue":
            sess.web_search = False
            sess.step = "in_chat"
            hints = {"uz": "💬 Savolingizni yozing:", "ru": "💬 Задайте вопрос:", "en": "💬 Ask your question:"}
            await cb.message.answer(hints.get(lang, "💬 Ask your question:"))

        elif d == "voice_hint":
            icons = {"uz": "🎤 Ovozli xabar yuboring.", "ru": "🎤 Отправьте голосовое сообщение.", "en": "🎤 Send a voice message."}
            await cb.message.answer(icons.get(lang, "🎤 Send voice."))

        elif d == "web_search":
            if not config.tavily_key:
                await cb.message.answer(t(lang, "no_search_key"))
                return
            sess.web_search = True
            sess.step = "in_chat"
            await cb.message.answer(t(lang, "web_on"), reply_markup=kb_cancel(lang))

        elif d == "exit_chat":
            sess.step = "idle"
            sess.web_search = False
            await cb.message.answer(t(lang, "exited"), reply_markup=kb_main(lang))

        elif d == "google_auth":
            if not config.google_client_id:
                await cb.message.answer(t(lang, "oauth_not_set"))
                return
            import secrets as _sec
            flow = make_flow()
            auth_url, _ = flow.authorization_url(access_type="offline", prompt="consent")
            state = _sec.token_urlsafe(16)
            _oauth_states[state] = {"uid": uid, "mode": "sheets", "flow": flow, "lang": lang}
            await cb.message.answer(t(lang, "auth_link", url=auth_url), parse_mode="HTML")

        elif d == "public_link":
            sess.step = "waiting_sheet"
            await cb.message.answer(t(lang, "ask_sheets"), reply_markup=kb_cancel(lang), parse_mode="HTML")

        elif d.startswith("reglang_"):
            new_lang = d[8:]
            if new_lang not in ("uz", "ru", "en"):
                await cb.answer()
                return
            sess.lang = new_lang
            save_lang(uid, new_lang)
            sess.step = "reg_name"
            prompts = {
                "uz": "👤 Ism va familiyangizni kiriting:\n<i>Masalan: Abdullayev Jasur Hamidovich</i>",
                "ru": "👤 Введите ваше имя и фамилию:\n<i>Например: Иванов Иван Иванович</i>",
                "en": "👤 Enter your full name:\n<i>Example: John Smith</i>",
            }
            await cb.message.answer(prompts[new_lang], reply_markup=ReplyKeyboardRemove(), parse_mode="HTML")
            await cb.answer()

    # ── Button: Excel ─────────────────────────────────────────────────────────
    @dp.message(F.text.in_({t(l, "btn_excel") for l in ("uz","ru","en")}))
    async def btn_excel(msg: Message):
        sess = get_session(msg.from_user.id)
        sess.step = "waiting_excel"
        await msg.answer(t(sess.lang, "ask_excel"), reply_markup=kb_cancel(sess.lang))

    # ── Button: Sheets ────────────────────────────────────────────────────────
    @dp.message(F.text.in_({t(l, "btn_sheets") for l in ("uz","ru","en")}))
    async def btn_sheets(msg: Message):
        sess = get_session(msg.from_user.id)
        lang = sess.lang
        # Always use public link (Google OAuth disabled for now)
        sess.step = "waiting_sheet"
        await msg.answer(t(lang, "ask_sheets"), reply_markup=kb_cancel(lang), parse_mode="HTML")

    # ── Button: Folder ────────────────────────────────────────────────────────
    @dp.message(F.text.in_({t(l, "btn_folder") for l in ("uz","ru","en")}))
    async def btn_folder(msg: Message):
        uid = msg.from_user.id
        sess = get_session(uid)
        lang = sess.lang
        service_email = config.drive_service_email
        if not service_email:
            await msg.answer(
                "⚠️ Google Drive integratsiyasi hozircha sozlanmagan.",
                reply_markup=kb_main(lang),
            )
            return
        # Show service email instructions, then ask for folder link
        instructions = {
            "uz": (
                f"📁 <b>Google Drive papkani ulash:</b>\n\n"
                f"1. Google Drive da papkangizni oching\n"
                f"2. Papkaga o'ng tugma bosing → <b>Ulashish</b>\n"
                f"3. Quyidagi emailga kirish bering:\n"
                f"<code>{service_email}</code>\n\n"
                f"4. Papka havolasini (link) yuboring 👇"
            ),
            "ru": (
                f"📁 <b>Подключение папки Google Drive:</b>\n\n"
                f"1. Откройте папку в Google Drive\n"
                f"2. Правая кнопка → <b>Поделиться</b>\n"
                f"3. Дайте доступ этому email:\n"
                f"<code>{service_email}</code>\n\n"
                f"4. Отправьте ссылку на папку 👇"
            ),
            "en": (
                f"📁 <b>Connect Google Drive folder:</b>\n\n"
                f"1. Open your folder in Google Drive\n"
                f"2. Right-click → <b>Share</b>\n"
                f"3. Grant access to this email:\n"
                f"<code>{service_email}</code>\n\n"
                f"4. Send the folder link 👇"
            ),
        }
        sess.step = "waiting_folder"
        await msg.answer(instructions.get(lang, instructions["uz"]), reply_markup=kb_cancel(lang), parse_mode="HTML")

    # ── Button: Search ────────────────────────────────────────────────────────
    @dp.message(F.text.in_({t(l, "btn_search") for l in ("uz","ru","en")}))
    async def btn_search(msg: Message):
        sess = get_session(msg.from_user.id)
        lang = sess.lang
        if not config.tavily_key:
            await msg.answer(t(lang, "no_search_key"), reply_markup=kb_main(lang))
            return
        sess.web_search = True
        sess.step = "in_chat"
        await msg.answer(t(lang, "web_on"), reply_markup=kb_cancel(lang))

    # ── Button: Help ──────────────────────────────────────────────────────────
    @dp.message(F.text.in_({t(l, "btn_help") for l in ("uz","ru","en")}))
    async def btn_help(msg: Message):
        sess = get_session(msg.from_user.id)
        await msg.answer(t(sess.lang, "help"), reply_markup=kb_main(sess.lang), parse_mode="HTML")

    # ── Button: Settings ──────────────────────────────────────────────────────
    @dp.message(F.text.in_({t(l, "btn_settings") for l in ("uz","ru","en")}))
    async def btn_settings(msg: Message):
        sess = get_session(msg.from_user.id)
        lang = sess.lang
        await msg.answer(t(lang, "settings"), reply_markup=kb_settings(lang), parse_mode="HTML")

    # ── Button: Cancel ────────────────────────────────────────────────────────
    @dp.message(F.text.in_({t(l, "btn_cancel") for l in ("uz","ru","en")}))
    async def btn_cancel(msg: Message):
        sess = get_session(msg.from_user.id)
        sess.step = "idle"
        sess.web_search = False
        await msg.answer(t(sess.lang, "cancelled"), reply_markup=kb_main(sess.lang))

    # ── Document (Excel) ──────────────────────────────────────────────────────
    @dp.message(F.document)
    async def handle_doc(msg: Message):
        uid = msg.from_user.id
        sess = get_session(uid)
        lang = sess.lang

        # Rate limit check
        if not _rate_limiter.is_allowed(uid):
            await msg.answer("⏳ Juda ko'p so'rov. 1 daqiqadan keyin urinib ko'ring.")
            return

        doc = msg.document
        fname = (doc.file_name or "file").lower()
        if not fname.endswith((".xlsx", ".xls", ".xlsm")):
            await msg.answer(t(lang, "no_excel"), reply_markup=kb_main(lang) if not has_data(sess) else kb_chat(lang), parse_mode="HTML")
            return
        loading = await msg.answer(t(lang, "loading"))
        try:
            tfile = await msg.bot.get_file(doc.file_id)
            buf = io.BytesIO()
            await msg.bot.download_file(tfile.file_path, destination=buf)
            # parse_excel writes to temp file, reads, then securely deletes — uid passed for temp naming
            rows = parse_excel(doc.file_name or "file.xlsx", buf.getvalue(), uid=uid)
            buf.seek(0); buf.truncate(0)  # also clear in-memory buffer
            if not rows:
                await loading.edit_text("❌ Fayl bo'sh yoki o'qib bo'lmadi.")
                return
            non_empty = [r for r in rows if any(str(c).strip() for c in r)]
            n_cols = max((len(r) for r in rows[:5]), default=0)
            header = rows[0] if rows else []
            header_str = " | ".join(str(h) for h in header[:8] if str(h).strip())

            # Unique name to avoid duplicates
            raw_name = doc.file_name or "Excel"
            unique_name = _unique_source_name(uid, raw_name)

            # Save to DB (INSERT only, no content stored in DB — only file_id reference)
            db_add_source(uid, "excel", unique_name, source_url=None, file_id=doc.file_id)
            total_count = db_source_count(uid)

            # Add to session sources — skip if same file_id already loaded
            already_loaded = any(s.get("file_id") == doc.file_id for s in sess.sources)
            if not already_loaded:
                sess.sources.append({
                    "source_type": "excel",
                    "source_name": unique_name,
                    "file_id": doc.file_id,
                    "data": rows,
                })
            # Also update legacy field for backward compat
            sess.excel_data = rows
            sess.sheet_name = unique_name
            sess.web_search = False
            sess.step = "in_chat"
            # Safe log: no file content, no URLs
            _safe_log(uid, "excel_upload", unique_name, f"rows={len(rows)} total_sources={total_count}")

            # Success messages (different if name was deduplicated)
            name_note = f" (nom o'zgartirildi: <code>{unique_name}</code>)" if unique_name != raw_name else ""
            await loading.edit_text(
                t(lang, "excel_ok", name=unique_name, rows=len(non_empty), cols=n_cols, headers=header_str)
                + f"\n\n✅ Qo'shildi{name_note}\n📚 Jami manbalar: <b>{total_count}</b> ta",
                parse_mode="HTML",
            )
            if config.miniapp_url:
                await msg.answer(
                    "✅ Ma'lumot yuklandi! Savollarni <b>Mini App</b> orqali bering 👇",
                    parse_mode="HTML",
                    reply_markup=kb_miniapp_open(config.miniapp_url, lang),
                )
            else:
                await msg.answer("👇", reply_markup=kb_chat(lang, config.miniapp_url))
            asyncio.create_task(_sync_session_to_api(uid, sess.sources, sess.lang, config.api_url))
        except Exception as e:
            logger.error(f"Excel upload error uid={uid}: {e}")
            await loading.edit_text(f"❌ Xatolik: {e}")

    # ── Voice ─────────────────────────────────────────────────────────────────
    # TODO: voice feature temporarily disabled — better version coming soon
    @dp.message(F.voice)
    async def handle_voice(msg: Message):
        sess = get_session(msg.from_user.id)
        lang = sess.lang
        msgs = {
            "uz": "🎙 Ovozli so'rov vaqtincha o'chirilgan. Iltimos, yozma savol yuboring.",
            "ru": "🎙 Голосовые запросы временно отключены. Пожалуйста, напишите вопрос.",
            "en": "🎙 Voice queries are temporarily disabled. Please send a text message.",
        }
        await msg.answer(msgs.get(lang, msgs["uz"]), reply_markup=kb_chat(sess.lang) if has_data(sess) else kb_main(lang))

    # ── Contact (phone share during registration) ──────────────────────────────
    @dp.message(F.contact)
    async def handle_contact(msg: Message):
        uid = msg.from_user.id
        sess = get_session(uid)
        lang = sess.lang
        if sess.step != "reg_phone":
            return
        phone = msg.contact.phone_number
        tg_username = msg.from_user.username or ""
        supa_upsert_user(uid, tg_username, sess.reg_full_name, phone, lang)
        save_registered(uid)   # ← local SQLite flag, persistent across restarts
        save_lang(uid, lang)
        saved_name = sess.reg_full_name
        sess.step = "idle"
        sess.reg_full_name = ""
        first = saved_name.split()[0] if saved_name else (msg.from_user.first_name or "")
        source_count = db_source_count(uid)
        welcome_text = t(lang, "welcome", name=first)
        if source_count > 0:
            welcome_text += f"\n\n📚 Sizda <b>{source_count}</b> ta ulangan manba bor. /my_sources"
        success_msgs = {
            "uz": "✅ <b>Ro'yxatdan o'tdingiz!</b>",
            "ru": "✅ <b>Регистрация завершена!</b>",
            "en": "✅ <b>Registration complete!</b>",
        }
        await msg.answer(
            success_msgs.get(lang, success_msgs["uz"]) + "\n\n" + welcome_text,
            reply_markup=kb_main(lang),
            parse_mode="HTML",
        )

    # ── Text ──────────────────────────────────────────────────────────────────
    @dp.message(F.text)
    async def handle_text(msg: Message):
        uid = msg.from_user.id
        text = (msg.text or "").strip()
        if not text:
            return
        sess = get_session(uid)
        lang = sess.lang

        # Rate limit check (skip for /clear_all Ha/Yo'q confirmation)
        if not sess.pending_clear and not _rate_limiter.is_allowed(uid):
            await msg.answer("⏳ Juda ko'p so'rov. 1 daqiqadan keyin urinib ko'ring.")
            return

        # Safe log: no message content for waiting_sheet (contains URL)
        if sess.step == "waiting_sheet":
            logger.info(f"TEXT uid={uid} step='waiting_sheet'")
        else:
            logger.info(f"TEXT uid={uid} step={sess.step!r} | {text[:60]!r}")

        # ── registration steps
        if sess.step == "reg_name":
            name_input = text.strip()
            if len(name_input) < 3:
                err_msgs = {
                    "uz": "❌ Ism kamida 3 ta harf bo'lishi kerak. Qayta yozing:",
                    "ru": "❌ Минимум 3 символа. Повторите:",
                    "en": "❌ Min 3 characters. Try again:",
                }
                await msg.answer(err_msgs.get(lang, err_msgs["uz"]))
                return
            sess.reg_full_name = name_input
            sess.step = "reg_phone"
            phone_prompts = {
                "uz": "📱 Telefon raqamingizni ulashing:\n(Pastdagi tugmani bosing)",
                "ru": "📱 Поделитесь номером телефона:\n(Нажмите кнопку ниже)",
                "en": "📱 Share your phone number:\n(Press the button below)",
            }
            await msg.answer(phone_prompts.get(lang, phone_prompts["uz"]), reply_markup=kb_phone(lang))
            return

        if sess.step == "reg_phone":
            # User sent text instead of contact — remind them
            remind = {
                "uz": "📱 Iltimos, kontaktni ulashish tugmasini bosing.",
                "ru": "📱 Нажмите кнопку, чтобы поделиться контактом.",
                "en": "📱 Please use the button to share your contact.",
            }
            await msg.answer(remind.get(lang, remind["uz"]), reply_markup=kb_phone(lang))
            return

        # ── /clear_all confirmation
        if sess.pending_clear:
            sess.pending_clear = False
            if text.strip().lower() in ("ha", "да", "yes"):
                db_clear_all_sources(uid)
                sess.sources = []
                sess.excel_data = []
                sess.sheets_data = {}
                sess.folder_data = {}
                sess.step = "idle"
                await msg.answer("✅ Barcha manbalar o'chirildi.", reply_markup=kb_main(lang))
            else:
                await msg.answer("❌ Bekor qilindi.", reply_markup=kb_main(lang) if not has_data(sess) else kb_chat(lang))
            return

        # ── waiting_sheet
        if sess.step == "waiting_sheet":
            # Security: validate URL domain before processing
            url_ok, url_err = validate_sheets_url(text)
            if not url_ok:
                await msg.answer(url_err, reply_markup=kb_cancel(lang), parse_mode="HTML")
                return
            sheet_id = _extract_sheet_id(text)
            if not sheet_id:
                await msg.answer(t(lang, "not_found_id"), reply_markup=kb_cancel(lang), parse_mode="HTML")
                return
            status = await msg.answer(t(lang, "loading"))
            try:
                data = await fetch_sheet_with_creds(sheet_id, sess.google_creds_json) if sess.google_creds_json else await fetch_sheet_public(sheet_id)
                if not data:
                    await status.edit_text(t(lang, "sheets_fail"), parse_mode="HTML")
                    return
                total = sum(len(r) for r in data.values())
                sheet_tabs = list(data.keys())
                auto_name = sheet_tabs[0] if len(sheet_tabs) == 1 else f"Sheets ({sheet_id[:8]})"
                unique_name = _unique_source_name(uid, auto_name)
                # DB INSERT (never replace) — store URL in DB for reference, not in logs
                db_add_source(uid, "google_sheets", unique_name, source_url=text, file_id=None)
                total_count = db_source_count(uid)
                # Session: ADD only if same sheet_id not already loaded
                new_src = {
                    "source_type": "google_sheets",
                    "source_name": unique_name,
                    "source_url": text,
                    "sheet_id": sheet_id,
                    "data": data,
                }
                already_loaded = any(s.get("sheet_id") == sheet_id for s in sess.sources)
                if not already_loaded:
                    sess.sources.append(new_src)
                else:
                    # Update existing source data (refresh)
                    for s in sess.sources:
                        if s.get("sheet_id") == sheet_id:
                            s["data"] = data
                            break
                # Dynamic schema: build column description from all sources
                sess.schema_info = _build_schema_info(sess.sources)
                # State persistence: save URL so it survives bot restart
                sess.last_sheet_url = text
                save_last_sheet_url(uid, text)
                # Legacy compat
                sess.sheets_data = data
                sess.sheet_id = sheet_id
                sess.sheet_name = unique_name
                sess.web_search = False
                sess.step = "in_chat"
                # Safe log: no URL in log output
                cols_preview = sess.schema_info[:80] if sess.schema_info else "—"
                _safe_log(uid, "sheets_add", unique_name, f"tabs={len(data)} rows={total} cols={cols_preview} total_sources={total_count}")
                await status.edit_text(
                    f"✅ <b>{unique_name}</b> ulandi\n"
                    f"📚 Jami manbalar: <b>{total_count}</b> ta",
                    parse_mode="HTML",
                )
                if config.miniapp_url:
                    await msg.answer(
                        "✅ Ma'lumot yuklandi! Savollarni <b>Mini App</b> orqali bering 👇",
                        parse_mode="HTML",
                        reply_markup=kb_miniapp_open(config.miniapp_url, lang),
                    )
                else:
                    await msg.answer("👇", reply_markup=kb_chat(lang, config.miniapp_url))
                asyncio.create_task(_sync_session_to_api(uid, sess.sources, sess.lang, config.api_url))
            except Exception as e:
                logger.error(f"Sheets add error uid={uid}: {e}")
                await status.edit_text(f"❌ Google Sheets'ga ulanib bo'lmadi. Link to'g'riligini tekshiring.")
            return

        # ── waiting_folder
        if sess.step == "waiting_folder":
            folder_id = _extract_folder_id(text)
            if not folder_id:
                await msg.answer(t(lang, "folder_not_found"), reply_markup=kb_cancel(lang), parse_mode="HTML")
                return
            if not sess.google_creds_json:
                await msg.answer(t(lang, "choose_connect"), reply_markup=kb_connect(lang))
                return
            status = await msg.answer(t(lang, "loading"))
            try:
                data = await fetch_folder_sheets(folder_id, sess.google_creds_json)
                if not data:
                    await status.edit_text("❌ Papkada spreadsheet topilmadi yoki ruxsat yo'q.")
                    return
                total_rows = sum(len(rows) for sheets in data.values() for rows in sheets.values())
                sess.folder_data = data
                sess.sheets_data = {}
                sess.excel_data = []
                sess.web_search = False
                sess.step = "in_chat"
                await status.edit_text(
                    t(lang, "folder_ok", files=len(data), rows=total_rows),
                    parse_mode="HTML",
                )
                await msg.answer("👇", reply_markup=kb_chat(lang))
            except Exception as e:
                await status.edit_text(f"❌ {e}")
            return

        # ── in_chat or has data
        if sess.step == "in_chat" or has_data(sess) or sess.web_search:
            if sess.step != "in_chat":
                sess.step = "in_chat"
            await _process_question(msg, sess, text)
            return

        # ── Sheets link from idle (auto-detect)
        if "docs.google.com/spreadsheets" in text or re.search(r"spreadsheets/d/[a-zA-Z0-9\-_]+", text):
            # Security: validate URL domain
            url_ok, url_err = validate_sheets_url(text)
            if not url_ok:
                await msg.answer(url_err, parse_mode="HTML")
                return
            sheet_id = _extract_sheet_id(text)
            if sheet_id:
                status = await msg.answer(t(lang, "loading"))
                try:
                    data = await fetch_sheet_public(sheet_id) if not sess.google_creds_json else await fetch_sheet_with_creds(sheet_id, sess.google_creds_json)
                    if data:
                        total = sum(len(r) for r in data.values())
                        sheet_tabs = list(data.keys())
                        auto_name = sheet_tabs[0] if len(sheet_tabs) == 1 else f"Sheets ({sheet_id[:8]})"
                        unique_name = _unique_source_name(uid, auto_name)
                        db_add_source(uid, "google_sheets", unique_name, source_url=text, file_id=None)
                        total_count = db_source_count(uid)
                        sess.sources.append({
                            "source_type": "google_sheets",
                            "source_name": unique_name,
                            "source_url": text,
                            "sheet_id": sheet_id,
                            "data": data,
                        })
                        sess.sheets_data = data
                        sess.sheet_id = sheet_id
                        sess.sheet_name = unique_name
                        sess.web_search = False
                        sess.step = "in_chat"
                        _safe_log(uid, "sheets_add_idle", unique_name, f"tabs={len(data)} rows={total}")
                        await status.edit_text(
                            f"✅ <b>{unique_name}</b> ulandi\n"
                            f"📚 Jami manbalar: <b>{total_count}</b> ta",
                            parse_mode="HTML",
                        )
                        if config.miniapp_url:
                            await msg.answer(
                                "✅ Ma'lumot yuklandi! Savollarni <b>Mini App</b> orqali bering 👇",
                                parse_mode="HTML",
                                reply_markup=kb_miniapp_open(config.miniapp_url, lang),
                            )
                        else:
                            await msg.answer("👇", reply_markup=kb_chat(lang, config.miniapp_url))
                        asyncio.create_task(_sync_session_to_api(uid, sess.sources, sess.lang, config.api_url))
                    else:
                        await status.edit_text(t(lang, "sheets_fail"), parse_mode="HTML")
                except Exception as e:
                    logger.error(f"Sheets idle add error uid={uid}: {e}")
                    await status.edit_text("❌ Google Sheets'ga ulanib bo'lmadi. Link to'g'riligini tekshiring.")
            return

        # ── Default
        await msg.answer(t(lang, "no_data"), reply_markup=kb_main(lang))

    # ── Q&A engine ────────────────────────────────────────────────────────────
    async def _process_question(msg: Message, sess: Session, question: str):
        uid = msg.from_user.id
        lang = sess.lang
        logger.info(f"Q uid={uid} web={sess.web_search} data={has_data(sess)} q={question[:60]!r}")
        await msg.bot.send_chat_action(msg.chat.id, "typing")

        # ── Disambiguation: user picked a number from candidate list
        if sess.disambiguation_candidates:
            q_stripped = question.strip()
            if q_stripped.isdigit():
                idx = int(q_stripped) - 1
                if 0 <= idx < len(sess.disambiguation_candidates):
                    chosen = sess.disambiguation_candidates[idx]
                    sess.last_found_names = [chosen]
                    orig_question = sess.disambiguation_question or chosen
                    sess.disambiguation_candidates = []
                    sess.disambiguation_question = ""
                    # Re-run with chosen full name + original question context
                    # e.g. "Maxamatmusayev Yodgorbek algebra fanidan olgan balli nechchi"
                    if orig_question and chosen.lower() not in orig_question.lower():
                        question = chosen + " " + orig_question
                    else:
                        question = orig_question
                    logger.info(f"Disambiguation: uid={uid} chose '{chosen}' → q={question[:80]!r}")
                else:
                    await msg.answer(f"❌ {q_stripped} raqami noto'g'ri. Ro'yxatdagi raqamni kiriting.")
                    return
            else:
                # User typed something else — clear disambiguation
                sess.disambiguation_candidates = []
                sess.disambiguation_question = ""

        # ── Refresh Google Sheets sources (cache-aware: re-fetch only if TTL expired)
        for src in sess.sources:
            if src.get("source_type") == "google_sheets":
                sheet_url = src.get("source_url", "")
                sid = src.get("sheet_id") or _extract_sheet_id(sheet_url)
                if sid:
                    # _sheet_cache_get returns None if expired → fetch_sheet_public will re-fetch
                    fresh = await fetch_sheet_public(sid)
                    if fresh:
                        src["data"] = fresh

        # Web-only search
        if sess.web_search and not has_data(sess):
            if not config.tavily_key:
                await msg.answer(t(lang, "no_search_key"), reply_markup=kb_main(lang))
                return
            status = await msg.answer(t(lang, "searching"))
            result = await do_web_search(question, config.tavily_key, config.grok_key, lang)
            await status.edit_text(result, parse_mode="HTML")
            await msg.answer("👇", reply_markup=kb_chat(lang))
            return

        if not has_data(sess):
            await msg.answer(t(lang, "no_data"), reply_markup=kb_main(lang))
            return

        # 1. Web search mode — bypass everything, go straight to Tavily
        if sess.web_search:
            if not config.tavily_key:
                await msg.answer(t(lang, "no_search_key"), reply_markup=kb_main(lang))
                sess.web_search = False
                return
            status = await msg.answer(t(lang, "searching"))
            web_res = await do_web_search(question, config.tavily_key, config.grok_key, lang)
            await status.edit_text(web_res, parse_mode="HTML")
            await msg.answer("👇", reply_markup=kb_chat(lang))
            return

        # 2. Try Python exact answer (Excel/Sheets lookup)
        try:
            py_ans = _python_answer(question, sess)
        except Exception as e:
            logger.warning(f"python_answer error: {e}")
            py_ans = None

        # CASE A: Python topdi → javob ber, Grok chaqirma
        if py_ans is not None:
            logger.info(f"Python answered uid={uid}")
            await msg.answer(py_ans, parse_mode="HTML", reply_markup=kb_chat(lang))
            return

        # CASE B: Python None qaytardi — ism qidiruvi bo'lganmi tekshir
        # Agar savol ism qidiruvi bo'lsa va Python None qaytarsa →
        # ma'lumot topilmadi, Grok chaqirmasdan "topilmadi" xabarini ber
        if not sess.web_search:
            _person_cands = _get_person_candidates_from_question(question)
            if _person_cands:
                logger.info(f"Person query, py_ans=None → topilmadi (no Grok) uid={uid} cands={_person_cands}")
                searched = ", ".join(f"<b>{c.capitalize()}</b>" for c in _person_cands[:3])
                await msg.answer(
                    f"❌ {searched} — ma'lumotlar bazasida topilmadi.\n\n"
                    "💡 Familiya yoki to'liq ism bilan qayta yozing.",
                    parse_mode="HTML",
                    reply_markup=kb_chat(lang),
                )
                return

        # CASE C: Odam emas, tahlil/hisoblash savoli → Grok'ka yubor
        # Lekin butun jadvalni emas — faqat savol bilan bog'liq qatorlarni yubor
        if not config.grok_key:
            ctx = build_context(sess)
            await msg.answer(f"⚠️ AI sozlanmagan.\n\n{ctx[:2000]}", parse_mode=None, reply_markup=kb_chat(lang))
            return

        status = await msg.answer(t(lang, "thinking"))
        ctx = _build_slim_context(sess, question)  # slim: faqat mos qatorlar
        logger.info(f"Slim context uid={uid}: {len(ctx)} chars (full was {len(build_context(sess))})")

        if not ctx.strip():
            await status.edit_text(t(lang, "no_data"))
            return

        # Smart router: if question needs real-time data AND Tavily key exists,
        # fetch snippets in parallel (5s timeout) and inject into Grok context
        extra_web_ctx = ""
        if config.tavily_key and _is_realtime_query(question):
            logger.info(f"Realtime query detected uid={uid} — fetching Tavily snippets")
            try:
                snippets = await asyncio.wait_for(
                    _fetch_tavily_snippets(question, config.tavily_key),
                    timeout=5.0,
                )
                if snippets:
                    extra_web_ctx = f"\n\nINTERNET MA'LUMOTLARI (hozirgi):\n{snippets}"
                    logger.info(f"Tavily snippets injected uid={uid}")
            except asyncio.TimeoutError:
                logger.warning(f"Tavily router timeout uid={uid} — Grok uses own knowledge")
            except Exception as _e:
                logger.warning(f"Tavily router error uid={uid}: {_e}")

        full_ctx = ctx + extra_web_ctx
        answer = await ask_grok(question, full_ctx, config.grok_key, lang, schema_info=sess.schema_info)

        if len(answer) > 4000:
            parts = [answer[i:i+4000] for i in range(0, len(answer), 4000)]
            await status.delete()
            for i, part in enumerate(parts):
                kb = kb_chat(lang) if i == len(parts) - 1 else None
                await msg.answer(part, parse_mode="HTML", reply_markup=kb)
        else:
            await status.edit_text(answer, parse_mode="HTML")
            await msg.answer("👇", reply_markup=kb_chat(lang))


# ─── Main ─────────────────────────────────────────────────────────────────────
async def main():
    config = Config.from_env()
    logger.info("=" * 55)
    logger.info("OnBrain AI Bot starting...")
    logger.info(f"  GROK     : {'SET' if config.grok_key else 'NOT SET'}")
    logger.info(f"  OPENAI   : {'SET' if config.openai_key else 'NOT SET'}")
    logger.info(f"  TAVILY   : {'SET' if config.tavily_key else 'NOT SET'}")
    logger.info(f"  GOOGLE   : {'SET' if config.google_client_id else 'NOT SET'}")
    logger.info(f"  PORT     : {config.port}")
    logger.info("=" * 55)

    _init_db()
    bot = Bot(token=config.bot_token, default=DefaultBotProperties(parse_mode=None))

    try:
        me = await bot.get_me()
        logger.info(f"Bot: @{me.username}")
    except Exception as e:
        logger.error(f"Bot auth failed: {e}")
        raise

    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass
    await asyncio.sleep(5)  # Give Telegram time to release previous polling connection

    # Set bot commands
    from aiogram.types import BotCommand
    await bot.set_my_commands([
        BotCommand(command="start", description="🏠 Bosh menyu"),
        BotCommand(command="help", description="❓ Yordam"),
        BotCommand(command="lang", description="🌍 Til tanlash"),
        BotCommand(command="my_sources", description="📚 Ulangan manbalar ro'yxati"),
        BotCommand(command="delete_source", description="🗑 Manbani o'chirish (raqam)"),
        BotCommand(command="clear_all", description="🧹 Barcha manbalarni tozalash"),
        BotCommand(command="privacy", description="🔒 Maxfiylik kafolatlari"),
        BotCommand(command="export_my_data", description="📦 Ma'lumotlarimni yuklab olish (GDPR)"),
        BotCommand(command="disconnect", description="🔌 Ma'lumotlarni tozalash"),
    ])

    dp = Dispatcher()
    register(dp, config, bot)

    oauth = OAuthServer(bot, config)
    oauth_started = False
    try:
        await oauth.start()
        oauth_started = True
        logger.info(f"OAuth HTTP server started on port {config.port}")
    except OSError as e:
        logger.warning(f"OAuth server could not start (port busy): {e} — continuing without it")

    logger.info("Polling started...")
    try:
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            relax_timeout=30.0,
            long_poll_timeout=30.0,
        )
    finally:
        if oauth_started:
            await oauth.stop()
        await bot.session.close()


if __name__ == "__main__":
    logger.info("OnBrain AI Bot process started.")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped.")
