import asyncio
import io
import json
import logging
import os
import re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import aiohttp
import openpyxl
import requests
import xlrd
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
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

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

MAX_ROWS = 1000
MAX_COLS = 50
MAX_CHARS = 100_000


@dataclass
class Session:
    step: str = "idle"
    excel_data: list = field(default_factory=list)
    sheets_data: dict = field(default_factory=dict)
    folder_data: dict = field(default_factory=dict)
    sheet_name: str = ""
    sheet_id: str = ""
    google_creds_json: str = ""
    web_search: bool = False


_sessions: dict = {}


def get_session(uid: int) -> Session:
    if uid not in _sessions:
        _sessions[uid] = Session()
    return _sessions[uid]


_DB = os.environ.get("SQLITE_TOKEN_DB", "google_tokens.db")


def _db_conn():
    conn = sqlite3.connect(_DB, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db():
    with _db_conn() as c:
        c.execute(
            "CREATE TABLE IF NOT EXISTS tokens("
            "uid INTEGER PRIMARY KEY, creds TEXT, updated TEXT)"
        )
        c.commit()


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


def parse_excel(name: str, content: bytes) -> list:
    rows = []
    try:
        if name.lower().endswith(".xls"):
            wb = xlrd.open_workbook(file_contents=content)
            ws = wb.sheet_by_index(0)
            for r in range(ws.nrows):
                rows.append([str(ws.cell_value(r, c)) for c in range(ws.ncols)])
        else:
            wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
            ws = wb.active
            for row in ws.iter_rows(values_only=True):
                rows.append([str(v) if v is not None else "" for v in row])
    except Exception as e:
        logger.error(f"Excel parse error: {e}")
    return rows


def _rows_to_text(rows: list, max_rows: int = MAX_ROWS) -> str:
    """Convert list of rows to readable text, keeping ALL cells including empty ones for alignment."""
    lines = []
    for i, row in enumerate(rows[:max_rows]):
        # Keep all columns up to MAX_COLS, replace None/empty with dash
        cells = []
        for c in row[:MAX_COLS]:
            val = str(c).strip() if c is not None else ""
            cells.append(val if val else "-")
        # Skip fully empty rows
        if all(v == "-" for v in cells):
            continue
        lines.append(f"{i+1}. {' | '.join(cells)}")
    return "\n".join(lines)


def build_context(session: Session) -> str:
    parts = []
    if session.folder_data:
        for sid, sheets in session.folder_data.items():
            parts.append(f"\n=== {sid} ===")
            for title, rows in sheets.items():
                parts.append(f"--- {title} ---")
                parts.append(_rows_to_text(rows))
    elif session.sheets_data:
        name = session.sheet_name or "Sheet"
        parts.append(f"=== {name} ===")
        for title, rows in session.sheets_data.items():
            parts.append(f"--- {title} ---")
            parts.append(_rows_to_text(rows))
    elif session.excel_data:
        name = session.sheet_name or "Excel"
        parts.append(f"=== {name} ===")
        parts.append(_rows_to_text(session.excel_data))
    ctx = "\n".join(parts)
    # If too long, keep first MAX_CHARS chars but warn
    if len(ctx) > MAX_CHARS:
        logger.warning(f"Context truncated: {len(ctx)} -> {MAX_CHARS} chars")
        ctx = ctx[:MAX_CHARS]
    return ctx


def has_data(session: Session) -> bool:
    return bool(session.excel_data or session.sheets_data or session.folder_data)


async def ask_grok(question: str, context: str, grok_key: str) -> str:
    system = """Sen jadval (Excel/Sheets) malumotlarini tahlil qiluvchi assistantsan.

MUHIM QOIDALAR:
1. Faqat quyida berilgan jadval malumotlari asosida javob ber.
2. Ism qidirishda QATTIQ QOIDA:
   - To'liq mos: "Yodgorbek" => "Yodgorbek" satrini qidir
   - Qisqa variant: "Yodgor" => "Yodgor" bilan boshlanadigan BARCHA ismlarni topib ko'r (Yodgorbek, Yodgorali, Yodgor)
   - Agar "Moxizoda" so'ralsa => "Moxizoda" degan ism bor satrni qidir, "Moxinur" EMAS
   - Har bir ism ALOHIDA qidiriladi va har biri uchun ALOHIDA javob beriladi
3. Ko'p shaxs so'ralganda (masalan "Moxizoda va Yodgorbeking ballari"):
   - Har birini alohida qidir
   - Har biri uchun topilgan yoki topilmaganini ayt
4. "Umumiy ball" yoki "ball" so'ralganda: hamma ball ustunlarini qo'shib yig'indisini ber
5. Malumot topilmasa: "Jadvalda [ism] topilmadi" de, boshqa ismni o'rniga qo'yma
6. Javob o'zbek tilida, qisqa va aniq bo'lsin
7. Raqamlarni oqilona yoz"""

    user_prompt = (
        f"JADVAL MALUMOTLARI:\n{context}\n\n"
        f"SAVOL: {question}\n\n"
        f"Yuqoridagi jadval malumotlariga qarab aniq javob ber."
    )

    # Use grok-3-mini for better accuracy with data analysis
    models = ["grok-3-mini", "grok-3-mini-fast", "grok-2-latest"]
    last_err = ""
    for model in models:
        try:
            async with aiohttp.ClientSession() as http:
                async with http.post(
                    "https://api.x.ai/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {grok_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": model,
                        "messages": [
                            {"role": "system", "content": system},
                            {"role": "user", "content": user_prompt},
                        ],
                        "temperature": 0.1,
                        "max_tokens": 3000,
                    },
                    timeout=aiohttp.ClientTimeout(total=90),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        answer = data["choices"][0]["message"]["content"]
                        logger.info(f"Grok ({model}) ok: {answer[:100]}")
                        return answer
                    else:
                        body = await resp.text()
                        last_err = f"{model}: HTTP {resp.status} -- {body[:120]}"
                        logger.warning(f"Grok failed: {last_err}")
        except Exception as e:
            last_err = f"{model}: {e}"
            logger.warning(f"Grok exception: {last_err}")
    return f"AI xizmatida xatolik: {last_err[:100]}"


async def do_web_search(query: str, tavily_key: str) -> str:
    try:
        resp = await asyncio.to_thread(
            requests.post,
            "https://api.tavily.com/search",
            json={
                "api_key": tavily_key,
                "query": query,
                "include_answer": True,
                "max_results": 5,
            },
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            answer = data.get("answer", "")
            sources = data.get("results", [])
            result = f"Internet qidiruv natijasi:\n\n{answer or 'Javob topilmadi.'}"
            if sources:
                result += "\n\nManbalar:\n"
                for i, s in enumerate(sources[:3], 1):
                    result += f"{i}. {s.get('title', '')}\n"
            return result
        return f"Tavily xatolik: HTTP {resp.status_code}"
    except Exception as e:
        return f"Internet qidiruv xatolik: {e}"


def _extract_sheet_id(url: str):
    for p in [r"spreadsheets/d/([a-zA-Z0-9\-_]+)", r"^([a-zA-Z0-9\-_]{40,})$"]:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None


def _extract_folder_id(url: str):
    for p in [
        r"drive/folders/([a-zA-Z0-9\-_]+)",
        r"open\?id=([a-zA-Z0-9\-_]+)",
        r"id=([a-zA-Z0-9\-_]+)",
    ]:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None


def _parse_csv(text: str) -> list:
    import csv
    rows = []
    try:
        reader = csv.reader(io.StringIO(text))
        for row in reader:
            if any(c.strip() for c in row):
                rows.append(row)
    except Exception:
        pass
    return rows


async def fetch_sheet_public(sheet_id: str) -> dict:
    result = {}
    try:
        csv_url = (
            f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
        )
        async with aiohttp.ClientSession() as http:
            async with http.get(csv_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    rows = _parse_csv(text)
                    if rows:
                        result["Sheet1"] = rows
    except Exception as e:
        logger.error(f"fetch_sheet_public error: {e}")
    return result


async def fetch_sheet_with_creds(sheet_id: str, creds_json: str) -> dict:
    result = {}
    try:
        creds = Credentials.from_authorized_user_info(json.loads(creds_json), scopes=SCOPES)
        svc = build("sheets", "v4", credentials=creds)
        meta = svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
        for tab in meta.get("sheets", []):
            title = tab["properties"]["title"]
            try:
                vals = svc.spreadsheets().values().get(
                    spreadsheetId=sheet_id,
                    range=title,
                    valueRenderOption="FORMATTED_VALUE",
                ).execute()
                rows = vals.get("values", [])
                if rows:
                    result[title] = rows
            except Exception as e:
                logger.warning(f"Tab '{title}' error: {e}")
    except Exception as e:
        logger.error(f"fetch_sheet_with_creds error: {e}")
    return result


async def fetch_folder_sheets(folder_id: str, creds_json: str) -> dict:
    result = {}
    try:
        creds = Credentials.from_authorized_user_info(json.loads(creds_json), scopes=SCOPES)
        drive_svc = build("drive", "v3", credentials=creds)
        sheets_svc = build("sheets", "v4", credentials=creds)
        items = []
        page_token = None
        while True:
            query = (
                f"'{folder_id}' in parents and "
                "mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
            )
            resp = drive_svc.files().list(
                q=query,
                fields="nextPageToken,files(id,name)",
                pageSize=50,
                pageToken=page_token,
            ).execute()
            items.extend(resp.get("files", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        logger.info(f"Folder {folder_id}: {len(items)} spreadsheets")
        for item in items[:20]:
            sid = item["id"]
            sname = item["name"]
            try:
                meta = sheets_svc.spreadsheets().get(spreadsheetId=sid).execute()
                sheets_data = {}
                for tab in meta.get("sheets", [])[:10]:
                    title = tab["properties"]["title"]
                    try:
                        vals = sheets_svc.spreadsheets().values().get(
                            spreadsheetId=sid,
                            range=title,
                            valueRenderOption="FORMATTED_VALUE",
                        ).execute()
                        rows = vals.get("values", [])
                        if rows:
                            sheets_data[title] = rows
                    except Exception as e:
                        logger.warning(f"Tab error: {e}")
                if sheets_data:
                    result[f"{sname}::{sid}"] = sheets_data
            except Exception as e:
                logger.warning(f"Spreadsheet error: {e}")
    except Exception as e:
        logger.error(f"fetch_folder_sheets error: {e}")
    return result


async def transcribe_voice(ogg_bytes: bytes, openai_key: str):
    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=openai_key)
        audio_file = io.BytesIO(ogg_bytes)
        audio_file.name = "voice.ogg"
        transcript = await client.audio.transcriptions.create(
            model="whisper-1",
            file=audio_file,
            language="uz",
            temperature=0,
        )
        text = transcript.text.strip()
        return (text, "") if text else (None, "Nutq aniqlanmadi")
    except ImportError:
        return None, "OpenAI kutubxonasi ornatilmagan"
    except Exception as e:
        logger.error(f"Transcription error: {e}")
        return None, f"Xatolik: {str(e)[:80]}"


def kb_main():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Google Sheets"), KeyboardButton(text="Excel yuklash")],
            [KeyboardButton(text="Internet qidiruv"), KeyboardButton(text="Yordam")],
        ],
        resize_keyboard=True,
    )


def kb_chat():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Savol berish davom", callback_data="chat_continue"),
                InlineKeyboardButton(text="Ovozli savol", callback_data="voice_hint"),
            ],
            [InlineKeyboardButton(text="Internet qidiruv", callback_data="web_search")],
            [InlineKeyboardButton(text="Chatdan chiqish", callback_data="exit_chat")],
        ]
    )


def kb_cancel():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Bekor qilish")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def kb_connect():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Google hisobiga ulash", callback_data="google_auth")],
            [
                InlineKeyboardButton(
                    text="Ommaviy link bilan (OAuth siz)",
                    callback_data="public_link",
                )
            ],
        ]
    )


class OAuthServer:
    def __init__(self, bot: Bot, config):
        self.bot = bot
        self.config = config
        self._runner = None

    async def start(self):
        app = web.Application()
        app.router.add_get("/", self._handle_oauth)
        app.router.add_get("/health", self._health)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.config.host, self.config.port)
        await site.start()
        logger.info(f"HTTP server started on port {self.config.port}")

    async def stop(self):
        if self._runner:
            await self._runner.cleanup()

    async def _health(self, request):
        return web.Response(text="OK")

    async def _handle_oauth(self, request):
        code = request.query.get("code")
        state = request.query.get("state")
        if not code or not state or state not in _oauth_states:
            return web.Response(
                text="<h2>Xatolik: notogri OAuth sorov</h2>",
                content_type="text/html",
            )
        info = _oauth_states.pop(state)
        uid = info["uid"]
        mode = info["mode"]
        flow = info["flow"]
        try:
            flow.fetch_token(code=code)
            creds_json = flow.credentials.to_json()
            save_token(uid, creds_json)
            sess = get_session(uid)
            sess.google_creds_json = creds_json
            if mode == "sheets":
                sess.step = "waiting_sheet"
                await self.bot.send_message(
                    uid,
                    "Google hisobiga ulandi!\nEndi Google Sheets havolasini yuboring:",
                    reply_markup=kb_cancel(),
                )
            elif mode == "folder":
                sess.step = "waiting_folder"
                await self.bot.send_message(
                    uid,
                    "Google hisobiga ulandi!\nEndi Google Drive papka havolasini yuboring:",
                    reply_markup=kb_cancel(),
                )
            return web.Response(
                text="<h2>Muvaffaqiyatli ulandi! Botga qayting.</h2>",
                content_type="text/html",
            )
        except Exception as e:
            logger.error(f"OAuth error: {e}")
            await self.bot.send_message(uid, f"OAuth xatolik: {e}")
            return web.Response(text=f"<h2>Xatolik: {e}</h2>", content_type="text/html")


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

    @classmethod
    def from_env(cls) -> "Config":
        load_dotenv()
        bot_token = os.getenv("BOT_TOKEN", "").strip()
        if not bot_token:
            raise RuntimeError("BOT_TOKEN is not set!")
        port = int(os.getenv("PORT", os.getenv("SERVER_PORT", "8080")))
        domain = os.getenv("APP_DOMAIN", "").strip()
        redirect = os.getenv("GOOGLE_REDIRECT_URI", "").strip()
        if not redirect:
            if domain and domain != "localhost":
                redirect = f"https://{domain}/"
            else:
                redirect = f"http://localhost:{port}/"
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
        )


def register(dp: Dispatcher, config: Config, bot: Bot):

    def make_flow():
        return Flow.from_client_config(
            {
                "web": {
                    "client_id": config.google_client_id,
                    "client_secret": config.google_client_secret,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": [config.redirect_uri],
                }
            },
            scopes=SCOPES,
            redirect_uri=config.redirect_uri,
        )

    @dp.message(CommandStart())
    async def cmd_start(msg: Message):
        uid = msg.from_user.id
        sess = get_session(uid)
        sess.step = "idle"
        sess.web_search = False
        if not sess.google_creds_json:
            creds = load_refresh_token(uid)
            if creds:
                sess.google_creds_json = creds
        name = msg.from_user.first_name or "Foydalanuvchi"
        await msg.answer(
            f"Salom, {name}!\n\n"
            "OnBrain AI Bot - jadval malumotlari bilan ishlash uchun\n\n"
            "Nima qilish mumkin:\n"
            "- Google Sheets ulash\n"
            "- Excel fayl yuklash\n"
            "- Internet qidiruv\n"
            "- Ovozli savol berish\n\n"
            "Pastdagi tugmalardan birini tanlang:",
            reply_markup=kb_main(),
        )

    @dp.message(Command("help"))
    async def cmd_help(msg: Message):
        await msg.answer(
            "YORDAM\n\n"
            "Excel/Sheets bilan ishlash:\n"
            "1. Excel yuklash -> Excel faylni yuboring\n"
            "2. Google Sheets -> havolani yuboring\n"
            "3. Savol bering (matn yoki ovoz)\n\n"
            "Internet qidiruv:\n"
            "Internet qidiruv tugmasini bosing -> savol yuboring\n\n"
            "Ovozli savol:\n"
            "Ovozli xabar yuboring - bot transcribe qilib javob beradi\n\n"
            "Buyruqlar:\n"
            "/start - bosh menyu\n"
            "/help - yordam\n"
            "/disconnect - Googledan chiqish",
            reply_markup=kb_main(),
        )

    @dp.message(Command("disconnect"))
    async def cmd_disconnect(msg: Message):
        uid = msg.from_user.id
        sess = get_session(uid)
        sess.step = "idle"
        sess.google_creds_json = ""
        sess.sheets_data = {}
        sess.folder_data = {}
        sess.excel_data = []
        sess.web_search = False
        try:
            with _db_conn() as c:
                c.execute("DELETE FROM tokens WHERE uid=?", (uid,))
                c.commit()
        except Exception:
            pass
        await msg.answer("Malumotlar tozalandi. /start yuboring.", reply_markup=kb_main())

    @dp.callback_query()
    async def handle_callback(cb: CallbackQuery):
        uid = cb.from_user.id
        sess = get_session(uid)
        data = cb.data
        await cb.answer()

        if data == "chat_continue":
            sess.web_search = False
            await cb.message.answer("Savolingizni yuboring:", reply_markup=kb_cancel())

        elif data == "voice_hint":
            await cb.message.answer(
                "Ovozli xabar yuboring - savol sifatida qabul qilinadi."
            )

        elif data == "web_search":
            sess.web_search = True
            sess.step = "in_chat"
            await cb.message.answer(
                "Internet qidiruv yoqildi. Savolingizni yuboring:",
                reply_markup=kb_cancel(),
            )

        elif data == "exit_chat":
            sess.step = "idle"
            sess.web_search = False
            await cb.message.answer("Chat yopildi.", reply_markup=kb_main())

        elif data == "google_auth":
            if not config.google_client_id:
                await cb.message.answer("Google OAuth sozlanmagan.")
                return
            import secrets as _sec
            flow = make_flow()
            auth_url, _ = flow.authorization_url(
                access_type="offline", prompt="consent"
            )
            state = _sec.token_urlsafe(16)
            _oauth_states[state] = {"uid": uid, "mode": "sheets", "flow": flow}
            await cb.message.answer(
                f"Google hisobiga kirish uchun:\n{auth_url}"
            )

        elif data == "public_link":
            sess.step = "waiting_sheet"
            await cb.message.answer(
                "Google Sheets havolasini yuboring\n"
                "(fayl ommaviy bolishi kerak - 'Anyone with link can view')",
                reply_markup=kb_cancel(),
            )

    @dp.message(F.text == "Google Sheets")
    async def btn_sheets(msg: Message):
        uid = msg.from_user.id
        sess = get_session(uid)
        if sess.google_creds_json:
            sess.step = "waiting_sheet"
            await msg.answer(
                "Google Sheets havolasini yuboring:\nhttps://docs.google.com/spreadsheets/d/...",
                reply_markup=kb_cancel(),
            )
        else:
            await msg.answer(
                "Google Sheets ulash\n\nQuyidagilardan birini tanlang:",
                reply_markup=kb_connect(),
            )

    @dp.message(F.text == "Excel yuklash")
    async def btn_excel(msg: Message):
        uid = msg.from_user.id
        sess = get_session(uid)
        sess.step = "waiting_excel"
        await msg.answer(
            "Excel faylni yuboring (.xlsx yoki .xls):", reply_markup=kb_cancel()
        )

    @dp.message(F.text == "Internet qidiruv")
    async def btn_websearch(msg: Message):
        uid = msg.from_user.id
        sess = get_session(uid)
        if not config.tavily_key:
            await msg.answer("Internet qidiruv sozlanmagan (TAVILY_API_KEY yoq).")
            return
        sess.web_search = True
        sess.step = "in_chat"
        await msg.answer(
            "Internet qidiruv yoqildi. Savolingizni yuboring:", reply_markup=kb_cancel()
        )

    @dp.message(F.text == "Yordam")
    async def btn_help(msg: Message):
        await cmd_help(msg)

    @dp.message(F.text == "Bekor qilish")
    async def btn_cancel(msg: Message):
        uid = msg.from_user.id
        sess = get_session(uid)
        sess.step = "idle"
        sess.web_search = False
        await msg.answer("Bekor qilindi.", reply_markup=kb_main())

    @dp.message(F.document)
    async def handle_doc(msg: Message):
        uid = msg.from_user.id
        sess = get_session(uid)
        doc = msg.document
        fname = (doc.file_name or "file").lower()
        if not fname.endswith((".xlsx", ".xls", ".xlsm")):
            await msg.answer(
                "Faqat Excel fayl (.xlsx yoki .xls) qabul qilinadi.",
                reply_markup=kb_main() if not has_data(sess) else kb_chat(),
            )
            return
        await msg.answer("Excel yuklanmoqda...")
        try:
            tfile = await msg.bot.get_file(doc.file_id)
            buf = io.BytesIO()
            await msg.bot.download_file(tfile.file_path, destination=buf)
            rows = parse_excel(doc.file_name or "file.xlsx", buf.getvalue())
            if not rows:
                await msg.answer("Fayl bosh yoki oqib bolmadi.")
                return
            # Count non-empty rows
            non_empty = [r for r in rows if any(str(c).strip() for c in r)]
            n_cols = max((len(r) for r in rows[:5]), default=0)
            # Log header row for debugging
            header = rows[0] if rows else []
            logger.info(f"Excel uid={uid}: {len(rows)} rows, {n_cols} cols, file={doc.file_name}")
            logger.info(f"Excel header uid={uid}: {header[:10]}")
            sess.excel_data = rows
            sess.sheets_data = {}
            sess.folder_data = {}
            sess.sheet_name = doc.file_name or "Excel"
            sess.web_search = False
            sess.step = "in_chat"
            # Show first row (headers) to user
            header_str = " | ".join(str(h) for h in header[:8] if str(h).strip())
            await msg.answer(
                f"Excel yuklandi!\n"
                f"Fayl: {doc.file_name}\n"
                f"Qatorlar: {len(non_empty)} ta\n"
                f"Ustunlar: {n_cols} ta\n"
                f"Sarlavhalar: {header_str}\n\n"
                "Savolingizni yozing yoki ovozli yuboring:",
                reply_markup=kb_chat(),
            )
        except Exception as e:
            logger.error(f"Excel error uid={uid}: {e}")
            await msg.answer(f"Excel oqushda xatolik: {e}", reply_markup=kb_main())

    @dp.message(F.voice)
    async def handle_voice(msg: Message):
        uid = msg.from_user.id
        sess = get_session(uid)
        if not config.openai_key:
            await msg.answer(
                "Ovozli savol ishlamaydi - OPENAI_API_KEY ornatilmagan.\n"
                "Savolingizni matn shaklida yuboring.",
                reply_markup=kb_chat() if has_data(sess) else kb_main(),
            )
            return
        if not has_data(sess) and not sess.web_search:
            await msg.answer(
                "Avval malumot yuklang (Excel yoki Google Sheets).",
                reply_markup=kb_main(),
            )
            return
        await msg.bot.send_chat_action(msg.chat.id, "typing")
        try:
            tfile = await msg.bot.get_file(msg.voice.file_id)
            buf = io.BytesIO()
            await msg.bot.download_file(tfile.file_path, destination=buf)
            ogg_bytes = buf.getvalue()
        except Exception as e:
            await msg.answer(f"Audio yuklashda xatolik: {e}")
            return
        await msg.answer("Ovoz aniqlanayapti...")
        text, err = await transcribe_voice(ogg_bytes, config.openai_key)
        if not text:
            await msg.answer(
                f"Ovozni aniqlashda xatolik: {err}\nSavolingizni matn shaklida yuboring."
            )
            return
        await msg.answer(f"Aniqlandi: {text}")
        await _process_question(msg, sess, text)

    @dp.message(F.text)
    async def handle_text(msg: Message):
        uid = msg.from_user.id
        text = (msg.text or "").strip()
        if not text:
            return
        sess = get_session(uid)
        logger.info(
            f"TEXT uid={uid} step={sess.step!r} web={sess.web_search} "
            f"excel={len(sess.excel_data)} sheets={len(sess.sheets_data)} "
            f"folder={len(sess.folder_data)} | {text[:60]!r}"
        )

        # waiting_sheet step
        if sess.step == "waiting_sheet":
            sheet_id = _extract_sheet_id(text)
            if not sheet_id:
                await msg.answer(
                    "Google Sheets havolasi topilmadi.\n\n"
                    "Misol:\nhttps://docs.google.com/spreadsheets/d/1ABC.../edit",
                    reply_markup=kb_cancel(),
                )
                return
            await msg.answer("Google Sheets yuklanmoqda...")
            try:
                if sess.google_creds_json:
                    data = await fetch_sheet_with_creds(sheet_id, sess.google_creds_json)
                else:
                    data = await fetch_sheet_public(sheet_id)
                if not data:
                    await msg.answer(
                        "Google Sheets yuklanmadi.\n"
                        "Sabab: Fayl yopiq yoki ID notogri.\n"
                        "Share -> Anyone with link -> Viewer qiling.",
                        reply_markup=kb_cancel(),
                    )
                    return
                total = sum(len(r) for r in data.values())
                sess.sheets_data = data
                sess.folder_data = {}
                sess.excel_data = []
                sess.sheet_id = sheet_id
                sess.sheet_name = text[:50]
                sess.web_search = False
                sess.step = "in_chat"
                info = "\n".join(f"  {t}: {len(r)} qator" for t, r in data.items())
                await msg.answer(
                    f"Google Sheets yuklandi!\n\n{info}\nJami: {total} qator\n\n"
                    "Savolingizni yozing yoki ovozli yuboring:",
                    reply_markup=kb_chat(),
                )
            except Exception as e:
                logger.error(f"Sheets error uid={uid}: {e}")
                await msg.answer(f"Xatolik: {e}", reply_markup=kb_cancel())
            return

        # waiting_folder step
        if sess.step == "waiting_folder":
            folder_id = _extract_folder_id(text)
            if not folder_id:
                await msg.answer(
                    "Google Drive papka havolasi topilmadi.\n\n"
                    "Misol:\nhttps://drive.google.com/drive/folders/1ABC...",
                    reply_markup=kb_cancel(),
                )
                return
            if not sess.google_creds_json:
                await msg.answer(
                    "Google Drive uchun avval hisobingizga ulaning:",
                    reply_markup=kb_connect(),
                )
                return
            await msg.answer("Google Drive papkasi yuklanmoqda...")
            try:
                data = await fetch_folder_sheets(folder_id, sess.google_creds_json)
                if not data:
                    await msg.answer(
                        "Papkada spreadsheet topilmadi yoki ruxsat yoq.",
                        reply_markup=kb_cancel(),
                    )
                    return
                total_rows = sum(
                    len(rows) for sheets in data.values() for rows in sheets.values()
                )
                sess.folder_data = data
                sess.sheets_data = {}
                sess.excel_data = []
                sess.web_search = False
                sess.step = "in_chat"
                info = "\n".join(
                    f"  {n.split('::')[0]}: {len(s)} sheet"
                    for n, s in list(data.items())[:10]
                )
                await msg.answer(
                    f"Google Drive papkasi yuklandi!\n\n{info}\n"
                    f"{len(data)} jadval, {total_rows} qator\n\n"
                    "Savolingizni yozing yoki ovozli yuboring:",
                    reply_markup=kb_chat(),
                )
            except Exception as e:
                logger.error(f"Folder error uid={uid}: {e}")
                await msg.answer(f"Xatolik: {e}", reply_markup=kb_cancel())
            return

        # in_chat or has data
        if sess.step == "in_chat" or has_data(sess) or sess.web_search:
            if sess.step != "in_chat":
                sess.step = "in_chat"
            await _process_question(msg, sess, text)
            return

        # Google Sheets link sent from idle
        if "docs.google.com/spreadsheets" in text or re.search(
            r"spreadsheets/d/[a-zA-Z0-9\-_]+", text
        ):
            sheet_id = _extract_sheet_id(text)
            if sheet_id:
                await msg.answer("Google Sheets yuklanmoqda...")
                try:
                    if sess.google_creds_json:
                        data = await fetch_sheet_with_creds(sheet_id, sess.google_creds_json)
                    else:
                        data = await fetch_sheet_public(sheet_id)
                    if data:
                        total = sum(len(r) for r in data.values())
                        sess.sheets_data = data
                        sess.folder_data = {}
                        sess.excel_data = []
                        sess.sheet_id = sheet_id
                        sess.web_search = False
                        sess.step = "in_chat"
                        await msg.answer(
                            f"Google Sheets yuklandi ({total} qator).\nSavolingizni yuboring:",
                            reply_markup=kb_chat(),
                        )
                    else:
                        await msg.answer(
                            "Sheets yuklanmadi. Faylni ommaviy qiling.",
                            reply_markup=kb_cancel(),
                        )
                except Exception as e:
                    await msg.answer(f"Xatolik: {e}")
            return

        # Google Drive folder link from idle
        if "drive.google.com" in text and (
            "folders" in text or "open?id" in text
        ):
            if sess.google_creds_json:
                folder_id = _extract_folder_id(text)
                if folder_id:
                    sess.step = "waiting_folder"
                    await msg.answer("Yuklanmoqda...")
                    try:
                        data = await fetch_folder_sheets(folder_id, sess.google_creds_json)
                        if data:
                            total_rows = sum(
                                len(r) for s in data.values() for r in s.values()
                            )
                            sess.folder_data = data
                            sess.sheets_data = {}
                            sess.excel_data = []
                            sess.step = "in_chat"
                            await msg.answer(
                                f"Papka yuklandi! {len(data)} jadval, {total_rows} qator.\nSavol bering:",
                                reply_markup=kb_chat(),
                            )
                        else:
                            await msg.answer(
                                "Papkada malumot topilmadi.", reply_markup=kb_main()
                            )
                    except Exception as e:
                        await msg.answer(f"Xatolik: {e}")
            else:
                await msg.answer(
                    "Google Drive uchun avval hisobingizga ulaning:",
                    reply_markup=kb_connect(),
                )
            return

        # Default
        await msg.answer(
            "Nima qilish kerak?\n\n"
            "- Excel yuklash tugmasini bosing\n"
            "- Google Sheets tugmasini bosing\n"
            "- Internet qidiruv tugmasini bosing\n\n"
            "Yoki /start buyrug'ini yuboring.",
            reply_markup=kb_main(),
        )

    async def _process_question(msg: Message, sess: Session, question: str):
        uid = msg.from_user.id
        logger.info(
            f"Q&A uid={uid} web={sess.web_search} has_data={has_data(sess)} q={question[:60]!r}"
        )
        await msg.bot.send_chat_action(msg.chat.id, "typing")

        if sess.web_search and not has_data(sess):
            if not config.tavily_key:
                await msg.answer("Internet qidiruv sozlanmagan.", reply_markup=kb_main())
                return
            await msg.answer("Internetdan qidirilmoqda...")
            result = await do_web_search(question, config.tavily_key)
            await msg.answer(result, parse_mode=None, reply_markup=kb_chat())
            return

        if not has_data(sess):
            await msg.answer(
                "Malumot topilmadi. Avval Excel yuklang yoki Google Sheets ulang.",
                reply_markup=kb_main(),
            )
            return

        context = build_context(sess)
        ctx_lines = context.count("\n")
        logger.info(f"Context uid={uid}: {len(context)} chars, {ctx_lines} lines")

        # Log first 500 chars of context for debugging
        logger.info(f"Context preview uid={uid}:\n{context[:500]}")

        if not context.strip():
            await msg.answer("Malumotlar bosh korinmoqda.", reply_markup=kb_main())
            return

        if not config.grok_key:
            await msg.answer(
                f"AI sozlanmagan (GROK_API_KEY yoq).\n\nMalumotlar:\n{context[:1000]}",
                parse_mode=None,
                reply_markup=kb_chat(),
            )
            return

        await msg.answer("AI tahlil qilyapti...")
        answer = await ask_grok(question, context, config.grok_key)

        if sess.web_search and config.tavily_key:
            web_result = await do_web_search(question, config.tavily_key)
            answer = f"{answer}\n\n{web_result}"

        if len(answer) > 4000:
            parts = [answer[i : i + 4000] for i in range(0, len(answer), 4000)]
            for i, part in enumerate(parts):
                kb = kb_chat() if i == len(parts) - 1 else None
                await msg.answer(part, parse_mode=None, reply_markup=kb)
        else:
            await msg.answer(answer, parse_mode=None, reply_markup=kb_chat())


async def main():
    config = Config.from_env()
    logger.info("=" * 55)
    logger.info("OnBrain AI Bot starting...")
    logger.info(f"  GROK_API_KEY  : {'SET len=' + str(len(config.grok_key)) if config.grok_key else 'NOT SET'}")
    logger.info(f"  OPENAI_API_KEY: {'SET' if config.openai_key else 'NOT SET'}")
    logger.info(f"  TAVILY_API_KEY: {'SET' if config.tavily_key else 'NOT SET'}")
    logger.info(f"  GOOGLE_CLIENT : {'SET' if config.google_client_id else 'NOT SET'}")
    logger.info(f"  Port          : {config.port}")
    logger.info(f"  Redirect URI  : {config.redirect_uri}")
    logger.info("=" * 55)

    _init_db()

    bot = Bot(
        token=config.bot_token,
        default=DefaultBotProperties(parse_mode=None),
    )

    try:
        me = await bot.get_me()
        logger.info(f"Bot: @{me.username} (id={me.id})")
    except Exception as e:
        logger.error(f"Bot auth failed: {e}")
        raise

    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass
    await asyncio.sleep(3)

    dp = Dispatcher()
    register(dp, config, bot)

    oauth_server = OAuthServer(bot, config)
    await oauth_server.start()

    logger.info("Starting polling...")
    try:
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            relax_timeout=30.0,
            long_poll_timeout=30.0,
        )
    finally:
        await oauth_server.stop()
        await bot.session.close()


if __name__ == "__main__":
    logger.info("OnBrain AI Bot process started.")
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            logger.info("Stopped.")
            break
        except Exception as e:
            logger.error(f"Bot crashed ({type(e).__name__}): {e}")
            if "conflict" in str(e).lower():
                logger.info("Conflict - waiting 15s...")
                time.sleep(15)
            else:
                logger.info("Restarting in 5s...")
                time.sleep(5)
