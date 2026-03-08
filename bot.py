import asyncio
import io
import json
import logging
import os
import re
import secrets
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import openpyxl
import xlrd
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
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
# OpenAI lazy import - faqat kerak bo'lganda load qilish
import httpx

import gspread


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("onbrain-ai-bot")

EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]
REDIRECT_URI = "http://localhost:8080/"
MAIN_MENU_SHEETS = "📊 Google Sheets ulash"
MAIN_MENU_EXCEL = "📁 Excel fayl yuklash"
MAX_ROWS_FOR_CONTEXT = 80
MAX_COLS_FOR_CONTEXT = 15
MAX_CHARS_CONTEXT = 12000


@dataclass
class Config:
    bot_token: str
    openai_api_key: str
    google_client_id: str
    google_client_secret: str
    supabase_url: str
    supabase_anon_key: str

    @classmethod
    def from_env(cls) -> "Config":
        load_dotenv()
        required = {
            "BOT_TOKEN": os.getenv("BOT_TOKEN", "").strip(),
            "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY", "").strip(),
            "GOOGLE_CLIENT_ID": os.getenv("GOOGLE_CLIENT_ID", "").strip(),
            "GOOGLE_CLIENT_SECRET": os.getenv("GOOGLE_CLIENT_SECRET", "").strip(),
            "SUPABASE_URL": os.getenv("SUPABASE_URL", "").strip(),
            "SUPABASE_ANON_KEY": os.getenv("SUPABASE_ANON_KEY", "").strip(),
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise RuntimeError(
                "Quyidagi .env qiymatlari to'ldirilmagan: " + ", ".join(missing)
            )
        return cls(
            bot_token=required["BOT_TOKEN"],
            openai_api_key=required["OPENAI_API_KEY"],
            google_client_id=required["GOOGLE_CLIENT_ID"],
            google_client_secret=required["GOOGLE_CLIENT_SECRET"],
            supabase_url=required["SUPABASE_URL"],
            supabase_anon_key=required["SUPABASE_ANON_KEY"],
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
    excel_data: list[list[Any]] = field(default_factory=list)
    google_credentials_json: str | None = None
    pending_sheets: dict[str, str] = field(default_factory=dict)


class SessionStore:
    def __init__(self) -> None:
        self._store: dict[int, UserSession] = {}

    def get(self, telegram_id: int) -> UserSession:
        if telegram_id not in self._store:
            self._store[telegram_id] = UserSession()
        return self._store[telegram_id]


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
            
            response = httpx.post(
                f"{self.url}/rest/v1/users",
                headers=self.headers,
                json=payload,
            )
            
            # Check for status code errors
            if response.status_code >= 400:
                error_detail = response.text
                logger.error(f"Supabase error {response.status_code}: {error_detail}")
                if "phone_number" in error_detail.lower():
                    logger.critical("🚨 SUPABASE'DA phone_number USTUNI YO'Q! SUPABASE_QUICK_FIX.sql'ni run qiling!")
                raise Exception(f"Supabase error: {error_detail}")
            
            response.raise_for_status()
            logger.info(f"✅ User {telegram_id} created successfully with phone {phone_number}")
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
        self.client.table("messages").insert(payload).execute()


class GoogleOAuthService:
    def __init__(self, client_id: str, client_secret: str) -> None:
        self.client_config = {
            "web": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        }
        self.pending_flows: dict[str, dict[str, Any]] = {}

    def create_auth_url(self, telegram_id: int) -> str:
        flow = Flow.from_client_config(
            self.client_config, scopes=SCOPES, redirect_uri=REDIRECT_URI
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
        self.cleanup_stale_flows()
        return auth_url

    def cleanup_stale_flows(self) -> None:
        now = time.time()
        stale = [state for state, item in self.pending_flows.items() if now - item["created_at"] > 900]
        for state in stale:
            self.pending_flows.pop(state, None)

    def exchange_code(self, state: str, code: str) -> tuple[int, Credentials]:
        if state not in self.pending_flows:
            raise ValueError("OAuth holati topilmadi yoki muddati tugagan.")
        flow_item = self.pending_flows.pop(state)
        flow: Flow = flow_item["flow"]
        flow.fetch_token(code=code)
        telegram_id: int = flow_item["telegram_id"]
        return telegram_id, flow.credentials


def build_main_menu() -> InlineKeyboardMarkup:
    """Build main menu with Mini App button"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="💬 Chat with AI",
                web_app=WebAppInfo(url="https://on-brain.vercel.app/miniapp")
            )],
            [InlineKeyboardButton(text=MAIN_MENU_SHEETS, callback_data="sheets")],
            [InlineKeyboardButton(text=MAIN_MENU_EXCEL, callback_data="excel")],
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
        return "Ma'lumot topilmadi."
    lines = [" | ".join(row) for row in trimmed]
    text = "\n".join(lines)
    return text[:MAX_CHARS_CONTEXT]


def credentials_from_json(credentials_json: str) -> Credentials:
    data = json.loads(credentials_json)
    creds = Credentials.from_authorized_user_info(data, scopes=SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(GoogleAuthRequest())
    return creds


def list_google_sheets(credentials: Credentials) -> list[dict[str, str]]:
    if credentials.expired and credentials.refresh_token:
        credentials.refresh(GoogleAuthRequest())
    gc = gspread.authorize(credentials)
    files = gc.list_spreadsheet_files()
    sheets = [{"id": item["id"], "name": item.get("name", "Nomsiz jadval")} for item in files]
    if sheets:
        return sheets
    books = gc.openall()
    return [{"id": book.id, "name": book.title} for book in books]


def fetch_sheet_rows(credentials: Credentials, sheet_id: str) -> list[list[Any]]:
    if credentials.expired and credentials.refresh_token:
        credentials.refresh(GoogleAuthRequest())
    gc = gspread.authorize(credentials)
    workbook = gc.open_by_key(sheet_id)
    worksheet = workbook.sheet1
    return worksheet.get_all_values()


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
    raise ValueError("Noto'g'ri fayl turi.")


class AppContext:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.supabase_service = SupabaseService(
            config.supabase_url, config.supabase_anon_key
        )
        self.sessions = SessionStore()
        self.oauth_service = GoogleOAuthService(
            client_id=config.google_client_id,
            client_secret=config.google_client_secret,
        )
        self.openai_api_key = config.openai_api_key  # Store key for lazy initialization
        self.openai_client = None
        self.bot: Bot | None = None
    
    def get_openai_client(self):
        """Lazy initialization of OpenAI client"""
        if self.openai_client is None:
            try:
                from openai import AsyncOpenAI
                self.openai_client = AsyncOpenAI(api_key=self.openai_api_key)
            except Exception as exc:
                logger.error(f"OpenAI client initialization error: {exc}")
                return None
        return self.openai_client

    async def handle_oauth_callback(self, state: str, code: str) -> str:
        telegram_id, credentials = await asyncio.to_thread(
            self.oauth_service.exchange_code, state, code
        )
        session = self.sessions.get(telegram_id)
        session.google_credentials_json = credentials.to_json()
        session.step = "ready"
        try:
            sheets = await asyncio.to_thread(list_google_sheets, credentials)
        except Exception as exc:
            logger.exception("Google Sheets ro'yxatini olishda xato: %s", exc)
            if self.bot:
                await self.bot.send_message(
                    telegram_id,
                    "Google Sheets ro'yxatini olishda xatolik yuz berdi. Iltimos, qayta urinib ko'ring.",
                )
            return "Ruxsat olindi, lekin Sheets ro'yxatini olishda xatolik yuz berdi."

        if not sheets:
            if self.bot:
                await self.bot.send_message(
                    telegram_id,
                    "Sizning Google Drive hisobingizda Google Sheets topilmadi.",
                )
            return "Ruxsat olindi, lekin Google Sheets topilmadi."

        token = secrets.token_hex(4)
        session.pending_sheets = {
            f"{token}:{item['id']}": item["name"] for item in sheets[:100]
        }
        buttons = [
            [
                InlineKeyboardButton(
                    text=name[:60],
                    callback_data=f"sheet:{key}",
                )
            ]
            for key, name in session.pending_sheets.items()
        ]
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        if self.bot:
            await self.bot.send_message(
                telegram_id,
                "Google Sheets muvaffaqiyatli ulandi. Quyidagi jadvaldan birini tanlang:",
                reply_markup=keyboard,
            )
        return "Muvaffaqiyatli. Telegram botga qayting va jadvalni tanlang."


class OAuthServer:
    def __init__(self, context: AppContext) -> None:
        self.context = context
        self.runner: web.AppRunner | None = None
        self.site: web.TCPSite | None = None

    async def start(self) -> None:
        app = web.Application()
        app.add_routes([web.get("/", self._callback)])
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, host="0.0.0.0", port=8080)
        await self.site.start()
        logger.info("OAuth callback server ishga tushdi: http://localhost:8080/")

    async def stop(self) -> None:
        if self.runner:
            await self.runner.cleanup()

    async def _callback(self, request: web.Request) -> web.Response:
        state = request.query.get("state")
        code = request.query.get("code")
        error = request.query.get("error")
        if error:
            return web.Response(
                text=f"OAuth xatoligi: {error}. Telegram botga qayting va qayta urinib ko'ring."
            )
        if not state or not code:
            return web.Response(text="Noto'g'ri OAuth so'rovi.")
        try:
            msg = await self.context.handle_oauth_callback(state, code)
            return web.Response(text=msg)
        except Exception as exc:
            logger.exception("OAuth callback xatosi: %s", exc)
            return web.Response(
                text="OAuth jarayonida xatolik yuz berdi. Telegram botga qaytib qayta urinib ko'ring."
            )


def register_handlers(dp: Dispatcher, ctx: AppContext) -> None:
    @dp.message(CommandStart())
    async def start_handler(message: Message) -> None:
        """Handle /start command"""
        telegram_id = message.from_user.id
        session = ctx.sessions.get(telegram_id)
        
        logger.info(f"📱 /start - User {telegram_id}")
        
        try:
            # Check if user already registered
            user = await ctx.supabase_service.get_user_by_telegram(telegram_id)
            
            if user:
                # User already registered - show main menu
                first_name = user.get("first_name", "")
                last_name = user.get("last_name", "")
                full_name = f"{first_name} {last_name}".strip()
                
                session.full_name = full_name
                session.email = user.get("email")
                session.step = "ready"
                
                # Check for active sheet integration
                active = await ctx.supabase_service.get_active_integration(telegram_id)
                if active:
                    session.sheet_id = active.get("sheet_id")
                    session.sheet_name = active.get("sheet_name")
                
                logger.info(f"✅ User {telegram_id} already registered: {full_name}")
                
                await message.answer(
                    f"👋 Xush kelibsiz, {full_name}!\n\n"
                    "Menyudan kerakli bo'limni tanlang:",
                    reply_markup=build_main_menu(),
                )
                return
            
            # New user - start registration
            logger.info(f"🆕 New user {telegram_id} - starting registration")
            session.step = "waiting_first_name"
            
            await message.answer(
                "Assalomu alaykum! 👋 OnBrain AI botiga xush kelibsiz.\n\n"
                "✍️ <b>Ro'yxatdan o'tish uchun, iltimos, ismingizni yuboring:</b>",
                parse_mode="HTML",
            )
            
        except Exception as exc:
            logger.exception(f"❌ /start handler error for user {telegram_id}: {exc}")
            await message.answer(
                "❌ Xatolik yuz berdi. Iltimos, /start buyrug'ini qayta yuboring."
            )

    @dp.message(F.text == MAIN_MENU_SHEETS)
    async def connect_sheets_handler(message: Message) -> None:
        telegram_id = message.from_user.id
        session = ctx.sessions.get(telegram_id)
        if session.step in {"waiting_name", "waiting_email"}:
            await message.answer("Avval ro'yxatdan o'tishni yakunlang. /start buyrug'ini yuboring.")
            return
        try:
            auth_url = ctx.oauth_service.create_auth_url(telegram_id)
            await message.answer(
                "Google Sheets ulash uchun quyidagi havolani oching:\n"
                f"{auth_url}\n\n"
                "Ruxsat berganingizdan keyin bot avtomatik ravishda jadvallarni yuboradi."
            )
        except Exception as exc:
            logger.exception("Google OAuth link yaratishda xato: %s", exc)
            await message.answer(
                "Google ulanish havolasini yaratishda xatolik yuz berdi. Iltimos, qayta urinib ko'ring."
            )

    @dp.message(F.text == MAIN_MENU_EXCEL)
    async def upload_excel_menu_handler(message: Message) -> None:
        telegram_id = message.from_user.id
        session = ctx.sessions.get(telegram_id)
        if session.step in {"waiting_name", "waiting_email"}:
            await message.answer("Avval ro'yxatdan o'tishni yakunlang. /start buyrug'ini yuboring.")
            return
        session.step = "waiting_excel"
        await message.answer(
            "Excel fayl yuboring (.xlsx yoki .xls).\n"
            "Fayl qabul qilingach, savollaringizga ma'lumot asosida javob beraman."
        )

    @dp.message(F.text)
    async def text_handler(message: Message) -> None:
        """Unified text message handler for registration flow"""
        telegram_id = message.from_user.id
        session = ctx.sessions.get(telegram_id)
        
        # IMPORTANT: Check session.step and handle accordingly
        
        # ====== WAITING FOR FIRST NAME ======
        if session.step == "waiting_first_name":
            try:
                first_name = message.text.strip()
                if not first_name or len(first_name) < 2:
                    await message.answer("❌ Iltimos, 2 ta harfdan ko'proq ismingiz bilan qayta urinib ko'ring.")
                    return
                
                session.full_name = first_name
                session.step = "waiting_last_name"
                
                logger.info(f"✅ User {telegram_id} entered first name: {first_name}")
                
                await message.answer(
                    "👤 <b>Endi familiyangizni yuboring:</b>",
                    parse_mode="HTML"
                )
                
            except Exception as exc:
                logger.exception(f"❌ Ism kiritishda xato: {exc}")
                await message.answer("❌ Xatolik yuz berdi. Iltimos, qayta urinib ko'ring.")
            return  # IMPORTANT: Return to prevent further processing
        
        # ====== WAITING FOR LAST NAME ======
        if session.step == "waiting_last_name":
            try:
                last_name = message.text.strip()
                if not last_name or len(last_name) < 2:
                    await message.answer("❌ Iltimos, 2 ta harfdan ko'proq familiyangiz bilan qayta urinib ko'ring.")
                    return
                
                session.full_name = f"{session.full_name} {last_name}"
                session.step = "waiting_contact"
                
                logger.info(f"✅ User {telegram_id} entered last name: {last_name}")
                
                await message.answer(
                    "📱 <b>Endi kontaktingizni ulashing:</b>\n\n"
                    "Pastdagi tugmani bosib, o'zingizning telefon raqamingizni baham ko'ling.",
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
                logger.exception(f"❌ Familiya kiritishda xato: {exc}")
                await message.answer("❌ Xatolik yuz berdi. Iltimos, qayta urinib ko'ring.")
            return  # IMPORTANT: Return to prevent further processing
        
        # ====== OTHER TEXT MESSAGES (MENU HANDLING) ======
        # Continue with existing menu handlers
        # (This handler will now delegate to other handlers if step != waiting_first_name/last_name)

    @dp.message(F.contact)
    async def contact_handler(message: Message) -> None:
        """Handle contact sharing during registration"""
        telegram_id = message.from_user.id
        session = ctx.sessions.get(telegram_id)
        
        logger.info(f"📱 Contact received from user {telegram_id}, step={session.step}")
        
        if session.step != "waiting_contact":
            logger.warning(f"⚠️ User {telegram_id} sent contact at wrong step: {session.step}")
            await message.answer(
                "❌ Kontakt tugmasi bu vaqtda mumkin emas. Iltimos, /start buyrug'ini yuboring.",
                reply_markup=ReplyKeyboardRemove(),
            )
            return
        
        try:
            # Extract phone number from contact
            phone_number = message.contact.phone_number
            
            if not phone_number:
                logger.warning(f"⚠️ User {telegram_id} sent contact without phone number")
                await message.answer(
                    "❌ Telefon raqami topilmadi. Iltimos, kontaktni qayta ulashga urinib ko'ring.",
                    reply_markup=ReplyKeyboardMarkup(
                        keyboard=[
                            [KeyboardButton(text="📱 Kontaktni ulashish", request_contact=True)],
                        ],
                        resize_keyboard=True,
                        one_time_keyboard=True,
                    ),
                )
                return
            
            session.phone_number = phone_number
            user_email = f"{telegram_id}@telegram.local"
            session.email = user_email
            
            logger.info(f"✅ Creating user: {telegram_id}, name={session.full_name}, phone={phone_number}")
            
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
                    "📋 Menyudan kerakli bo'limni tanlang:",
                    reply_markup=build_main_menu(),
                )
            else:
                raise Exception("User creation failed - result is False")
            
        except Exception as exc:
            logger.exception(f"❌ Contact handler error for {telegram_id}: {exc}")
            session.step = "idle"
            await message.answer(
                f"❌ Ro'yxatdan o'tishda xatolik yuz berdi:\n\n{str(exc)}\n\n"
                "Iltimos, /start buyrug'ini yuboring.",
                reply_markup=ReplyKeyboardRemove(),
            )

    @dp.message(F.text == "Bekor qilish")
    async def cancel_registration_handler(message: Message) -> None:
        """Handle cancel during registration"""
        telegram_id = message.from_user.id
        session = ctx.sessions.get(telegram_id)
        
        if session.step in {"waiting_first_name", "waiting_last_name", "waiting_contact"}:
            session.step = "idle"
            session.full_name = None
            session.phone_number = None
            await message.answer(
                "Ro'yxatdan o'tish bekor qilindi.\n\n"
                "Agar qayta ro'yxatdan o'tmoqchi bo'lsangiz, /start buyrug'ini yuboring.",
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
            await callback.answer("Jadval havolasi muddati tugagan. Qayta ulab ko'ring.", show_alert=True)
            return
        selected_sheet_name = session.pending_sheets[key]
        selected_sheet_id = key.split(":", 1)[1]
        if not session.google_credentials_json:
            await callback.answer("Google ruxsati topilmadi. Qayta ulab ko'ring.", show_alert=True)
            return
        try:
            credentials = await asyncio.to_thread(
                credentials_from_json, session.google_credentials_json
            )
            rows = await asyncio.to_thread(fetch_sheet_rows, credentials, selected_sheet_id)
            session.sheet_id = selected_sheet_id
            session.sheet_name = selected_sheet_name
            session.sheet_data = rows
            session.excel_data = []
            session.step = "ready"
            await ctx.supabase_service.save_integration(
                telegram_id, selected_sheet_id, selected_sheet_name
            )
            await callback.message.edit_text(
                f"✅ Ulandi: {selected_sheet_name}\n"
                "Endi savolingizni yozing, jadval ma'lumotlari asosida javob beraman."
            )
            await callback.answer("Google Sheet muvaffaqiyatli ulandi.")
        except Exception as exc:
            logger.exception("Sheet tanlashda xato: %s", exc)
            await callback.answer(
                "Jadvalni ulashda xatolik yuz berdi. Iltimos, qayta urinib ko'ring.",
                show_alert=True,
            )

    @dp.message(F.document)
    async def document_handler(message: Message, bot: Bot) -> None:
        telegram_id = message.from_user.id
        session = ctx.sessions.get(telegram_id)
        if session.step != "waiting_excel":
            await message.answer(
                "Agar Excel yuklamoqchi bo'lsangiz, avval menyudan \"📁 Excel fayl yuklash\" tugmasini bosing."
            )
            return

        doc = message.document
        file_name = doc.file_name or ""
        if not file_name.lower().endswith((".xlsx", ".xls")):
            await message.answer(
                "Noto'g'ri fayl turi. Iltimos, faqat .xlsx yoki .xls formatidagi fayl yuboring."
            )
            return

        try:
            telegram_file = await bot.get_file(doc.file_id)
            buffer = io.BytesIO()
            await bot.download_file(telegram_file.file_path, destination=buffer)
            excel_rows = await asyncio.to_thread(parse_excel_bytes, file_name, buffer.getvalue())
            if not excel_rows:
                await message.answer("Fayl bo'sh ko'rinmoqda. Iltimos, boshqa fayl yuboring.")
                return
            session.excel_data = excel_rows
            session.sheet_data = []
            session.sheet_id = None
            session.sheet_name = file_name
            session.step = "ready"
            await message.answer(
                f"✅ Excel fayl muvaffaqiyatli yuklandi: {file_name}\n"
                "Endi savolingizni yozing."
            )
        except Exception as exc:
            logger.exception("Excel faylni qayta ishlash xatosi: %s", exc)
            await message.answer(
                "Excel faylni o'qishda xatolik yuz berdi. Fayl formatini tekshirib, qayta yuboring."
            )

    @dp.message(F.text)
    async def text_router(message: Message) -> None:
        telegram_id = message.from_user.id
        session = ctx.sessions.get(telegram_id)
        text = (message.text or "").strip()
        if not text:
            return

        if text in {MAIN_MENU_SHEETS, MAIN_MENU_EXCEL}:
            return

        if session.step == "waiting_magic_link":
            # User should click the magic link button instead
            await message.answer("Ro'yxatdan o'tish uchun `/start` buyrug'ini yuboring.")
            return

        if session.step != "ready":
            await message.answer("Jarayonni boshlash uchun /start buyrug'ini yuboring.")
            return

        context_rows = session.sheet_data or session.excel_data
        if not context_rows:
            await message.answer(
                "Avval Google Sheets ulang yoki Excel fayl yuklang, keyin savol bering."
            )
            return

        await message.answer("Savolingiz qabul qilindi, javob tayyorlanmoqda...")
        context_text = table_to_text(context_rows)
        prompt = (
            "Siz OnBrain AI yordamchisisiz. Foydalanuvchiga faqat o'zbek tilida, aniq va foydali javob bering.\n"
            "Javob faqat berilgan jadval ma'lumotlariga tayansin.\n\n"
            f"Jadval ma'lumoti:\n{context_text}\n\n"
            f"Foydalanuvchi savoli:\n{text}"
        )
        try:
            response = await ctx.openai_client.responses.create(
                model="gpt-4o",
                input=[
                    {"role": "system", "content": "Siz professional data yordamchisiz."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
            )
            answer = (response.output_text or "").strip()
            if not answer:
                answer = "Kechirasiz, hozircha aniq javob hosil bo'lmadi. Iltimos, savolni boshqacha yuboring."
            await message.answer(answer)
            await ctx.supabase_service.save_message(
                telegram_id=telegram_id,
                question=text,
                answer=answer,
            )
        except Exception as exc:
            # Handle any OpenAI or other errors
            error_msg = str(exc).lower()
            if "rate" in error_msg or "too many" in error_msg:
                await message.answer(
                    "So'rovlar soni juda ko'p. Iltimos, birozdan so'ng qayta urinib ko'ring."
                )
            elif "connection" in error_msg or "network" in error_msg:
                await message.answer(
                    "Tarmoq bilan bog'liq xatolik yuz berdi. Internetni tekshirib, qayta urinib ko'ring."
                )
            else:
                await message.answer(
                    "AI xizmatida vaqtinchalik xatolik yuz berdi. Iltimos, keyinroq urinib ko'ring."
                )
                logger.exception("AI javobida xato: %s", exc)


async def main() -> None:
    config = Config.from_env()
    context = AppContext(config)
    
    # Create bot with default settings
    bot = Bot(
        token=config.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    context.bot = bot
    
    # Test the bot token first
    try:
        me = await bot.get_me()
        logger.info(f"Bot authenticated: {me.first_name} (@{me.username})")
    except Exception as e:
        logger.error(f"Failed to authenticate bot: {e}")
        raise
    
    dp = Dispatcher()
    register_handlers(dp, context)

    # OAuth server optional - only needed if using Google OAuth
    # oauth_server = OAuthServer(context)
    # await oauth_server.start()
    
    logger.info("Starting bot polling loop...")
    polling_task = None
    try:
        # Start polling - aiogram handles reconnection automatically
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
        # oauth_server.stop() - not needed, OAuth server disabled
        try:
            await bot.session.close()
        except:
            pass


if __name__ == "__main__":
    logger.info("🚀 OnBrain AI Bot Starting...")
    while True:
        try:
            asyncio.run(main())
            logger.info("Bot run completed normally")
        except KeyboardInterrupt:
            logger.info("Bot stopped by user (Ctrl+C)")
            break
        except asyncio.CancelledError:
            logger.warning("Bot was cancelled, restarting...")
            time.sleep(5)
        except Exception as e:
            error_name = type(e).__name__
            logger.error(f"Bot error ({error_name}): {e}")
            logger.info("Restarting bot in 5 seconds...")
            time.sleep(5)
