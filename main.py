"""
OnBrain AI - Telegram Mini App Backend v2.0
Full-featured: Excel upload, Google Sheets, AI chat with data context
"""

import os
import io
import re
import csv
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import httpx
from fastapi import FastAPI, HTTPException, Request, UploadFile, File, Form
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Env vars ──────────────────────────────────────────────
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
GROK_API_KEY   = os.getenv("GROK_API_KEY", "")
SUPABASE_URL   = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY   = os.getenv("SUPABASE_ANON_KEY", "")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")

# Use Grok if no OpenAI key (xAI API is OpenAI-compatible)
if not OPENAI_API_KEY and GROK_API_KEY:
    AI_KEY      = GROK_API_KEY
    AI_BASE_URL = "https://api.x.ai/v1"
    AI_MODEL    = "grok-3-latest"
else:
    AI_KEY      = OPENAI_API_KEY
    AI_BASE_URL = "https://api.openai.com/v1"
    AI_MODEL    = "gpt-4o"

# ── In-memory session store ───────────────────────────────
# {telegram_id: {"sources": [...], "lang": "uz", "web_search": False}}
_sessions: Dict[int, Dict] = {}

# ── App ───────────────────────────────────────────────────
app = FastAPI(title="OnBrain AI", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Lazy Supabase ─────────────────────────────────────────
_supabase = None

def get_supabase():
    global _supabase
    if _supabase is None and SUPABASE_URL and SUPABASE_KEY:
        try:
            from supabase import create_client
            _supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            logger.info("Supabase connected")
        except Exception as e:
            logger.warning(f"Supabase init failed: {e}")
            _supabase = False
    return _supabase if _supabase else None

# ── Session helpers ───────────────────────────────────────
def session_get(uid: int) -> Dict:
    return _sessions.setdefault(uid, {"sources": [], "lang": "uz", "web_search": False})

def session_save(uid: int, data: Dict):
    _sessions[uid] = data

# ── Build AI context from sources ─────────────────────────
def build_context(sources: List[Dict]) -> str:
    parts = []
    for s in sources:
        if s.get("disabled"):
            continue
        name = s.get("name", "Ma'lumot")
        rows = s.get("preview", [])
        if not rows:
            continue
        header = list(rows[0].keys())
        lines = [" | ".join(str(c) for c in header)]
        for row in rows[:300]:
            lines.append(" | ".join(str(row.get(c, "")) for c in header))
        parts.append(f"### {name}\n" + "\n".join(lines))
    return "\n\n".join(parts)

# ── Google Sheets URL → CSV URL ───────────────────────────
def sheets_to_csv_url(url: str) -> Optional[str]:
    m = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)', url)
    if not m:
        return None
    sid = m.group(1)
    gid_m = re.search(r'[?&#]gid=(\d+)', url)
    gid = gid_m.group(1) if gid_m else "0"
    return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv&gid={gid}"

# ═════════════════ ENDPOINTS ════════════════════════════

@app.get("/health")
@app.get("/api/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

@app.get("/")
async def root():
    return {"message": "OnBrain AI v2.0", "status": "running"}

# ── Serve Mini App HTML ───────────────────────────────────
@app.get("/miniapp")
async def get_miniapp():
    for p in [
        os.path.join(os.path.dirname(__file__), "static", "index.html"),
        os.path.join(os.getcwd(), "static", "index.html"),
        "static/index.html",
    ]:
        if os.path.exists(p):
            with open(p, encoding="utf-8") as f:
                return HTMLResponse(f.read())
    raise HTTPException(500, "index.html not found")

# ── Register / Update user ────────────────────────────────
@app.post("/api/register")
async def register(request: Request):
    data = await request.json()
    uid = int(data.get("telegram_id", 0))
    if not uid:
        raise HTTPException(400, "telegram_id required")

    sess = session_get(uid)
    sess["lang"] = data.get("lang", sess.get("lang", "uz"))
    sess["name"] = data.get("full_name", "User")
    session_save(uid, sess)

    sb = get_supabase()
    if sb:
        try:
            ex = sb.table("users").select("id").eq("telegram_id", uid).execute()
            payload = {
                "full_name": data.get("full_name", ""),
                "username": data.get("username", ""),
                "lang": sess["lang"],
                "updated_at": datetime.now().isoformat()
            }
            if ex.data:
                sb.table("users").update(payload).eq("telegram_id", uid).execute()
            else:
                payload.update({"telegram_id": uid, "created_at": datetime.now().isoformat()})
                sb.table("users").insert(payload).execute()
        except Exception as e:
            logger.warning(f"Supabase register: {e}")

    return {"success": True}

# ── Get user ──────────────────────────────────────────────
@app.get("/api/user/{telegram_id}")
async def get_user(telegram_id: int):
    sess = session_get(telegram_id)
    return {"success": True, "user": {"telegram_id": telegram_id, "lang": sess.get("lang", "uz")}}

# ── Get session (sources + settings) ─────────────────────
@app.get("/api/session/{telegram_id}")
async def get_session(telegram_id: int):
    sess = session_get(telegram_id)
    # Strip heavy preview data before sending
    light_sources = [{k: v for k, v in s.items() if k not in ("preview", "csv_url")}
                     for s in sess.get("sources", [])]
    return {
        "success": True,
        "sources": light_sources,
        "lang": sess.get("lang", "uz"),
        "web_search": sess.get("web_search", False)
    }

# ── Set language ──────────────────────────────────────────
@app.post("/api/lang")
async def set_lang(request: Request):
    data = await request.json()
    uid = int(data.get("telegram_id", 0))
    if uid:
        sess = session_get(uid)
        sess["lang"] = data.get("lang", "uz")
        session_save(uid, sess)
    return {"success": True}

# ── Web search toggle ─────────────────────────────────────
@app.post("/api/web_search")
async def toggle_web_search(request: Request):
    data = await request.json()
    uid = int(data.get("telegram_id", 0))
    enabled = bool(data.get("enabled", False))
    if uid:
        sess = session_get(uid)
        sess["web_search"] = enabled
        session_save(uid, sess)
    return {"success": True, "enabled": enabled}

# ── Clear all sources ─────────────────────────────────────
@app.post("/api/clear")
async def clear_sources(request: Request):
    data = await request.json()
    uid = int(data.get("telegram_id", 0))
    if uid:
        sess = session_get(uid)
        sess["sources"] = []
        session_save(uid, sess)
    return {"success": True}

# ── Excel Upload ──────────────────────────────────────────
@app.post("/api/upload")
async def upload_excel(
    telegram_id: int = Form(...),
    file: UploadFile = File(...)
):
    fname = file.filename or ""
    if not re.search(r'\.(xlsx|xls|xlsm)$', fname, re.I):
        raise HTTPException(400, "Faqat .xlsx, .xls, .xlsm fayllari qabul qilinadi")

    try:
        import openpyxl
    except ImportError:
        raise HTTPException(500, "Server xatosi: openpyxl kutubxonasi yo'q")

    try:
        content = await file.read()
        wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)

        all_rows: List[Dict] = []
        total_cols = 0

        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            raw = list(ws.iter_rows(values_only=True))
            if len(raw) < 2:
                continue
            header = [str(c).strip() if c is not None else f"Col{i+1}" for i, c in enumerate(raw[0])]
            total_cols = max(total_cols, len(header))
            for row in raw[1:]:
                if any(c is not None for c in row):
                    all_rows.append({
                        header[i]: (row[i] if i < len(row) else None)
                        for i in range(len(header))
                    })
        wb.close()

        if not all_rows:
            raise HTTPException(400, "Fayl bo'sh yoki o'qib bo'lmadi")

        source_name = os.path.splitext(fname)[0]
        sess = session_get(telegram_id)
        sess["sources"] = [s for s in sess["sources"] if s.get("type") != "excel"]
        sess["sources"].append({
            "name": source_name,
            "type": "excel",
            "rows": len(all_rows),
            "cols": total_cols,
            "preview": all_rows,
            "disabled": False
        })
        session_save(telegram_id, sess)

        logger.info(f"Excel: {source_name} ({len(all_rows)} rows) for uid={telegram_id}")
        return {
            "success": True,
            "source_name": source_name,
            "rows": len(all_rows),
            "cols": total_cols
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Excel upload error: {e}")
        raise HTTPException(500, f"Fayl o'qishda xato: {str(e)}")

# ── Google Sheets Connect ─────────────────────────────────
@app.post("/api/sheets")
async def connect_sheets(request: Request):
    data = await request.json()
    uid = int(data.get("telegram_id", 0))
    url = data.get("url", "").strip()

    if not url:
        raise HTTPException(400, "URL kiriting")

    csv_url = sheets_to_csv_url(url)
    if not csv_url:
        raise HTTPException(400, "Noto'g'ri Google Sheets URL. Havola /spreadsheets/d/... ko'rinishida bo'lishi kerak")

    try:
        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
            resp = await client.get(csv_url)

        if resp.status_code == 403:
            raise HTTPException(400,
                "Kirish rad etildi. Faylni 'Hamma ko'rishi mumkin' qilib ulashing: "
                "Share → Anyone with the link → Viewer → Copy link")
        if resp.status_code != 200:
            raise HTTPException(400, f"Google Sheets ochilmadi (HTTP {resp.status_code})")

        lines = list(csv.reader(io.StringIO(resp.text)))
        if len(lines) < 2:
            raise HTTPException(400, "Jadval bo'sh")

        header = [c.strip() or f"Col{i+1}" for i, c in enumerate(lines[0])]
        rows = []
        for line in lines[1:]:
            if any(c.strip() for c in line):
                rows.append({header[i]: (line[i] if i < len(line) else "") for i in range(len(header))})

        if not rows:
            raise HTTPException(400, "Jadvaldagi ma'lumotlar topilmadi")

        source_name = f"Google Sheets ({len(rows)} qator)"

        sess = session_get(uid)
        sess["sources"] = [s for s in sess["sources"] if s.get("url") != url]
        sess["sources"].append({
            "name": source_name,
            "type": "google_sheets",
            "rows": len(rows),
            "tabs": header[:5],
            "url": url,
            "csv_url": csv_url,
            "preview": rows,
            "disabled": False
        })
        session_save(uid, sess)

        logger.info(f"Sheets: {source_name} for uid={uid}")
        return {
            "success": True,
            "source_name": source_name,
            "rows": len(rows),
            "tabs": header[:5]
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Sheets error: {e}")
        raise HTTPException(500, f"Google Sheets ulanishda xato: {str(e)}")

# ── Tavily web search ─────────────────────────────────────
async def tavily_search(query: str) -> str:
    if not TAVILY_API_KEY:
        return ""
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                "https://api.tavily.com/search",
                json={"api_key": TAVILY_API_KEY, "query": query, "max_results": 5, "search_depth": "basic"},
                headers={"Content-Type": "application/json"}
            )
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            snippets = [f"- {r.get('title','')}: {r.get('content','')[:300]}" for r in results]
            return "\n".join(snippets)
    except Exception as e:
        logger.warning(f"Tavily search error: {e}")
    return ""

# ── AI Chat ───────────────────────────────────────────────
@app.post("/api/chat")
async def chat(request: Request):
    data = await request.json()
    uid = int(data.get("telegram_id", 0))
    message = data.get("message", "").strip()

    if not message:
        raise HTTPException(400, "Savol bo'sh")
    if not AI_KEY:
        raise HTTPException(500, "AI API key sozlanmagan (server .env faylini tekshiring)")

    sess = session_get(uid)
    context = build_context(sess.get("sources", []))
    lang = sess.get("lang", "uz")
    web = sess.get("web_search", False)

    lang_map = {"uz": "O'zbek tilida", "ru": "Русском языке", "en": "English"}
    lang_str = lang_map.get(lang, "O'zbek tilida")

    # Web search
    web_context = ""
    if web and TAVILY_API_KEY:
        web_context = await tavily_search(message)

    if context:
        system = f"""Siz OnBrain AI — ma'lumot tahlil yordamchisiz.
Javobni {lang_str} yozing.

Foydalanuvchi ma'lumot manbalari:

{context}
{'--- Internet qidiruv natijalari ---' + chr(10) + web_context if web_context else ''}

Savollarga yuqoridagi ma'lumotlar asosida javob bering. Raqamlar, foizlar, jadval ko'rinishida aniq javob yozing."""
    else:
        system = f"""Siz OnBrain AI — aqlli yordamchi.
Javobni {lang_str} yozing.
{'--- Internet qidiruv natijalari ---' + chr(10) + web_context + chr(10) if web_context else ''}
Savollarga qisqa, aniq va foydali javob bering."""

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"{AI_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {AI_KEY}", "Content-Type": "application/json"},
                json={
                    "model": AI_MODEL,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": message}
                    ],
                    "temperature": 0.7,
                    "max_tokens": 1500
                }
            )

        if resp.status_code != 200:
            err_detail = resp.json().get("error", {}).get("message", resp.text[:300])
            raise HTTPException(500, f"OpenAI xato: {err_detail}")

        answer = resp.json()["choices"][0]["message"]["content"]

        # Save to Supabase (optional)
        sb = get_supabase()
        if sb:
            try:
                sb.table("messages").insert({
                    "telegram_id": uid,
                    "question": message,
                    "answer": answer,
                    "created_at": datetime.now().isoformat()
                }).execute()
            except Exception as e:
                logger.warning(f"Supabase save: {e}")

        return {"success": True, "answer": answer}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Chat error: {e}")
        raise HTTPException(500, f"AI xato: {str(e)}")

# ── Error handler ─────────────────────────────────────────
@app.exception_handler(HTTPException)
async def http_exc(req: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"success": False, "detail": exc.detail}
    )

# ── Startup ───────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    logger.info("🚀 OnBrain AI v2.0 starting...")
    logger.info(f"   AI model: {AI_MODEL} @ {AI_BASE_URL}")
    logger.info(f"   AI key  : {'✅ configured' if AI_KEY else '❌ NOT SET'}")
    logger.info(f"   Supabase: {'✅ configured' if SUPABASE_URL else '⚠️  optional/not set'}")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
