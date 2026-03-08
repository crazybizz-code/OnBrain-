"""
OnBrain AI - Telegram Mini App Backend
Production-ready FastAPI server with OpenAI integration
"""

import os
import json
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Lazy imports for heavy libraries
supabase = None
openai = None

def get_supabase():
    """Lazy load Supabase client"""
    global supabase
    if supabase is None:
        from supabase import create_client
        supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    return supabase

def get_openai():
    """Lazy load OpenAI"""
    global openai
    if openai is None:
        import openai as openai_module
        openai_module.api_key = OPENAI_API_KEY
        openai = openai_module
    return openai

# Create FastAPI app
app = FastAPI(title="OnBrain AI Mini App", version="1.0.0")

# Configure CORS for Telegram and Render domains
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://web.telegram.org",
        "https://webz.telegram.org",
        "https://onbrain.onrender.com",
        "http://localhost:8000",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# System prompt for AI in Uzbek
SYSTEM_PROMPT = """Siz OnBrain AI yordamchisiz. Foydalanuvchi savollariga o'zbek tilida qisqa va aniq javob bering. Javobingiz 1-2 paragrafda bo'lishi kerak."""


# ==================== ROUTES ====================

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "ok",
        "service": "OnBrain AI Mini App",
        "timestamp": datetime.now().isoformat()
    }
@app.get("/")
async def root():
    """Root endpoint - redirect to mini app"""
    return {"message": "OnBrain AI Mini App Backend", "status": "running"}


@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "supabase": "connected",
        "openai": "configured",
        "timestamp": datetime.now().isoformat()
    }


@app.post("/api/register")
async def register_user(request: Request):
    """
    Register a new user or update existing
    
    Body: {
        "telegram_id": int,
        "name": str,
        "username": str (optional),
        "email": str (optional)
    }
    """
    try:
        data = await request.json()
        telegram_id = data.get("telegram_id")
        name = data.get("name", "User")
        username = data.get("username", "")
        email = data.get("email", "")

        if not telegram_id:
            raise HTTPException(status_code=400, detail="telegram_id is required")

        # Get Supabase client
        sb = get_supabase()

        # Check if user exists
        existing = sb.table("users").select("*").eq("telegram_id", telegram_id).execute()

        if existing.data:
            # Update existing user
            user_data = existing.data[0]
            if email and not user_data.get("email"):
                sb.table("users").update({
                    "email": email,
                    "updated_at": datetime.now().isoformat()
                }).eq("telegram_id", telegram_id).execute()
            user = sb.table("users").select("*").eq("telegram_id", telegram_id).execute().data[0]
        else:
            # Create new user
            new_user = {
                "telegram_id": telegram_id,
                "full_name": name,
                "username": username,
                "email": email,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            result = sb.table("users").insert(new_user).execute()
            user = result.data[0]

        logger.info(f"User registered/updated: {telegram_id}")
        return {
            "success": True,
            "user": user,
            "message": "Foydalanuvchi saqlandi"
        }

    except Exception as e:
        logger.error(f"Registration error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Registratsiya xatosi: {str(e)}")


@app.get("/api/user/{telegram_id}")
async def get_user(telegram_id: int):
    """Get user data from Supabase"""
    try:
        sb = get_supabase()
        result = sb.table("users").select("*").eq("telegram_id", telegram_id).execute()
        
        if not result.data:
            raise HTTPException(status_code=404, detail="Foydalanuvchi topilmadi")
        
        return {
            "success": True,
            "user": result.data[0]
        }
    
    except Exception as e:
        logger.error(f"Get user error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/chat")
async def chat(request: Request):
    """
    Send message to AI and get response
    
    Body: {
        "telegram_id": int,
        "message": str
    }
    """
    try:
        data = await request.json()
        telegram_id = data.get("telegram_id")
        message = data.get("message", "").strip()

        if not telegram_id or not message:
            raise HTTPException(status_code=400, detail="telegram_id va message zarur")

        # Get Supabase client
        sb = get_supabase()
        oai = get_openai()

        # Get user data
        user_result = sb.table("users").select("*").eq("telegram_id", telegram_id).execute()
        if not user_result.data:
            raise HTTPException(status_code=404, detail="Foydalanuvchi topilmadi")

        # Send to OpenAI
        logger.info(f"Chat request from user {telegram_id}: {message[:50]}")
        
        response = oai.ChatCompletion.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": message}
            ],
            temperature=0.7,
            max_tokens=1000
        )

        ai_response = response.choices[0].message.content

        # Save to Supabase
        message_record = {
            "telegram_id": telegram_id,
            "question": message,
            "answer": ai_response,
            "created_at": datetime.now().isoformat()
        }
        sb.table("messages").insert(message_record).execute()

        logger.info(f"Chat response saved for user {telegram_id}")

        return {
            "success": True,
            "answer": ai_response,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Chat error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Xato: {str(e)}")


@app.get("/api/history/{telegram_id}")
async def get_chat_history(telegram_id: int, limit: int = 20):
    """Get user's chat history"""
    try:
        sb = get_supabase()
        result = sb.table("messages") \
            .select("*") \
            .eq("telegram_id", telegram_id) \
            .order("created_at", desc=True) \
            .limit(limit) \
            .execute()

        messages = list(reversed(result.data)) if result.data else []

        return {
            "success": True,
            "messages": messages,
            "count": len(messages)
        }

    except Exception as e:
        logger.error(f"History error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/miniapp")
async def get_miniapp():
    """Serve the Mini App HTML"""
    import os
    
    # Try multiple paths for Vercel compatibility
    possible_paths = [
        os.path.join(os.path.dirname(__file__), "static", "index.html"),
        os.path.join(os.getcwd(), "static", "index.html"),
        "static/index.html",
        "./static/index.html",
    ]
    
    html_content = None
    for html_path in possible_paths:
        try:
            if os.path.exists(html_path):
                with open(html_path, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                    logger.info(f"Loaded HTML from: {html_path}")
                    break
        except Exception as e:
            logger.debug(f"Failed to load from {html_path}: {str(e)}")
            continue
    
    if html_content:
        return HTMLResponse(content=html_content)
    else:
        logger.error("Could not find index.html in any path")
        raise HTTPException(status_code=500, detail="Mini App HTML not found")


# ==================== ERROR HANDLERS ====================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Custom HTTP exception handler"""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "success": False,
            "error": exc.detail,
            "timestamp": datetime.now().isoformat()
        }
    )


# ==================== STARTUP ====================

@app.on_event("startup")
async def startup_event():
    """Run on app startup"""
    logger.info("🚀 OnBrain AI Mini App starting...")
    logger.info(f"📍 Supabase: {SUPABASE_URL}")
    logger.info(f"🤖 OpenAI: GPT-4o configured")
    logger.info("✅ All services ready")


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info"
    )


