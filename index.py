from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(title="OnBrain AI Mini App", version="1.0.0")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== HEALTH ENDPOINTS ====================

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "OnBrain AI Mini App Backend",
        "status": "running",
        "version": "1.0.0"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "ok",
        "service": "OnBrain AI Mini App",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/health")
async def api_health():
    """API health check"""
    return {
        "status": "healthy",
        "service": "OnBrain AI API",
        "timestamp": datetime.now().isoformat()
    }

# ==================== MINI APP ENDPOINT ====================

@app.get("/miniapp")
async def get_miniapp():
    """Serve the Mini App HTML"""
    try:
        # Look for static/index.html
        static_paths = [
            os.path.join(os.path.dirname(__file__), "..", "static", "index.html"),
            os.path.join(os.getcwd(), "static", "index.html"),
            os.path.join("/", "app", "static", "index.html"),
            "static/index.html",
        ]
        
        for path in static_paths:
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    return HTMLResponse(content=f.read())
        
        # Fallback HTML if file not found
        return HTMLResponse(content="""
        <!DOCTYPE html>
        <html>
        <head>
            <title>OnBrain AI Mini App</title>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                * {
                    margin: 0;
                    padding: 0;
                    box-sizing: border-box;
                }
                body {
                    background: #0D0D1A;
                    color: #FFFFFF;
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    min-height: 100vh;
                    padding: 20px;
                }
                .container {
                    text-align: center;
                    background: rgba(123, 47, 255, 0.1);
                    padding: 40px;
                    border-radius: 12px;
                    border: 1px solid #7B2FFF;
                }
                h1 {
                    color: #7B2FFF;
                    margin-bottom: 20px;
                    font-size: 32px;
                }
                p {
                    font-size: 16px;
                    line-height: 1.6;
                    margin-bottom: 15px;
                }
                .loading {
                    display: inline-block;
                    margin-top: 20px;
                }
                .spinner {
                    width: 40px;
                    height: 40px;
                    border: 4px solid rgba(123, 47, 255, 0.3);
                    border-top: 4px solid #7B2FFF;
                    border-radius: 50%;
                    animation: spin 1s linear infinite;
                }
                @keyframes spin {
                    0% { transform: rotate(0deg); }
                    100% { transform: rotate(360deg); }
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🚀 OnBrain AI</h1>
                <p>Mini App yuklanmoqda...</p>
                <p style="font-size: 14px; color: #AAA;">Mini App faylidagi HTML faylini topa olmadi</p>
                <div class="loading">
                    <div class="spinner"></div>
                </div>
            </div>
        </body>
        </html>
        """)
    except Exception as e:
        logger.error(f"Error loading miniapp: {str(e)}")
        return HTMLResponse(
            content=f"""
            <html>
            <body style="background: #0D0D1A; color: red; font-family: Arial; padding: 50px; text-align: center;">
                <h1>❌ Xato</h1>
                <p>{str(e)}</p>
            </body>
            </html>
            """,
            status_code=500
        )

# ==================== PLACEHOLDER API ENDPOINTS ====================

@app.post("/api/register")
async def register(request: Request):
    """Register endpoint placeholder"""
    return {"success": True, "message": "Foydalanuvchi saqlandi"}

@app.post("/api/chat")
async def chat(request: Request):
    """Chat endpoint placeholder"""
    try:
        data = await request.json()
        return {
            "success": True,
            "answer": "Salom! Men OnBrain AI yordamchisiman.",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/user/{telegram_id}")
async def get_user(telegram_id: int):
    """Get user endpoint placeholder"""
    return {"success": True, "user": {"telegram_id": telegram_id}}

@app.get("/api/history/{telegram_id}")
async def get_history(telegram_id: int):
    """Get history endpoint placeholder"""
    return {"success": True, "messages": [], "count": 0}

# ==================== ERROR HANDLER ====================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"success": False, "error": exc.detail}
    )
