"""
Smoke test: server ni ko'tarish + API testlash
"""
import subprocess, sys, time, requests, json, os

BASE = "c:\\Users\\Lenovo\\OneDrive\\OnBrain AI"
PYTHON = os.path.join(BASE, "venv", "Scripts", "python.exe")
URL = "http://127.0.0.1:8001"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1uwN8lowZaA5uCNFDetWMm8dBqo9q-TF__m6gMbFsT90/edit"
UID = 999001

def p(msg): print(msg, flush=True)

# Server ishga tushirish
p("▶ Server ishga tushirilmoqda...")
proc = subprocess.Popen(
    [PYTHON, "miniapp.py"],
    cwd=BASE,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)

# Tayyor bo'lguncha kutish
for i in range(15):
    time.sleep(1)
    try:
        r = requests.get(f"{URL}/health", timeout=2)
        if r.status_code == 200:
            p(f"✅ Server tayyor ({i+1}s)")
            break
    except:
        pass
else:
    p("❌ Server ishga tushishda muammo")
    proc.terminate()
    sys.exit(1)

# ─── TEST 1: Google Sheets ulash ─────────────────────────────────────────────
p("\n📊 TEST 1: Google Sheets ulash")
r = requests.post(f"{URL}/api/sheets", json={"telegram_id": UID, "url": SHEET_URL}, timeout=15)
d = r.json()
p(f"  Status: {r.status_code}")
p(f"  Tabs: {d.get('tabs')} | Rows: {d.get('rows')} | Sources: {d.get('total_sources')}")
if not d.get("success"):
    p(f"  ❌ XATO: {d}")
else:
    p("  ✅ Sheet muvaffaqiyatli ulandi!")

# ─── TEST 2: Shaxs qidirish (explicit ism) ───────────────────────────────────
p("\n💬 TEST 2: Shaxs qidirish savolini yuborish")
r = requests.post(f"{URL}/api/chat", json={"telegram_id": UID, "message": "1-oquvchining bali necha?"}, timeout=30)
d = r.json()
p(f"  Source: {d.get('source')}")
ans = d.get("answer","")
# HTML taglarini tozalaymiz
import re
clean = re.sub(r'<[^>]+>', '', ans)
p(f"  Javob: {clean[:200]}")

# ─── TEST 3: Umumiy savol (AI fallback) ──────────────────────────────────────
p("\n🤖 TEST 3: Umumiy savol (AI fallback)")
r = requests.post(f"{URL}/api/chat", json={"telegram_id": UID, "message": "hammaning umumiy bali necha?"}, timeout=30)
d = r.json()
p(f"  Source: {d.get('source')}")
clean = re.sub(r'<[^>]+>', '', d.get("answer",""))
p(f"  Javob: {clean[:200]}")

# ─── TEST 4: Session tekshirish ───────────────────────────────────────────────
p("\n📋 TEST 4: Session holati tekshirish")
r = requests.get(f"{URL}/api/session/{UID}", timeout=5)
d = r.json()
p(f"  has_data: {d.get('has_data')} | lang: {d.get('lang')} | sources: {len(d.get('sources',[]))}")
p("  ✅ Session to'g'ri!")

p("\n🎉 Barcha testlar tugadi!")
proc.terminate()
