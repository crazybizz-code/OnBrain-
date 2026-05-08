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
BOT_TOKEN      = os.getenv("BOT_TOKEN", "")
ADMIN_CHAT_ID  = os.getenv("ADMIN_CHAT_ID", "")  # Your Telegram ID for notifications

# ALWAYS use Grok first (xAI API is OpenAI-compatible)
# Only fall back to OpenAI if GROK_API_KEY is missing
if GROK_API_KEY:
    AI_KEY      = GROK_API_KEY
    AI_BASE_URL = "https://api.x.ai/v1"
    AI_MODEL    = "grok-3-latest"
else:
    AI_KEY      = OPENAI_API_KEY
    AI_BASE_URL = "https://api.openai.com/v1"
    AI_MODEL    = "gpt-4o"

# ── In-memory session store ───────────────────────────────
# {telegram_id: {"sources": [...], "lang": "uz", "web_search": False, "chat_count": 0, "rated": False}}
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

# ── MVP RAG: keyword-based chunk retrieval ────────────────
# No embeddings, no vector DB needed for structured Excel/Sheets data.

# Uzbek suffix list for morphology normalization
_UZ_SUFFIXES = [
    "larning", "larga", "lardan", "larni", "larda", "lar",
    "ning", "ndan", "ngga", "ndan", "dagi", "dan", "ga",
    "ni", "da", "gi", "chi", "lik", "siz", "gina",
]

def _normalize(word: str) -> str:
    """Strip Uzbek grammatical suffixes to get root form."""
    w = word.lower().strip("'ʻ")
    for suf in _UZ_SUFFIXES:
        if w.endswith(suf) and len(w) - len(suf) >= 3:
            return w[:-len(suf)]
    return w

def _tokenize(text: str) -> set:
    """Lowercase words, 3+ chars, no stop words. Returns roots + originals."""
    STOP = {
        "va", "bu", "u", "biz", "men", "sen", "ular", "ham", "esa", "uchun",
        "bilan", "dan", "ga", "da", "ni", "ning", "dagi", "nima", "kim",
        "qancha", "qachon", "nechchi", "nechanchi", "menga", "unga",
        "the", "is", "are", "was", "were", "of", "in", "on", "at", "to",
        "ayt", "ber", "top", "qil", "kors", "korsatib",
    }
    words = re.findall(r"[a-zA-Zа-яА-ЯёЁ'ʻo'O']+", text.lower())
    tokens = set()
    for w in words:
        if len(w) >= 3 and w not in STOP:
            tokens.add(w)
            root = _normalize(w)
            if len(root) >= 3:
                tokens.add(root)
    return tokens

def _sanitize_input(text: str) -> str:
    """
    Prompt injection protection.
    Remove attempts to override system prompt or leak data.
    """
    INJECTION_PATTERNS = [
        r"ignore\s+(previous|above|all)\s+instructions?",
        r"forget\s+(everything|all|previous)",
        r"you\s+are\s+now",
        r"new\s+instructions?:",
        r"system\s*:",
        r"<\s*system\s*>",
        r"\[INST\]",
        r"###\s*system",
        r"act\s+as\s+(a\s+)?(?:different|new|another)",
        r"pretend\s+(?:you\s+are|to\s+be)",
        r"show\s+(me\s+)?(?:your\s+)?(?:system\s+)?prompt",
        r"reveal\s+(?:your\s+)?(?:instructions?|prompt|system)",
        r"print\s+(?:your\s+)?(?:instructions?|prompt|system)",
        r"barcha\s+(?:foydalanuvchi|user)\s+ma['ʻ]lumotlar",
        r"boshqa\s+(?:foydalanuvchi|user)",
    ]
    for pattern in INJECTION_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return "[FILTERED]"
    # Truncate extremely long inputs (prevent token stuffing)
    return text[:2000]

def _row_to_text(row: Dict, header: List[str]) -> str:
    return " | ".join(str(row.get(h, "")) for h in header)


def _extract_context_entities(context: str) -> set:
    """Extract all word tokens from retrieved context for validation."""
    tokens = set(re.findall(r"[A-Za-zА-Яа-яЁёÀ-ÿA-Za-z'ʻ\u0400-\u04FF\u00C0-\u024F]+", context))
    return {t.lower() for t in tokens if len(t) >= 3}


def _validate_response(answer: str, context: str) -> str:
    """
    Post-processing: check if AI introduced names NOT in context.
    Strategy: extract capitalized multi-word sequences (likely names) from answer,
    verify each word exists in context tokens. If a name word is absent → flag.
    Returns cleaned answer or rejection message.
    """
    if not context:
        return answer

    ctx_tokens = _extract_context_entities(context)

    # Find capitalized sequences in answer (likely person names)
    # e.g. "Muhammadali Karimov" — two capitalized words in a row
    name_candidates = re.findall(
        r'\b([A-ZА-ЯЎҚҒҲ\u00C0-\u024F][a-zа-яўқғҳ\u00C0-\u024F\'ʻ]{2,}(?:\s+[A-ZА-ЯЎҚҒҲ\u00C0-\u024F][a-zа-яўқғҳ\u00C0-\u024F\'ʻ]{2,})*)\b',
        answer
    )

    hallucinated = []
    for name in name_candidates:
        words = name.split()
        for w in words:
            if w.lower() not in ctx_tokens:
                hallucinated.append(name)
                break

    if hallucinated:
        # Log but return rejection
        logger.warning(f"[HALLUCINATION] AI added entities not in context: {hallucinated}")
        return "❌ Ma'lumotlarda bu savol bo'yicha to'liq javob topilmadi. Iltimos, aniqroq savol bering."

    return answer

def _score_row(row_text: str, query_tokens: set) -> float:
    """
    Score row relevance. Returns float 0..1 (confidence).
    Exact match scores higher than partial.
    """
    row_lower = row_text.lower()
    if not query_tokens:
        return 0.0
    matched = sum(1 for t in query_tokens if t in row_lower)
    return matched / len(query_tokens)

# Minimum confidence threshold — below this, row is ignored
RETRIEVAL_CONFIDENCE_THRESHOLD = 0.15  # at least 15% of query tokens must match

def build_context(sources: List[Dict], question: str = "") -> tuple:
    """
    MVP RAG: retrieve only relevant rows for the question.
    Returns (context_str, max_confidence_float, matched_sources_list).
    max_confidence: 0.0 = nothing found, 1.0 = perfect match.
    """
    query_tokens = _tokenize(question) if question else set()
    parts = []
    max_confidence = 0.0
    matched_sources = []

    for s in sources:
        if s.get("disabled"):
            continue
        name = s.get("name", "Ma'lumot")
        rows = s.get("preview", [])
        if not rows:
            continue
        header = list(rows[0].keys())
        header_line = " | ".join(str(c) for c in header)

        if not query_tokens:
            selected = [(0.5, i + 1, row) for i, row in enumerate(rows[:50])]
            note = f"(birinchi 50 qator)"
        else:
            scored = []
            for i, row in enumerate(rows):
                row_text = _row_to_text(row, header)
                score = _score_row(row_text, query_tokens)
                if score >= RETRIEVAL_CONFIDENCE_THRESHOLD:
                    scored.append((score, i + 1, row))  # (score, row_num, row)
            scored.sort(key=lambda x: x[0], reverse=True)
            top = scored[:80]
            selected = top
            total_matched = len(scored)
            if top:
                note = f"(savolga mos {total_matched} qatordan top {len(top)} ta)"
            else:
                note = "(mos qator topilmadi)"

        if not selected:
            parts.append(
                f"### {name}\n"
                f"{header_line}\n"
                f"[Bu manbada savolga mos ma'lumot topilmadi]"
            )
            continue

        # Track confidence
        src_max_conf = max(score for score, _, _ in selected)
        if src_max_conf > max_confidence:
            max_confidence = src_max_conf
        matched_sources.append({
            "name": name,
            "matched_rows": len(selected),
            "confidence": round(src_max_conf, 2),
        })

        # Build context lines with row numbers (source attribution)
        lines = [f"{header_line} | [qator#]"]
        for score, row_num, row in selected:
            lines.append(_row_to_text(row, header) + f" | [#{row_num}]")

        parts.append(f"### {name} {note}\n" + "\n".join(lines))

    return "\n\n".join(parts), max_confidence, matched_sources


# ── Strict Person Search (no AI, no hallucination) ───────

def _to_num(v) -> Optional[float]:
    try:
        return float(str(v).replace(",", ".").strip())
    except Exception:
        return None

def _strip_suffix_simple(w: str) -> str:
    """Remove common Uzbek grammatical suffixes."""
    w = w.lower().strip()
    for suf in ["ning", "nig", "dan", "ga", "ni", "da", "lar"]:
        if w.endswith(suf) and len(w) - len(suf) >= 3:
            return w[:-len(suf)]
    return w

# Name column identifiers
_NAME_COL_KEYS = ["f.i.o", "fio", "fish", "ism", "name", "familiya", "to'liq ism", "toliq ism"]

# Stop words — never treat as person name tokens
_STOP = {
    "nechchi","necchi","qancha","ball","balli","ballari","baho","umumiy","jami",
    "fanidan","fani","fandan","olgan","olgani","nima","qaysi","qachon","kim",
    "necha","va","bilan","uchun","ning","ni","ga","da","dan","menga","top",
    "ayt","ber","ko'rsat","korsat","natija","natijasi","hisobi","score",
    "ingliz","rus","ona","tili","algebra","geometriya","matematika","fizika",
    "kimyo","biologiya","tarix","geografiya","adabiyot","informatika",
    "texnologiya","sport","jismoniy","sarflandi","sarflangan","xarajat",
    "sotildi","tushum","daromad","hamma","hammasi","barchasi","qayerda",
    "qanday","nima","nechta","nechtasi","yig'indi","total","yig",
}

# Subject → column keyword mapping
_SUBJECTS = {
    "algebra":     ["algebra"],
    "geometriya":  ["geometriya","геометрия"],
    "matematika":  ["matematika","математика","math"],
    "fizika":      ["fizika","физика","physics"],
    "kimyo":       ["kimyo","химия"],
    "biologiya":   ["biologiya","биология"],
    "tarix":       ["tarix","история"],
    "geografiya":  ["geografiya","география"],
    "adabiyot":    ["adabiyot","литература"],
    "ingliz":      ["ingliz","английский","english"],
    "rus":         ["rus tili","rus","русский"],
    "ona tili":    ["ona tili","ona","узбекский"],
    "informatika": ["informatika","информатика"],
    "jismoniy":    ["jismoniy","sport","физкультура"],
}

def _get_name_cols(header: List[str]) -> List[str]:
    """Return columns that look like person-name columns."""
    return [h for h in header if any(k in str(h).lower() for k in _NAME_COL_KEYS)]

def _extract_name_tokens(question: str) -> List[str]:
    """
    Extract potential person-name tokens from question.
    Only keeps words that are NOT stop-words and NOT digits.
    """
    words = re.split(r"[\s\-_.,!?\"'()[\]]+", question.strip())
    tokens = []
    for w in words:
        cl = _strip_suffix_simple(w)
        if len(cl) >= 3 and cl not in _STOP and not cl.isdigit():
            if cl not in tokens:
                tokens.append(cl)
    return tokens

def _row_matches_name_tokens(row: Dict, name_cols: List[str], tokens: List[str]) -> Optional[str]:
    """
    Returns the matched full name string if ANY token exactly matches
    a word inside a name-column cell. Otherwise None.
    ONLY searches name columns — NOT arbitrary text fields.
    """
    for nc in name_cols:
        cell = str(row.get(nc, "")).strip()
        if not cell:
            continue
        cell_words = re.split(r"[\s\-_]+", cell.lower())
        cell_words_stripped = [_strip_suffix_simple(w) for w in cell_words]
        for tok in tokens:
            tok_l = tok.lower()
            if tok_l in cell_words or tok_l in cell_words_stripped:
                return cell  # return original casing
    return None

def _answer_for_row(row: Dict, header: List[str], name: str, src_name: str, question: str) -> str:
    """
    Given a specific matched row, build a formatted answer string.
    Detects: specific subject score / umumiy ball / all scores.
    """
    q = question.lower()

    # Detect specific subject
    asked_kws = None
    for subj, kws in _SUBJECTS.items():
        if any(kw in q for kw in kws):
            asked_kws = kws
            break

    is_total = any(w in q for w in ["umumiy","jami","total","hammasi","yig'indi","yig"])

    # Skip name/id columns when summing
    _SKIP_COL_KEYS = {"f.i.o","fio","fish","ism","name","familiya","tartib","raqam","#","id","sn"}

    if asked_kws:
        for h in header:
            hl = str(h).lower()
            if any(kw in hl for kw in asked_kws):
                v = _to_num(row.get(h))
                if v is not None:
                    return f"👤 <b>{name}</b>\n📚 {h}: <b>{v}</b>\n📂 <i>{src_name}</i>"
        return f"👤 <b>{name}</b>\n❌ {asked_kws[0].capitalize()} ustuni topilmadi\n📂 <i>{src_name}</i>"

    # Try dedicated total column first
    for h in header:
        hl = str(h).lower()
        if ("umumiy" in hl and "ball" in hl) or hl in ["umumiy ball","total","jami ball","jami"]:
            v = _to_num(row.get(h))
            if v is not None:
                return f"👤 <b>{name}</b>\n🏆 {h}: <b>{v}</b>\n📂 <i>{src_name}</i>"

    # Sum all numeric non-name/id columns
    total = 0.0
    details = []
    for h in header:
        hl = str(h).lower()
        if any(k in hl for k in _SKIP_COL_KEYS):
            continue
        if ("umumiy" in hl and "ball" in hl) or hl in ["umumiy ball","total","jami ball","jami"]:
            continue
        v = _to_num(row.get(h))
        if v is not None:
            total += v
            details.append(f"{h}: {v}")

    if details:
        detail_str = " | ".join(details)
        return f"👤 <b>{name}</b>\n🏆 Jami ball: <b>{total:.1f}</b>\n📊 {detail_str}\n📂 <i>{src_name}</i>"

    # No numeric values — show entire row as-is
    row_str = " | ".join(f"{h}: {row.get(h,'')}" for h in header
                         if not any(k in str(h).lower() for k in _SKIP_COL_KEYS))
    return f"👤 <b>{name}</b>\n📋 {row_str}\n📂 <i>{src_name}</i>"


def _find_persons(question: str, sources: List[Dict]) -> List[Dict]:
    """
    Strict column-aware person search.
    Returns list of dicts: {name, answer, row, header, src_name, name_col}
    Empty list = no match.
    Only searches F.I.O/name columns — NOT arbitrary text.
    Logs matched tokens and rows for debug.
    """
    tokens = _extract_name_tokens(question)
    if not tokens:
        logger.debug(f"[SEARCH] No name tokens extracted from: {question!r}")
        return []

    logger.debug(f"[SEARCH] Name tokens: {tokens}")

    found = []
    seen_names = set()  # dedup by name string

    for src in sources:
        if src.get("disabled"):
            continue
        rows = src.get("preview", [])
        if not rows:
            continue
        src_name = src.get("name", "Manba")
        header = list(rows[0].keys())

        name_cols = _get_name_cols(header)
        if not name_cols:
            logger.debug(f"[SEARCH] Skipping {src_name!r} — no name columns found")
            continue

        logger.debug(f"[SEARCH] Searching in {src_name!r}, name_cols={name_cols}")

        for row in rows:
            matched = _row_matches_name_tokens(row, name_cols, tokens)
            if matched:
                key = matched.lower().strip()
                if key in seen_names:
                    continue
                seen_names.add(key)
                answer = _answer_for_row(row, header, matched, src_name, question)
                found.append({
                    "name": matched,
                    "answer": answer,
                    "row": row,
                    "header": header,
                    "src_name": src_name,
                    "name_col": name_cols[0],
                })

    logger.debug(f"[SEARCH] Matched {len(found)} unique person(s)")
    return found


def _answer_scoped(question: str, selected: Dict) -> str:
    """
    Answer question using ONLY the pre-selected student row.
    No new search — fully scoped to one person.
    """
    return _answer_for_row(
        selected["row"],
        selected["header"],
        selected["name"],
        selected["src_name"],
        question
    )


# ── Google Sheets URL → CSV URL ───────────────────────────
    """
    Try to answer directly from Excel/Sheets data without AI.
    Returns answer string or None (let AI handle it).
    """
    q = question.strip().lower()

    # Subject keywords → column name fragments
    SUBJECTS = {
        "algebra": ["algebra"],
        "geometriya": ["geometriya", "геометрия"],
        "matematika": ["matematika", "математика", "math"],
        "fizika": ["fizika", "физика", "physics"],
        "kimyo": ["kimyo", "химия"],
        "biologiya": ["biologiya", "биология"],
        "tarix": ["tarix", "история"],
        "geografiya": ["geografiya", "география"],
        "adabiyot": ["adabiyot", "литература"],
        "ingliz": ["ingliz", "английский", "english"],
        "rus": ["rus tili", "rus", "русский"],
        "ona tili": ["ona tili", "ona", "узбекский"],
        "informatika": ["informatika", "информатика"],
        "jismoniy": ["jismoniy", "sport", "физкультура"],
    }

    # Stop words — not person names
    STOP = {
        "nechchi", "necchi", "qancha", "ball", "balli", "ballari", "baho",
        "umumiy", "jami", "fanidan", "fani", "fandan", "olgan", "olgani",
        "nima", "qaysi", "qachon", "kim", "necha", "va", "bilan", "uchun",
        "ning", "ni", "ga", "da", "dan", "menga", "top", "ayt", "ber",
        "ko'rsat", "korsat", "natija", "natijasi", "hisobi", "score",
        "ingliz", "rus", "ona", "tili", "algebra", "geometriya", "matematika",
        "fizika", "kimyo", "biologiya", "tarix", "geografiya", "adabiyot",
        "informatika", "texnologiya", "sport", "jismoniy", "sarflandi",
        "sarflangan", "xarajat", "sotildi", "tushum", "daromad",
    }

    # Extract name candidates
    words = [w.strip(".,!?\"'()[]") for w in question.split()]
    name_candidates = []
    for w in words:
        cl = _strip_suffix_simple(w)
        if len(cl) >= 3 and cl not in STOP and not cl.isdigit():
            if cl not in name_candidates:
                name_candidates.append(cl)

    if not name_candidates:
        return None

    # Detect asked subject
    asked_subject_kws = None
    for subj, kws in SUBJECTS.items():
        if any(kw in q for kw in kws):
            asked_subject_kws = kws
            break

    is_total = any(w in q for w in ["umumiy", "jami", "total", "hammasi", "yig'indi"])

    results = []
    for src in sources:
        if src.get("disabled"):
            continue
        rows = src.get("preview", [])
        if not rows:
            continue
        src_name = src.get("name", "Manba")
        header = list(rows[0].keys())

        # Find name columns
        name_cols = [h for h in header if any(
            w in str(h).lower() for w in ["f.i.o", "fio", "ism", "name", "familiya", "fish"]
        )]
        if not name_cols:
            # No person name column — not a person registry, skip for lookup
            continue

        for row in rows:
            # Check if any name candidate matches any name column
            matched_name = None
            for nc in name_cols:
                cell = str(row.get(nc, "")).strip().lower()
                if not cell:
                    continue
                # Split cell into individual tokens (words)
                cell_tokens = re.split(r"[\s\-_]+", cell)
                cell_tokens_stripped = [_strip_suffix_simple(t) for t in cell_tokens]
                for cand in name_candidates:
                    cand_l = cand.lower()
                    # Full token match only — "muhammad" must be its own word
                    if cand_l in cell_tokens or cand_l in cell_tokens_stripped:
                        matched_name = str(row.get(nc, "")).strip()
                        break
                if matched_name:
                    break
            if not matched_name:
                continue

            # Found a matching row — extract value
            if asked_subject_kws:
                # Find subject column
                val = None
                col_name = None
                for h in header:
                    hl = str(h).lower()
                    if any(kw in hl for kw in asked_subject_kws):
                        v = _to_num(row.get(h))
                        if v is not None:
                            val = v
                            col_name = str(h)
                            break
                if val is not None:
                    results.append(f"👤 <b>{matched_name}</b>\n📚 {col_name}: <b>{val}</b>\n📂 <i>Manba: {src_name}</i>")
                else:
                    results.append(f"👤 <b>{matched_name}</b>\n❌ {asked_subject_kws[0].capitalize()} fani uchun ball ko'rsatilmagan\n📂 <i>Manba: {src_name}</i>")
            else:
                # Try to find dedicated "Umumiy ball" column
                total_val = None
                total_col = None
                for h in header:
                    hl = str(h).lower()
                    if ("umumiy" in hl and "ball" in hl) or hl in ["umumiy ball", "total", "jami ball"]:
                        v = _to_num(row.get(h))
                        if v is not None:
                            total_val = v
                            total_col = str(h)
                            break
                if total_val is not None:
                    results.append(f"👤 <b>{matched_name}</b>\n🏆 {total_col}: <b>{total_val}</b>\n📂 <i>Manba: {src_name}</i>")
                else:
                    # Sum all numeric non-name cols
                    total = 0.0
                    details = []
                    for h in header:
                        hl = str(h).lower()
                        if any(w in hl for w in ["f.i.o", "fio", "ism", "name", "familiya", "fish", "tartib", "raqam", "#"]):
                            continue
                        if ("umumiy" in hl and "ball" in hl) or hl in ["umumiy ball", "total", "jami ball"]:
                            continue
                        v = _to_num(row.get(h))
                        if v is not None:
                            total += v
                            details.append(f"{h}={v}")
                    if details:
                        results.append(f"👤 <b>{matched_name}</b>\n🏆 Jami ball: <b>{total:.1f}</b>\n📂 <i>Manba: {src_name}</i>")

    if not results:
        return None

    # Multiple results
    if len(results) == 1:
        return results[0]
    # Multiple people found — list them all
    header_line = f"📋 {len(results)} ta o'quvchi topildi:\n"
    return header_line + "\n\n".join(results)

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
async def get_session_endpoint(telegram_id: int):
    sess = session_get(telegram_id)
    # Strip heavy fields — only send metadata, not raw spreadsheet data
    HEAVY = {"preview", "csv_url", "data"}
    light_sources = [{k: v for k, v in s.items() if k not in HEAVY}
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

# ── Sync session from bot.py ──────────────────────────────
@app.post("/api/sync_session")
async def sync_session(request: Request):
    """
    Called by bot.py after Excel/Sheets connected.
    Merges sources into main.py session so mini app sees them.
    """
    data = await request.json()
    uid = int(data.get("telegram_id", 0))
    if not uid:
        raise HTTPException(400, "telegram_id required")
    sess = session_get(uid)
    # Merge incoming sources (add if not already present by name)
    incoming = data.get("sources", [])
    existing_names = {s.get("name") for s in sess.get("sources", [])}
    for src in incoming:
        if src.get("name") not in existing_names:
            sess.setdefault("sources", []).append(src)
            existing_names.add(src.get("name"))
    # Also update lang if provided
    if data.get("lang"):
        sess["lang"] = data["lang"]
    session_save(uid, sess)
    logger.info(f"Session synced from bot: uid={uid} sources={len(sess['sources'])}")
    return {"success": True, "sources": len(sess["sources"])}

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

# ── Clear selected student (session reset) ────────────────
@app.post("/api/clear_student")
async def clear_student(request: Request):
    """Reset selected student state — user can start a new search."""
    data = await request.json()
    uid = int(data.get("telegram_id", 0))
    if uid:
        sess = session_get(uid)
        sess.pop("__selected_student", None)
        sess.pop("__disambig_question", None)
        sess.pop("__disambig_persons", None)
        sess.pop("__disambig_names", None)
        session_save(uid, sess)
    return {"success": True}

# ── Disambiguation pick ───────────────────────────────────
@app.post("/api/disambiguate")
async def disambiguate(request: Request):
    """
    Mini app / bot calls this when user picks a name from disambiguation list.
    Saves selected student to session for follow-up questions.
    """
    data = await request.json()
    uid = int(data.get("telegram_id", 0))
    chosen_name = data.get("chosen", "").strip()
    if not uid or not chosen_name:
        raise HTTPException(400, "telegram_id and chosen required")

    sess = session_get(uid)
    orig_question = sess.get("__disambig_question", "")

    # Try to find the person from saved persons list first (no re-search needed)
    saved_persons: List[Dict] = sess.get("__disambig_persons", [])
    chosen_person = None
    chosen_lower = chosen_name.lower().strip()
    for p in saved_persons:
        if p["name"].lower().strip() == chosen_lower:
            chosen_person = p
            break

    # If not in saved list (e.g. bot path), re-run search with full name
    if not chosen_person:
        question = chosen_name + (" " + orig_question if orig_question else "")
        persons = _find_persons(question, sess.get("sources", []))
        for p in persons:
            if p["name"].lower().strip() == chosen_lower:
                chosen_person = p
                break

    # Clear disambig state and save selected student
    sess.pop("__disambig_persons", None)
    sess.pop("__disambig_question", None)
    sess.pop("__disambig_names", None)

    if chosen_person:
        # Save to session for follow-up questions
        sess["__selected_student"] = chosen_person
        sess["chat_count"] = sess.get("chat_count", 0) + 1
        session_save(uid, sess)
        ask_rating = (sess["chat_count"] == 5 and not sess.get("rated"))
        # Answer based on original question
        answer = _answer_scoped(orig_question or chosen_name, chosen_person)
        logger.info(f"[DISAMBIG_PICK] uid={uid} selected={chosen_person['name']!r}")
        return {"success": True, "answer": answer, "ask_rating": ask_rating}

    session_save(uid, sess)
    return {"success": True, "answer": "❌ Ma'lumot topilmadi.", "ask_rating": False}

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
    """
    Real-time web search via Tavily API.
    Uses 'advanced' depth for fresh, real-time results (news, prices, current events).
    """
    if not TAVILY_API_KEY:
        return ""
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(
                "https://api.tavily.com/search",
                json={
                    "api_key": TAVILY_API_KEY,
                    "query": query,
                    "max_results": 8,
                    "search_depth": "advanced",  # Real-time, fresh results (not basic)
                    "include_answer": True,      # Get AI-generated answer from search
                    "include_raw_content": False,
                    "include_images": False
                },
                headers={"Content-Type": "application/json"}
            )
        if resp.status_code == 200:
            data = resp.json()
            # Get AI answer if available
            answer = data.get("answer", "")
            results = data.get("results", [])
            
            output = []
            if answer:
                output.append(f"📌 ANSWER: {answer}\n")
            
            # Add top search results
            for r in results[:8]:
                title = r.get('title', '')
                content = r.get('content', '')[:400]
                url = r.get('url', '')
                output.append(f"• {title}\n  {content}\n  🔗 {url}")
            
            return "\n\n".join(output)
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
    lang = sess.get("lang", "uz")
    web = sess.get("web_search", False)

    # ── Prompt injection protection
    message = _sanitize_input(message)
    if message == "[FILTERED]":
        return {"success": True, "answer": "⚠️ Savol tarkibida ruxsat etilmagan buyruq aniqlandi.", "ask_rating": False}

    # ── RAG: build context with question-aware retrieval + confidence
    context, retrieval_confidence, matched_sources = build_context(sess.get("sources", []), message)

    # ── 1. Strict person search (no AI, no hallucination)
    if not web:
        # CASE A: selected student in session → scoped answer (no re-search)
        selected = sess.get("__selected_student")
        if selected:
            q_lower = message.lower()
            # Reset on explicit reset keywords
            reset_keywords = ["boshqa", "yangi", "reset", "qayta", "boshidan", "exit", "chiq"]
            explicit_reset = any(kw in q_lower for kw in reset_keywords)

            # Auto-reset: if none of the selected student's name words appear in the question
            # e.g. "kamera va ijaraga" has nothing to do with "Halimjonov Muhammad"
            student_name_words = set(
                w.lower() for w in re.split(r"[\s\-_]+", selected["name"])
                if len(w) >= 3
            )
            question_words = set(
                _strip_suffix_simple(w) for w in re.split(r"[\s\-_.,!?\"'()[\]]+", message)
                if len(w) >= 3
            )
            name_still_referenced = bool(student_name_words & question_words)

            if explicit_reset or not name_still_referenced:
                # Release scoped lock — treat as new question
                sess.pop("__selected_student", None)
                sess.pop("__disambig_question", None)
                session_save(uid, sess)
                # Fall through to CASE B / AI
            else:
                answer = _answer_scoped(message, selected)
                sess["chat_count"] = sess.get("chat_count", 0) + 1
                session_save(uid, sess)
                ask_rating = (sess["chat_count"] == 5 and not sess.get("rated"))
                logger.info(f"[SCOPED] uid={uid} student={selected['name']!r}")
                return {"success": True, "answer": answer, "ask_rating": ask_rating}

        # CASE B: global person search
        persons = _find_persons(message, sess.get("sources", []))

        if len(persons) == 1:
            # Exactly one match → direct answer + save to session
            p = persons[0]
            sess["__selected_student"] = p
            sess["__disambig_question"] = message
            sess["chat_count"] = sess.get("chat_count", 0) + 1
            session_save(uid, sess)
            ask_rating = (sess["chat_count"] == 5 and not sess.get("rated"))
            logger.info(f"[EXACT] uid={uid} student={p['name']!r}")
            return {"success": True, "answer": p["answer"], "ask_rating": ask_rating}

        if len(persons) > 1:
            # Multiple matches → disambiguation UI
            names = [p["name"] for p in persons]
            sess["__disambig_question"] = message
            sess["__disambig_persons"] = persons  # save full person objects
            sess.pop("__selected_student", None)
            session_save(uid, sess)
            logger.info(f"[DISAMBIG] uid={uid} candidates={names}")
            return {
                "success": True,
                "disambiguation": True,
                "question": message,
                "candidates": names,
                "answer": f"📋 {len(names)} ta o'quvchi topildi. Qaysi birini ko'rmoqchisiz?",
                "ask_rating": False,
            }

        # CASE C: no person match
        # Only refuse if: person sources exist AND all sources are person-type
        # If non-person sources also exist (e.g. expense sheet) → let AI handle it
        has_person_sources = any(
            _get_name_cols(list(s["preview"][0].keys()))
            for s in sess.get("sources", [])
            if not s.get("disabled") and s.get("preview")
        )
        has_non_person_sources = any(
            not _get_name_cols(list(s["preview"][0].keys()))
            for s in sess.get("sources", [])
            if not s.get("disabled") and s.get("preview")
        )
        name_tokens = _extract_name_tokens(message)
        # Only block if: has person sources, name tokens exist, but NO non-person sources
        # e.g. "kamera va ijara" → has_non_person_sources=True → go to AI
        if has_person_sources and name_tokens and not has_non_person_sources:
            logger.info(f"[NO_MATCH] uid={uid} query={message!r}")
            return {
                "success": True,
                "answer": "❌ Bu ismli o'quvchi ma'lumotlar bazasida topilmadi.",
                "ask_rating": False,
            }

    # ── 2. Confidence gate — if data exists but nothing relevant found, refuse
    sources_exist = bool(sess.get("sources"))
    if sources_exist and not web and retrieval_confidence < RETRIEVAL_CONFIDENCE_THRESHOLD:
        lang_refuse = {
            "uz": "❌ Ma'lumotlar bazasida bu savolga javob topilmadi.\nAniqroq savol bering yoki to'g'ri ma'lumot manbasi ulangan bo'lsin.",
            "ru": "❌ В базе данных ответ на этот вопрос не найден.\nУточните вопрос или убедитесь, что подключён нужный источник.",
            "en": "❌ No relevant data found for this question.\nPlease rephrase or ensure the correct data source is connected.",
        }
        return {"success": True, "answer": lang_refuse.get(lang, lang_refuse["uz"]), "ask_rating": False}

    # ── 3. AI answer
    lang_map = {"uz": "O'zbek tilida", "ru": "Русском языке", "en": "English"}
    lang_str = lang_map.get(lang, "O'zbek tilida")

    # Web search — ONLY when explicitly enabled
    web_context = ""
    if web and TAVILY_API_KEY:
        web_context = await tavily_search(message)

    # Source attribution note for AI
    src_note = ""
    if matched_sources:
        src_note = "MANBALAR: " + ", ".join(
            f"{s['name']} ({s['matched_rows']} qator, ishonch: {int(s['confidence']*100)}%)"
            for s in matched_sources
        )

    if web:
        # ── INTERNET MODE: only Tavily results, no internal data mixed in
        if web_context:
            system = f"""Siz OnBrain AI — internet qidiruv yordamchisiz.
Javobni {lang_str} yozing.

═══ INTERNET QIDIRUV NATIJALARI ═══
{web_context}
════════════════════════════════════

QOIDALAR:
1. FAQAT yuqoridagi internet natijalaridan foydalaning.
2. Natijalar ichida javob yo'q bo'lsa — "Internet qidiruvda topilmadi" deying.
3. Hech qanday o'zingizdan ma'lumot qo'shmang.
4. Manba URL larini ko'rsating."""
        else:
            system = f"""Siz OnBrain AI.
Javobni {lang_str} yozing.
Internet qidiruv natijalari topilmadi. Foydalanuvchiga shuni ayting."""
    elif context:
        # ── INTERNAL DATA MODE: only uploaded files, strictly grounded
        system = f"""Siz OnBrain AI — faqat ma'lumot EXTRACTION assistantisiz. Siz chatbot EMASSIZ.
Javobni {lang_str} yozing.
{src_note}

════════════ FAQAT MANA SHU MA'LUMOTLARDAN FOYDALANING ════════════
{context}
═══════════════════════════════════════════════════════════════════

MUTLAQ QOIDALAR — BUZISH QATIY MAN:

[ENTITY QOIDASI]
• Yuqoridagi jadvalda KO'RINMAGAN birorta ham ism, raqam yoki entity YOZMANG.
• Ro'yxatni DAVOM ettirmang — faqat jadvaldagi qatorlarni ko'rsating.
• O'xshash, taxminiy yoki "ehtimol shunday" degan entity — TAQIQLANGAN.

[FORMAT QOIDASI]
• Har bir element ALOHIDA qatorda ko'rsating.
• Pul miqdorlarini: 1 000 000 yoki 1,000,000 ko'rinishida yozing.
• Emoji ishlat: 💰 pul uchun, 📦 buyum uchun, 📊 statistika uchun.
• Misol format:
  💰 Ijara: 1,000,000 so'm
  📦 Kamera: 300,000 so'm
• Topilmasa → "❌ Ma'lumotlarda bu savol bo'yicha javob topilmadi."

[JAVOB QOIDASI]
• Faqat jadvaldagi ANIQ QIYMATLARNI qaytaring — hech narsani o'zingizdan qo'shmang.
• "Ehtimol", "taxminan", "odatda", "menimcha", "va boshqalar", "..." — TAQIQLANGAN.
• Jadval ichidagi ma'lumotga asoslansin."""
    else:
        # ── NO DATA MODE: no files, no web — refuse clearly
        system = f"""Siz OnBrain AI.
Javobni {lang_str} yozing.
Foydalanuvchi hech qanday ma'lumot yuklamagan va internet qidiruvi o'chirilgan.
Javob: "Ma'lumot bazasi bo'sh. Iltimos, Excel fayl yoki Google Sheets ulang, yoki Internet qidiruvni yoqing." """

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
                    "temperature": 0,
                    "max_tokens": 1500
                }
            )

        if resp.status_code != 200:
            err_detail = resp.json().get("error", {}).get("message", resp.text[:300])
            raise HTTPException(500, f"OpenAI xato: {err_detail}")

        answer = resp.json()["choices"][0]["message"]["content"]

        # Post-processing: validate AI didn't hallucinate entities not in context
        if context:
            answer = _validate_response(answer, context)

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

        # Track chat count for rating prompt trigger
        sess = session_get(uid)
        sess["chat_count"] = sess.get("chat_count", 0) + 1
        session_save(uid, sess)

        # Ask for rating after 5th message (if not rated yet)
        ask_rating = (sess["chat_count"] == 5 and not sess.get("rated"))

        return {"success": True, "answer": answer, "ask_rating": ask_rating}

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

# ── Rating endpoint ───────────────────────────────────────
@app.post("/api/rating")
async def submit_rating(request: Request):
    """User submits a rating. One per user, stored in Supabase + notifies admin."""
    try:
        data = await request.json()
        telegram_id = int(data.get("telegram_id", 0))
        stars        = int(data.get("stars", 0))        # 1-5
        full_name    = str(data.get("full_name", "")).strip()
        phone        = str(data.get("phone", "")).strip()
        comment      = str(data.get("comment", "")).strip()[:500]
        username     = str(data.get("username", "")).strip()

        if not telegram_id or not (1 <= stars <= 5):
            raise HTTPException(400, "telegram_id va stars (1-5) majburiy")

        sess = session_get(telegram_id)

        # ── 1. Check already rated (in-memory fast check) ──
        if sess.get("rated"):
            return {"success": False, "already_rated": True, "message": "Siz allaqachon baho berdingiz"}

        # ── 2. Check in Supabase ───────────────────────────
        sb = get_supabase()
        if sb:
            try:
                ex = sb.table("ratings").select("id").eq("telegram_id", telegram_id).execute()
                if ex.data:
                    sess["rated"] = True
                    session_save(telegram_id, sess)
                    return {"success": False, "already_rated": True, "message": "Siz allaqachon baho berdingiz"}
            except Exception:
                pass

        # ── 3. Save rating ─────────────────────────────────
        rating_row = {
            "telegram_id": telegram_id,
            "username":    username,
            "full_name":   full_name,
            "phone":       phone,
            "stars":       stars,
            "comment":     comment,
            "created_at":  datetime.now().isoformat(),
        }
        if sb:
            try:
                sb.table("ratings").insert(rating_row).execute()
            except Exception as e:
                logger.warning(f"Rating save error: {e}")

        # ── 4. Mark session as rated ───────────────────────
        sess["rated"] = True
        session_save(telegram_id, sess)

        # ── 5. Notify admin via Telegram ───────────────────
        if BOT_TOKEN and ADMIN_CHAT_ID:
            stars_display = "⭐" * stars + "☆" * (5 - stars)
            msg = (
                f"🌟 Yangi baho!\n\n"
                f"{stars_display} ({stars}/5)\n"
                f"👤 {full_name or 'Nomsiz'}"
                + (f" (@{username})" if username else "") + "\n"
                f"📞 {phone or 'Kiritilmagan'}\n"
                f"💬 {comment or '—'}\n"
                f"🆔 tg:{telegram_id}"
            )
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    await client.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                        json={"chat_id": ADMIN_CHAT_ID, "text": msg}
                    )
            except Exception:
                pass

        return {"success": True, "message": "Rahmat! Bahoyingiz qabul qilindi 🙏"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Rating error: {e}")
        raise HTTPException(500, str(e))


@app.get("/api/rating/check/{telegram_id}")
async def check_rating(telegram_id: int):
    """Check if user has already rated."""
    sess = session_get(telegram_id)
    if sess.get("rated"):
        return {"rated": True}
    sb = get_supabase()
    if sb:
        try:
            ex = sb.table("ratings").select("id").eq("telegram_id", telegram_id).execute()
            if ex.data:
                sess["rated"] = True
                session_save(telegram_id, sess)
                return {"rated": True}
        except Exception:
            pass
    return {"rated": False}


@app.get("/api/ratings")
async def get_ratings():
    """Admin: get all ratings summary."""
    sb = get_supabase()
    if not sb:
        return {"success": False, "message": "Supabase not configured"}
    try:
        res = sb.table("ratings").select("*").order("created_at", desc=True).execute()
        rows = res.data or []
        total = len(rows)
        avg   = round(sum(r["stars"] for r in rows) / total, 1) if total else 0
        return {"success": True, "total": total, "average": avg, "ratings": rows}
    except Exception as e:
        raise HTTPException(500, str(e))



# ── Startup ───────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    logger.info("🚀 OnBrain AI v2.0 starting...")
    logger.info(f"   AI model: {AI_MODEL} @ {AI_BASE_URL}")
    logger.info(f"   AI key  : {'✅ configured' if AI_KEY else '❌ NOT SET'}")
    logger.info(f"   Supabase: {'✅ configured' if SUPABASE_URL else '⚠️  optional/not set'}")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
