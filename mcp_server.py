#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OnBrain AI — Production MCP Server  (stdio JSON-RPC)
=====================================================
Tools exposed:
  • analyze_excel(file_path, user_query [, sheet_name])
  • analyze_sheets(sheet_url, user_query [, timeout])
  • cache_clear()

Protocol (newline-delimited JSON):
  → { "id": N, "method": "...", "params": {...} }
  ← { "id": N, "result": "..." }   |   { "id": N, "error": "..." }

Features:
  1. Fuzzy matching  — difflib (stdlib), threshold 0.68
  2. Column-aware math  — sum / max / min / mean / count avtomatik
  3. Unicode normalization — O'zbek/Rus imlo, katta-kichik, affiks tozalash
  4. Google Sheets URL → CSV (ID + gid xavfsiz extraction, regex)
  5. RAM cache  — sheet/excel DataFrames (TTL 5 daqiqa)
  6. Bot integration — call_mcp_tool() async subprocess wrapper
"""

import sys
import json
import traceback
import re
import time
import unicodedata
from io import StringIO
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

try:
    import pandas as pd
    import requests
except ImportError as exc:
    print(json.dumps({"id": None,
                      "error": f"Majburiy paket yo'q: {exc}. "
                               "Buyruq: pip install -r requirements.txt"}),
          flush=True)
    sys.exit(1)

# ─────────────────────────────────────────────
#  CONSTANTS
# ─────────────────────────────────────────────
FUZZY_THRESHOLD   = 0.68   # SequenceMatcher ratio minimal chegara
CACHE_TTL_SEC     = 300    # 5 daqiqa
MAX_ROWS_RETURNED = 15

# ─────────────────────────────────────────────
#  AGGREGATE KEYWORD MAPS  (O'zbek + Rus + Eng)
# ─────────────────────────────────────────────
AGG_KEYWORDS: Dict[str, List[str]] = {
    "sum":   ["jami", "summa", "umumiy", "общий", "итого", "total", "sum"],
    "max":   ["maksimal", "eng yuqori", "максимальный", "наибольший",
              "maximum", "max", "highest"],
    "min":   ["minimal", "eng past", "минимальный", "наименьший",
              "minimum", "min", "lowest"],
    "mean":  ["o'rtacha", "ortacha", "средний", "среднее",
              "average", "mean", "avg"],
    "count": ["nechta", "soni", "количество", "count", "how many"],
}

# ─────────────────────────────────────────────
#  IN-MEMORY CACHE
# ─────────────────────────────────────────────
_CACHE: Dict[str, Tuple[pd.DataFrame, float]] = {}


def cache_get(key: str) -> Optional[pd.DataFrame]:
    entry = _CACHE.get(key)
    if entry and (time.time() - entry[1]) < CACHE_TTL_SEC:
        return entry[0]
    if key in _CACHE:
        del _CACHE[key]   # TTL muddati o'tdi — tozalash
    return None


def cache_set(key: str, df: pd.DataFrame) -> None:
    _CACHE[key] = (df, time.time())


def cache_clear_all() -> int:
    """Barcha kesh yozuvlarini o'chiradi. O'chirilgan soni qaytaradi."""
    n = len(_CACHE)
    _CACHE.clear()
    return n


# ─────────────────────────────────────────────
#  TEXT NORMALIZATION
# ─────────────────────────────────────────────
_UZ_RU_AFFIXES = re.compile(
    r"(ning|ga|da|dan|ni|lar|larni|larga|lardan|larning|"
    r"ни|ла|нинг|га|дан|да|лар)$",
    re.IGNORECASE | re.UNICODE,
)


def normalize(text: str) -> str:
    """
    1. Unicode NFC normalizatsiya (ё, ў, қ, ғ, ҳ to'g'ri ko'rinishi)
    2. Kichik harfga o'tkazish
    3. Tirnoqlar, ortiqcha belgilarni olib tashlash
    4. Ko'p bo'shliqlarni birlashtirish
    """
    text = unicodedata.normalize("NFC", str(text)).lower().strip()
    text = re.sub(r"[\"'`«»]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text


def tokenize(query: str) -> List[str]:
    """So'zlarni ajratib, har birini normallashtiradi va affiks tozalaydi."""
    tokens = []
    for tok in normalize(query).split():
        tok = _UZ_RU_AFFIXES.sub("", tok).strip()
        if len(tok) >= 2:
            tokens.append(tok)
    return tokens


# ─────────────────────────────────────────────
#  FUZZY MATCHING  (difflib — tashqi paket yo'q)
# ─────────────────────────────────────────────

def fuzzy_score(a: str, b: str) -> float:
    """SequenceMatcher koeffitsienti [0.0 .. 1.0]"""
    return SequenceMatcher(None, a, b).ratio()


def token_matches_cell(token: str, cell_raw: str) -> bool:
    """
    Token hujayra qiymatiga mos keladimi?
    Bosqichma-bosqich tekshirish:
      1. Aniq substring — tez va ishonchli
      2. So'z-bo'yicha fuzzy  (masalan: "yusupov" ↔ "Yusupov Jasur")
      3. Qisqartma/typo fuzzy  (masalan: "tixstil" ↔ "tekstil")
    """
    cell_norm = normalize(cell_raw)

    # 1. Aniq substring
    if token in cell_norm:
        return True

    # 2. Har bir so'z bilan solishtirish
    for word in cell_norm.split():
        if fuzzy_score(token, word) >= FUZZY_THRESHOLD:
            return True

    # 3. Uzunroq token uchun butun hujayra bilan solishtirish
    if len(token) >= 4 and fuzzy_score(token, cell_norm) >= FUZZY_THRESHOLD:
        return True

    return False


def row_matches_any_token(row: pd.Series, tokens: List[str]) -> bool:
    """Qatorda kamida bitta token mos kelsa — True."""
    for val in row:
        if pd.isna(val):
            continue
        for tok in tokens:
            if token_matches_cell(tok, str(val)):
                return True
    return False


# ─────────────────────────────────────────────
#  COLUMN-AWARE ANALYTICS
# ─────────────────────────────────────────────

def detect_agg_operation(query: str) -> Optional[str]:
    """Query matni ichidan sum/max/min/mean/count kalit so'zini topadi."""
    q_norm = normalize(query)
    for op, keywords in AGG_KEYWORDS.items():
        for kw in keywords:
            if kw in q_norm:
                return op
    return None


def find_target_numeric_columns(df: pd.DataFrame, query: str) -> List[str]:
    """
    Quyidagi tartibda raqamli ustunlarni tanlaydi:
      1. Query tokenlariga fuzzy mos keladigan raqamli ustun nomi
      2. Hech biri mos kelmasa — barcha raqamli ustunlar
    """
    tokens = tokenize(query)
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if not num_cols:
        return []

    matched = [
        col for col in num_cols
        if any(token_matches_cell(tok, col) for tok in tokens)
    ]
    return matched if matched else num_cols


def compute_aggregation(df: pd.DataFrame, query: str) -> str:
    """Agregatsiya operatsiyasini bajaradi va natijani string sifatida qaytaradi."""
    op = detect_agg_operation(query)
    if op is None:
        return ""

    cols = find_target_numeric_columns(df, query)
    if not cols:
        return ""

    lines: List[str] = []
    for col in cols:
        series = df[col].dropna()
        if series.empty:
            continue
        try:
            if op == "sum":
                val = series.sum()
                lines.append(f"[SUM]  {col} -> Jami: {val:,.2f}")
            elif op == "max":
                val = series.max()
                idx = series.idxmax()
                extra = {k: v for k, v in df.loc[idx].items() if k != col}
                lines.append(f"[MAX]  {col} -> Maksimal: {val:,.2f}  |  {extra}")
            elif op == "min":
                val = series.min()
                idx = series.idxmin()
                extra = {k: v for k, v in df.loc[idx].items() if k != col}
                lines.append(f"[MIN]  {col} -> Minimal: {val:,.2f}  |  {extra}")
            elif op == "mean":
                val = series.mean()
                lines.append(f"[AVG]  {col} -> O'rtacha: {val:,.2f}")
            elif op == "count":
                lines.append(f"[CNT]  {col} -> Soni: {len(series)}")
        except Exception:
            pass

    return "\n".join(lines) if lines else ""


# ─────────────────────────────────────────────
#  MAIN SUMMARIZER
# ─────────────────────────────────────────────

def summarize(df: pd.DataFrame, query: str) -> str:
    """
    Qidiruv strategiyasi:
      0. Query bo'sh → jadval meta-ma'lumotini qaytaradi
      1. Agregatsiya kalit so'zi → matematik natija
      2. Fuzzy row-search → mos qatorlar
      3. Mos qator yo'q → xato xabar
    """
    if not query.strip():
        cols_preview = ", ".join(df.columns.tolist()[:10])
        extra = "..." if len(df.columns) > 10 else ""
        return (f"Jadval: {df.shape[0]} qator x {df.shape[1]} ustun\n"
                f"Ustunlar: {cols_preview}{extra}")

    # 1. Aggregate
    agg_result = compute_aggregation(df, query)
    if agg_result:
        return agg_result

    # 2. Fuzzy row search
    tokens = tokenize(query)
    if not tokens:
        return f"Query tokenlanmadi. Jadval: {len(df)} qator"

    matched = df[df.apply(lambda r: row_matches_any_token(r, tokens), axis=1)]

    # 3. Fallback: butun query bilan bir qatamlik fuzzy
    if matched.empty:
        full_q = " ".join(tokens)
        matched = df[df.apply(
            lambda r: any(
                fuzzy_score(full_q, normalize(str(v))) >= FUZZY_THRESHOLD
                for v in r if not pd.isna(v)
            ),
            axis=1,
        )]

    if matched.empty:
        return (f"[NOT FOUND] Mos qator topilmadi.\n"
                f"Qidiruv: '{query}' | Jadval: {len(df)} qator")

    rows   = matched.head(MAX_ROWS_RETURNED)
    cols   = list(rows.columns)
    lines  = [" | ".join(f"{c}: {r[c]}" for c in cols)
              for _, r in rows.iterrows()]
    suffix = (f"\n... va yana {len(matched) - MAX_ROWS_RETURNED} qator"
              if len(matched) > MAX_ROWS_RETURNED else "")

    return "\n".join(lines) + suffix


# ─────────────────────────────────────────────
#  GOOGLE SHEETS URL PARSER
# ─────────────────────────────────────────────

def sheets_url_to_csv(url: str) -> str:
    """
    Google Sheets URL → export CSV URL
    Qo'llab-quvvatlanadigan formatlar:
      • .../spreadsheets/d/{ID}/edit#gid={GID}
      • .../spreadsheets/d/{ID}/edit?gid={GID}&...
      • .../spreadsheets/d/{ID}/pub?gid={GID}&single=true&output=csv
      • To'g'ridan-to'g'ri CSV URL (o'zgartirilmaydi)
    """
    if "docs.google.com/spreadsheets" not in url:
        return url   # CSV yoki boshqa to'g'ridan-to'g'ri URL

    # Spreadsheet ID — /d/ dan keyingi alfanumerik qism
    m_id = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", url)
    if not m_id:
        raise ValueError("Google Sheets URL dan spreadsheet ID ajratib bo'lmadi")
    doc_id = m_id.group(1)

    # Sheet GID (ixtiyoriy)
    m_gid = re.search(r"[#?&]gid=(\d+)", url)
    gid = m_gid.group(1) if m_gid else None

    export_url = (
        f"https://docs.google.com/spreadsheets/d/{doc_id}/export?format=csv"
    )
    if gid:
        export_url += f"&gid={gid}"
    return export_url


# ─────────────────────────────────────────────
#  TOOL HANDLERS
# ─────────────────────────────────────────────

def analyze_excel(params: Dict[str, Any]) -> str:
    file_path  = str(params.get("file_path", "")).strip()
    user_query = str(params.get("user_query", ""))
    sheet_name = params.get("sheet_name", 0)

    if not file_path:
        return "error: 'file_path' parametri majburiy"

    cache_key = f"excel::{file_path}::{sheet_name}"
    df = cache_get(cache_key)

    if df is None:
        try:
            if file_path.lower().endswith(".csv"):
                df = pd.read_csv(file_path, encoding="utf-8-sig")
            else:
                df = pd.read_excel(file_path, sheet_name=sheet_name,
                                   engine="openpyxl")
            cache_set(cache_key, df)
        except Exception as exc:
            return f"error: fayl o'qib bo'lmadi — {exc}"

    return summarize(df, user_query)


def analyze_sheets(params: Dict[str, Any]) -> str:
    sheet_url  = str(params.get("sheet_url", "")).strip()
    user_query = str(params.get("user_query", ""))
    timeout    = int(params.get("timeout", 20))

    if not sheet_url:
        return "error: 'sheet_url' parametri majburiy"

    try:
        csv_url = sheets_url_to_csv(sheet_url)
    except ValueError as exc:
        return f"error: {exc}"

    cache_key = f"sheets::{csv_url}"
    df = cache_get(cache_key)

    if df is None:
        try:
            headers = {"User-Agent": "OnBrain-MCP/2.0"}
            resp = requests.get(csv_url, timeout=timeout, headers=headers)
            resp.raise_for_status()
            df = pd.read_csv(StringIO(resp.text))
            cache_set(cache_key, df)
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else "?"
            return (f"error: HTTP {code} — jadval ochiq (public) emasmi? "
                    f"URL: {csv_url}")
        except Exception as exc:
            return f"error: ma'lumot olib bo'lmadi — {exc}"

    return summarize(df, user_query)


def handle_cache_clear(params: Dict[str, Any]) -> str:  # noqa: ARG001
    n = cache_clear_all()
    return f"[OK] Kesh tozalandi - {n} ta yozuv o'chirildi."


# ─────────────────────────────────────────────
#  DISPATCH TABLE
# ─────────────────────────────────────────────
HANDLERS: Dict[str, Any] = {
    "analyze_excel":  analyze_excel,
    "analyze_sheets": analyze_sheets,
    "cache_clear":    handle_cache_clear,
}


# ─────────────────────────────────────────────
#  STDIO MAIN LOOP
# ─────────────────────────────────────────────

def main_loop() -> None:
    """Stdin'dan JSON-RPC xabarlarni o'qiydi, javoblarni stdout'ga chiqaradi."""
    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue

        mid: Any = None
        try:
            msg    = json.loads(line)
            mid    = msg.get("id")
            method = msg.get("method", "")
            params = msg.get("params") or {}

            if method == "quit":
                print(json.dumps({"id": mid, "result": "ok"}), flush=True)
                break

            handler = HANDLERS.get(method)
            if handler is None:
                print(json.dumps(
                    {"id": mid, "error": f"Noma'lum metod: '{method}'"}),
                    flush=True)
                continue

            result = handler(params)
            print(json.dumps({"id": mid, "result": result},
                             ensure_ascii=False), flush=True)

        except json.JSONDecodeError:
            print(json.dumps({"id": mid, "error": "invalid json"}), flush=True)
        except Exception as exc:
            print(json.dumps({"id": mid,
                              "error": str(exc),
                              "trace": traceback.format_exc()},
                             ensure_ascii=False), flush=True)


# ─────────────────────────────────────────────
#  BOT INTEGRATION HELPER
#  ─────────────────────────────────────────
#  bot.py ichida import:
#      from mcp_server import call_mcp_tool
#
#  Chaqiruv:
#      result = await call_mcp_tool(
#          "analyze_sheets",
#          sheet_url="https://docs.google.com/spreadsheets/d/...",
#          user_query="Yusupov umumiy ball",
#      )
# ─────────────────────────────────────────────

import asyncio
import os
import subprocess as _subprocess


async def call_mcp_tool(method: str, **params: Any) -> str:
    """
    mcp_server.py'ni subprocess sifatida ishga tushirib,
    bitta JSON-RPC so'rovini yuboradi va natijani qaytaradi.

    • asyncio run_in_executor orqali event loop bloklanmaydi
    • try-except bilan to'liq himoyalangan
    • Timeout: 30 sekund

    Parameters
    ----------
    method : str
        "analyze_excel", "analyze_sheets" yoki "cache_clear"
    **params :
        Tegishli parametrlar (file_path, sheet_url, user_query, ...)

    Returns
    -------
    str
        Natija matni yoki "error: ..." xabar
    """
    python_exec = sys.executable
    server_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "mcp_server.py"
    )
    payload = (
        json.dumps({"id": 1, "method": method, "params": params},
                   ensure_ascii=False)
        + "\nquit\n"
    )

    def _run_sync() -> str:
        try:
            proc = _subprocess.run(
                [python_exec, server_path],
                input=payload,
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=30,
            )
            stdout = proc.stdout.strip()
            if not stdout:
                stderr = proc.stderr.strip()[:300]
                return f"error: MCP server javob bermadi. stderr={stderr}"

            for line in stdout.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    resp = json.loads(line)
                    if "result" in resp:
                        return str(resp["result"])
                    if "error" in resp:
                        return f"error: {resp['error']}"
                except json.JSONDecodeError:
                    continue

            return f"error: javob tahlil qilib bo'lmadi: {stdout[:200]}"

        except _subprocess.TimeoutExpired:
            return "error: MCP server 30 sekundda javob bermadi (timeout)"
        except Exception as exc:
            return f"error: subprocess xatosi — {exc}"

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _run_sync)


# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    main_loop()
