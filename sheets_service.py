"""
sheets_service.py — Google Sheets reader for OnBrain AI
=========================================================
Two modes:
  1. Service Account (preferred for folders / no OAuth required)
  2. User OAuth credentials (backward-compatible with existing flow)

Public API:
  read_sheet_sa(sheet_id)              → dict[tab_name, rows]   (Service Account)
  read_sheet_oauth(creds, sheet_id)    → dict[tab_name, rows]   (OAuth)
  extract_sheet_id(url_or_id)          → str | None
  is_sheets_url(text)                  → bool
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger("onbrain.sheets_service")

# Service account scopes
_SA_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

SERVICE_ACCOUNT_EMAIL = "onbrain-ai@onbrain-ai-489203.iam.gserviceaccount.com"


# ──────────────────────────────────────────────
# URL helpers
# ──────────────────────────────────────────────

def extract_sheet_id(text: str) -> str | None:
    """
    Extract a Google Sheets spreadsheet ID from a URL or bare ID string.

    Handles:
      • https://docs.google.com/spreadsheets/d/<ID>/edit
      • https://docs.google.com/spreadsheets/d/e/<PUB_ID>/pubhtml
      • sheets.googleapis.com/v4/spreadsheets/<ID>
      • Bare 20–50 character alphanumeric IDs
    """
    if not text:
        return None
    text = text.strip()
    # Strip trailing /edit, /share, /export, query params
    text = re.sub(r'(/edit.*|/share.*|/export.*|\?.*|#.*)$', '', text)

    patterns = [
        r'docs\.google\.com/spreadsheets/d/([a-zA-Z0-9_-]{10,})',
        r'docs\.google\.com/spreadsheets/d/e/([a-zA-Z0-9_-]{10,})',
        r'sheets\.googleapis\.com/v4/spreadsheets/([a-zA-Z0-9_-]{10,})',
        r'^([a-zA-Z0-9_-]{20,})$',   # raw ID pasted directly
    ]

    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return m.group(1)
    return None


def is_sheets_url(text: str) -> bool:
    """Return True if *text* looks like a Google Sheets link."""
    if not text:
        return False
    lower = text.lower()
    return any(kw in lower for kw in (
        "docs.google.com/spreadsheets",
        "sheets.googleapis.com",
        "spreadsheets/d/",
        "sheets.google.com",
    ))


# ──────────────────────────────────────────────
# Service Account reader
# ──────────────────────────────────────────────

def _build_sa_sheets_service():
    """Build a Sheets API service using Service Account credentials from env."""
    import json
    sa_json = os.getenv("GOOGLE_SA_CREDENTIALS", "").strip()
    sa_file = os.getenv("GOOGLE_SA_KEY_FILE", "").strip()

    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=_SA_SCOPES
        )
    elif sa_file and os.path.isfile(sa_file):
        creds = service_account.Credentials.from_service_account_file(
            sa_file, scopes=_SA_SCOPES
        )
    else:
        raise EnvironmentError(
            "GOOGLE_SA_CREDENTIALS or GOOGLE_SA_KEY_FILE env var not set."
        )

    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _sa_available() -> bool:
    return bool(
        os.getenv("GOOGLE_SA_CREDENTIALS", "").strip()
        or os.getenv("GOOGLE_SA_KEY_FILE", "").strip()
    )


def _fetch_all_tabs_sync(service, sheet_id: str) -> dict[str, list[list[Any]]]:
    """
    Synchronous: read every tab of a spreadsheet.
    Returns {tab_name: rows}.
    """
    # Get sheet metadata
    meta = (
        service.spreadsheets()
        .get(spreadsheetId=sheet_id, fields="sheets.properties")
        .execute()
    )
    tabs: dict[str, list[list[Any]]] = {}

    for sheet in meta.get("sheets", []):
        title = sheet["properties"]["title"]
        try:
            result = (
                service.spreadsheets()
                .values()
                .get(spreadsheetId=sheet_id, range=title)
                .execute()
            )
            tabs[title] = result.get("values", [])
            logger.info("📊 Tab '%s': %d rows", title, len(tabs[title]))
        except Exception as e:
            logger.warning("⚠️ Could not read tab '%s': %s", title, e)
            tabs[title] = []

    return tabs


async def read_sheet_sa(sheet_id: str) -> dict[str, list[list[Any]]]:
    """
    Read all tabs of a Google Sheet using the Service Account.

    Returns:
        dict mapping tab name → list of rows (list[list[Any]])

    Raises:
        EnvironmentError  — if no service account credentials configured
        HttpError         — if the sheet is inaccessible (403/404)
        Exception         — other unexpected errors
    """
    service = _build_sa_sheets_service()
    return await asyncio.to_thread(_fetch_all_tabs_sync, service, sheet_id)


# ──────────────────────────────────────────────
# OAuth reader (backward-compatible)
# ──────────────────────────────────────────────

async def read_sheet_oauth(
    credentials,          # google.oauth2.credentials.Credentials
    sheet_id: str,
) -> dict[str, list[list[Any]]]:
    """
    Read all tabs of a Google Sheet using existing OAuth credentials.
    Wraps the synchronous gspread approach already used in bot.py.

    Returns:
        dict mapping tab name → rows
    """
    import gspread
    from google.auth.transport.requests import Request as _GReq

    def _sync() -> dict[str, list[list[Any]]]:
        if credentials.expired and credentials.refresh_token:
            credentials.refresh(_GReq())
        gc = gspread.authorize(credentials)
        wb = gc.open_by_key(sheet_id)
        result: dict[str, list[list[Any]]] = {}
        for ws in wb.worksheets():
            try:
                result[ws.title] = ws.get_all_values()
            except Exception as e:
                logger.warning("⚠️ Tab '%s': %s", ws.title, e)
                result[ws.title] = []
        return result

    return await asyncio.to_thread(_sync)


# ──────────────────────────────────────────────
# Convenience: auto-select mode
# ──────────────────────────────────────────────

async def read_sheet(
    sheet_id: str,
    *,
    credentials=None,
) -> tuple[dict[str, list[list[Any]]], str | None]:
    """
    Read a sheet using the best available method:
      - Service Account if configured
      - OAuth credentials if provided
      - Returns ({}, error_message) if neither is available

    Returns:
        (tabs_dict, error_message_or_None)
    """
    # Prefer service account (no OAuth needed)
    if _sa_available():
        try:
            tabs = await read_sheet_sa(sheet_id)
            return tabs, None
        except HttpError as exc:
            status = exc.resp.status if exc.resp else 0
            if status == 403:
                err = (
                    "🔒 <b>Ruxsat yo'q (403)</b>\n\n"
                    "Elektron jadvalini quyidagi email bilan ulashing:\n"
                    f"<code>{SERVICE_ACCOUNT_EMAIL}</code>\n\n"
                    "Sheets → Share → yuqoridagi emailni qo'shing."
                )
            elif status == 404:
                err = "❌ Elektron jadval topilmadi (404). Havola to'g'riligini tekshiring."
            else:
                err = f"❌ Google Sheets xatosi ({status}): {exc}"
            return {}, err
        except Exception as exc:
            logger.error("read_sheet_sa failed: %s", exc, exc_info=True)
            return {}, f"❌ Sheets o'qib bo'lmadi: {exc}"

    # Fall back to OAuth
    if credentials is not None:
        try:
            tabs = await read_sheet_oauth(credentials, sheet_id)
            return tabs, None
        except Exception as exc:
            logger.error("read_sheet_oauth failed: %s", exc, exc_info=True)
            return {}, f"❌ Sheets o'qib bo'lmadi: {exc}"

    return {}, (
        "⚙️ <b>Google Sheets ulanmagan</b>\n\n"
        "OAuth orqali ulanish uchun '📊 Google Sheets ulash' tugmasini bosing."
    )
