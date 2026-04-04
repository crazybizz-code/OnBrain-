"""
drive_service.py — Google Drive Service Account integration for OnBrain AI
===========================================================================
Handles:
  - Folder ID extraction from any Drive URL format
  - Listing files inside a folder (with pagination)
  - Downloading Google Sheets rows via Sheets API
  - Downloading Excel/XLSX files via Drive API
  - Permission error detection with friendly Uzbek messages

Service Account email:
  onbrain-ai@onbrain-ai-489203.iam.gserviceaccount.com

Env vars expected:
  GOOGLE_SA_CREDENTIALS  — full JSON of the service account key file (as a string)
  GOOGLE_SA_KEY_FILE     — path to the key file on disk (alternative)

Usage:
  from drive_service import DriveService
  svc = DriveService.from_env()
  result = await svc.process_folder(folder_url)
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any

import openpyxl
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload

logger = logging.getLogger("onbrain.drive_service")

# ──────────────────────────────────────────────
# MIME type constants
# ──────────────────────────────────────────────
MIME_GSHEET  = "application/vnd.google-apps.spreadsheet"
MIME_XLSX    = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
MIME_XLS     = "application/vnd.ms-excel"
MIME_CSV     = "text/csv"
MIME_FOLDER  = "application/vnd.google-apps.folder"

# Google Sheets/Drive API scopes
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

# The service account email — shown to users when access is denied
SERVICE_ACCOUNT_EMAIL = "onbrain-ai@onbrain-ai-489203.iam.gserviceaccount.com"

# ──────────────────────────────────────────────
# Result types
# ──────────────────────────────────────────────

@dataclass
class FileResult:
    """Rows read from one file in the folder."""
    name: str                             # Original file name
    rows: list[list[Any]] = field(default_factory=list)
    error: str | None = None             # Non-fatal per-file error


@dataclass
class FolderResult:
    """Aggregated result of processing a Drive folder."""
    files: list[FileResult] = field(default_factory=list)
    error: str | None = None            # Fatal error (no files processed)

    @property
    def ok(self) -> bool:
        return self.error is None

    @property
    def total_rows(self) -> int:
        return sum(len(f.rows) for f in self.files if f.rows)


# ──────────────────────────────────────────────
# URL / ID helpers
# ──────────────────────────────────────────────

def extract_folder_id(url: str) -> str | None:
    """
    Extract the folder ID from any Google Drive folder URL.

    Supported formats:
      • https://drive.google.com/drive/folders/FOLDER_ID
      • https://drive.google.com/drive/folders/FOLDER_ID?usp=sharing
      • https://drive.google.com/drive/u/0/folders/FOLDER_ID
      • https://drive.google.com/folders/FOLDER_ID
      • https://drive.google.com/open?id=FOLDER_ID
      • Raw folder ID (33-44 alphanumeric chars)
    """
    if not url:
        return None

    url = url.strip()

    patterns = [
        # Standard /drive/folders/<ID>  (with optional user index)
        r'drive\.google\.com/drive/(?:u/\d+/)?folders/([a-zA-Z0-9_-]{10,})',
        # Short /folders/<ID>
        r'drive\.google\.com/folders/([a-zA-Z0-9_-]{10,})',
        # open?id= format
        r'drive\.google\.com/open\?id=([a-zA-Z0-9_-]{10,})',
        # Bare ID pasted directly
        r'^([a-zA-Z0-9_-]{25,})$',
    ]

    for pattern in patterns:
        m = re.search(pattern, url)
        if m:
            return m.group(1)

    return None


def is_folder_url(text: str) -> bool:
    """Return True if *text* looks like a Google Drive folder link."""
    if not text:
        return False
    lower = text.lower()
    return (
        "drive.google.com" in lower
        and any(x in lower for x in ("folders/", "open?id="))
        and "spreadsheets" not in lower   # exclude Sheets links
    )


# ──────────────────────────────────────────────
# Main service class
# ──────────────────────────────────────────────

class DriveService:
    """
    Service Account–based Google Drive integration.

    Instantiate via DriveService.from_env() or DriveService(credentials_json=...).
    """

    def __init__(self, credentials_json: str | dict | None = None,
                 key_file: str | None = None):
        """
        Build the service.  Provide either:
          - credentials_json: the full service account JSON (string or dict)
          - key_file:         path to the .json key file on disk
        """
        if credentials_json:
            if isinstance(credentials_json, str):
                info = json.loads(credentials_json)
            else:
                info = credentials_json
            creds = service_account.Credentials.from_service_account_info(
                info, scopes=SCOPES
            )
        elif key_file:
            creds = service_account.Credentials.from_service_account_file(
                key_file, scopes=SCOPES
            )
        else:
            raise ValueError(
                "Provide credentials_json or key_file to DriveService."
            )

        self._creds   = creds
        self._drive   = build("drive",  "v3", credentials=creds, cache_discovery=False)
        self._sheets  = build("sheets", "v4", credentials=creds, cache_discovery=False)

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_env(cls) -> "DriveService":
        """
        Create from environment variables:
          GOOGLE_SA_CREDENTIALS  — full JSON string
          GOOGLE_SA_KEY_FILE     — path to .json key file (fallback)
        """
        sa_json = os.getenv("GOOGLE_SA_CREDENTIALS", "").strip()
        sa_file = os.getenv("GOOGLE_SA_KEY_FILE", "").strip()

        if sa_json:
            return cls(credentials_json=sa_json)
        if sa_file and os.path.isfile(sa_file):
            return cls(key_file=sa_file)
        raise EnvironmentError(
            "Set GOOGLE_SA_CREDENTIALS or GOOGLE_SA_KEY_FILE env var."
        )

    @classmethod
    def available(cls) -> bool:
        """Return True if service account credentials are configured."""
        return bool(
            os.getenv("GOOGLE_SA_CREDENTIALS", "").strip()
            or os.getenv("GOOGLE_SA_KEY_FILE", "").strip()
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def process_folder(self, folder_url: str) -> FolderResult:
        """
        Main entry point.  Given a Drive folder URL:
          1. Extract folder ID
          2. List all supported files (Sheets, XLSX, XLS, CSV)
          3. Read each file into rows
          4. Return FolderResult

        Never raises — errors are captured inside FolderResult.
        """
        folder_id = extract_folder_id(folder_url)
        if not folder_id:
            return FolderResult(
                error=(
                    "❌ Google Drive papka ID-ni ajratib ola olmadim.\n\n"
                    "Quyidagi formatlardan birini yuboring:\n"
                    "• <code>https://drive.google.com/drive/folders/FOLDER_ID</code>\n"
                    "• <code>https://drive.google.com/folders/FOLDER_ID</code>\n"
                    "• <code>https://drive.google.com/open?id=FOLDER_ID</code>"
                )
            )

        logger.info("📁 Processing Drive folder %s", folder_id)

        try:
            raw_files = await asyncio.to_thread(self._list_files, folder_id)
        except HttpError as exc:
            return FolderResult(error=self._http_error_message(exc))
        except Exception as exc:
            logger.error("list_files failed: %s", exc, exc_info=True)
            return FolderResult(error=f"❌ Papkani o'qib bo'lmadi: {exc}")

        supported = [
            f for f in raw_files
            if f["mimeType"] in (MIME_GSHEET, MIME_XLSX, MIME_XLS, MIME_CSV)
        ]
        skipped = len(raw_files) - len(supported)

        if not raw_files:
            return FolderResult(
                error=(
                    "📂 Papka bo'sh yoki bot uchun ruxsat yo'q.\n\n"
                    f"Iltimos, papkani quyidagi email bilan ulashing:\n"
                    f"<code>{SERVICE_ACCOUNT_EMAIL}</code>\n\n"
                    "Drive → Papka → Share → yuqoridagi email qo'shing."
                )
            )

        if not supported:
            mime_list = ", ".join(set(f["mimeType"].split(".")[-1] for f in raw_files))
            return FolderResult(
                error=(
                    f"📂 Papkada {len(raw_files)} ta fayl bor, lekin ularning "
                    f"hech biri qo'llab-quvvatlanmaydi ({mime_list}).\n\n"
                    "Qo'llab-quvvatlanadigan turlar:\n"
                    "• Google Sheets\n• Excel (.xlsx, .xls)\n• CSV"
                )
            )

        logger.info(
            "📊 %d supported files (%d skipped) in folder %s",
            len(supported), skipped, folder_id,
        )

        # Read each file concurrently (up to 5 at a time to avoid quota burst)
        sem = asyncio.Semaphore(5)

        async def _bounded(file_meta: dict) -> FileResult:
            async with sem:
                return await self._read_file(file_meta)

        file_results = await asyncio.gather(*[_bounded(f) for f in supported])
        return FolderResult(files=list(file_results))

    # ------------------------------------------------------------------
    # Internal: list files in folder
    # ------------------------------------------------------------------

    def _list_files(self, folder_id: str) -> list[dict]:
        """
        Return all non-trashed files directly inside *folder_id*.
        Handles pagination automatically.
        """
        query = (
            f"'{folder_id}' in parents"
            " and trashed = false"
            f" and mimeType != '{MIME_FOLDER}'"   # skip sub-folders
        )
        fields = "nextPageToken, files(id, name, mimeType, size)"
        files: list[dict] = []
        page_token: str | None = None

        while True:
            resp = (
                self._drive.files()
                .list(
                    q=query,
                    spaces="drive",
                    pageSize=100,
                    pageToken=page_token,
                    fields=fields,
                    orderBy="name",
                )
                .execute()
            )
            files.extend(resp.get("files", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        logger.info("📁 Listed %d files in folder %s", len(files), folder_id)
        return files

    # ------------------------------------------------------------------
    # Internal: read one file
    # ------------------------------------------------------------------

    async def _read_file(self, meta: dict) -> FileResult:
        name     = meta["name"]
        file_id  = meta["id"]
        mime     = meta["mimeType"]

        try:
            if mime == MIME_GSHEET:
                rows = await asyncio.to_thread(self._read_gsheet, file_id)
            elif mime in (MIME_XLSX, MIME_XLS):
                raw = await asyncio.to_thread(self._download_file, file_id)
                rows = _parse_xlsx(raw)
            elif mime == MIME_CSV:
                raw = await asyncio.to_thread(self._download_file, file_id)
                rows = _parse_csv(raw)
            else:
                return FileResult(name=name, error=f"Unsupported mime: {mime}")

            logger.info("✅ Read '%s': %d rows", name, len(rows))
            return FileResult(name=name, rows=rows)

        except HttpError as exc:
            msg = self._http_error_message(exc)
            logger.warning("⚠️ %s: %s", name, msg)
            return FileResult(name=name, error=msg)
        except Exception as exc:
            logger.warning("⚠️ %s: %s", name, exc, exc_info=True)
            return FileResult(name=name, error=str(exc))

    # ------------------------------------------------------------------
    # Internal: Google Sheets read
    # ------------------------------------------------------------------

    def _read_gsheet(self, sheet_id: str) -> list[list[Any]]:
        """
        Read the first sheet of a Google Spreadsheet via the Sheets API.
        Returns rows as list[list[Any]].
        """
        # Get spreadsheet metadata to find sheet names
        meta = (
            self._sheets.spreadsheets()
            .get(spreadsheetId=sheet_id, fields="sheets.properties")
            .execute()
        )
        sheets = meta.get("sheets", [])
        if not sheets:
            return []

        # Read the first worksheet
        sheet_title = sheets[0]["properties"]["title"]
        result = (
            self._sheets.spreadsheets()
            .values()
            .get(spreadsheetId=sheet_id, range=sheet_title)
            .execute()
        )
        return result.get("values", [])

    def _read_all_gsheet_tabs(self, sheet_id: str) -> dict[str, list[list[Any]]]:
        """
        Read ALL tabs from a Google Spreadsheet.
        Returns {tab_name: rows}.
        """
        meta = (
            self._sheets.spreadsheets()
            .get(spreadsheetId=sheet_id, fields="sheets.properties")
            .execute()
        )
        all_tabs: dict[str, list[list[Any]]] = {}
        for sheet in meta.get("sheets", []):
            title = sheet["properties"]["title"]
            try:
                result = (
                    self._sheets.spreadsheets()
                    .values()
                    .get(spreadsheetId=sheet_id, range=title)
                    .execute()
                )
                all_tabs[title] = result.get("values", [])
            except Exception as e:
                logger.warning("⚠️ Could not read tab '%s': %s", title, e)
                all_tabs[title] = []
        return all_tabs

    # ------------------------------------------------------------------
    # Internal: binary file download
    # ------------------------------------------------------------------

    def _download_file(self, file_id: str) -> bytes:
        """Download a binary file from Drive and return its bytes."""
        request = self._drive.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request, chunksize=4 * 1024 * 1024)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buf.getvalue()

    # ------------------------------------------------------------------
    # Error formatting
    # ------------------------------------------------------------------

    @staticmethod
    def _http_error_message(exc: HttpError) -> str:
        status = exc.resp.status if exc.resp else 0
        if status == 403:
            return (
                "🔒 <b>Ruxsat rad etildi (403)</b>\n\n"
                "Papkani quyidagi Service Account email bilan ulashing:\n"
                f"<code>{SERVICE_ACCOUNT_EMAIL}</code>\n\n"
                "<b>Qanday qilish kerak:</b>\n"
                "1. Google Drive → Papka → ⋮ → Share\n"
                "2. Yuqoridagi emailni qo'shing → Viewer\n"
                "3. Havolani qayta yuboring."
            )
        if status == 404:
            return (
                "❌ Papka topilmadi (404).\n"
                "Havolani tekshiring yoki papka o'chirilgan bo'lishi mumkin."
            )
        if status == 429:
            return "⏳ API cheklovi (429). Bir daqiqadan keyin qayta urinib ko'ring."
        return f"❌ Google Drive xatosi ({status}): {exc}"


# ──────────────────────────────────────────────
# File parsers
# ──────────────────────────────────────────────

def _parse_xlsx(raw: bytes) -> list[list[Any]]:
    """Parse XLSX bytes into rows using openpyxl."""
    wb = openpyxl.load_workbook(io.BytesIO(raw), data_only=True, read_only=True)
    ws = wb.active
    rows = []
    for row in ws.iter_rows(values_only=True):
        rows.append(list(row))
    wb.close()
    return rows


def _parse_csv(raw: bytes) -> list[list[Any]]:
    """Parse CSV bytes into rows."""
    import csv
    text = raw.decode("utf-8-sig", errors="replace")
    reader = csv.reader(text.splitlines())
    return [row for row in reader]


# ──────────────────────────────────────────────
# Standalone helpers (for use in bot.py)
# ──────────────────────────────────────────────

async def process_drive_folder(folder_url: str) -> FolderResult:
    """
    Convenience function — creates DriveService from env and processes folder.
    Returns FolderResult with .error set if service account is not configured.
    """
    if not DriveService.available():
        return FolderResult(
            error=(
                "🔐 <b>Papkani botga ulash uchun:</b>\n\n"
                "Quyidagi emailga <b>Viewer</b> huquqi bering:\n"
                f"<code>{SERVICE_ACCOUNT_EMAIL}</code>\n\n"
                "<b>Qanday qilish kerak:</b>\n"
                "1. Google Drive → Papkangizni toping\n"
                "2. ⋮ → Share (Ulashish)\n"
                "3. Yuqoridagi emailni qo'shing → Viewer\n"
                "4. Papka havolasini qayta yuboring."
            )
        )
    try:
        svc = DriveService.from_env()
        return await svc.process_folder(folder_url)
    except Exception as exc:
        logger.error("process_drive_folder failed: %s", exc, exc_info=True)
        return FolderResult(error=f"❌ Drive xizmati ishga tushmadi: {exc}")
