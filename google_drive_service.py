"""
Google Drive Service for OnBrain AI
Handles reading all spreadsheets from a Google Drive folder
"""

import logging
from typing import Optional, Dict, List, Any
import asyncio
import gspread
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


class GoogleDriveService:
    """Service to interact with Google Drive API and read spreadsheets"""
    
    def __init__(self, credentials: Credentials):
        """Initialize with Google OAuth credentials"""
        self.credentials = credentials
        self.drive_service = build('drive', 'v3', credentials=credentials)
        self.sheets_service = build('sheets', 'v4', credentials=credentials)
        self.gspread_client = gspread.authorize(credentials)
    
    async def get_folder_spreadsheets(self, folder_id: str) -> List[Dict[str, str]]:
        """
        Get all spreadsheets in a Google Drive folder
        
        Args:
            folder_id: Google Drive folder ID
            
        Returns:
            List of dicts with spreadsheet info: [{'id': '...', 'name': '...', 'url': '...'}]
        """
        try:
            # Try using gspread to list files (works better with shared files)
            logger.info(f"🔍 Searching for Google Sheets in folder {folder_id} using Drive API...")
            
            # First try with Drive API query
            results = await asyncio.to_thread(
                self._list_spreadsheets_sync,
                f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
            )
            
            if results:
                logger.info(f"📊 Found {len(results)} Google Sheets using Drive API")
                return results
            
            # If Drive API fails, try to list files and show all
            logger.warning(f"⚠️ No spreadsheets found with Drive API. Trying to list all files...")
            all_files = await asyncio.to_thread(
                self._list_all_files_sync,
                f"'{folder_id}' in parents and trashed=false"
            )
            
            logger.info(f"📁 Found {len(all_files)} total files in folder")
            for f in all_files:
                logger.info(f"   - {f['name']} (Type: {f['mimeType']})")
            
            return results
            
        except HttpError as error:
            logger.error(f"❌ Google Drive API error: {error}")
            raise
        except Exception as e:
            logger.error(f"❌ Error getting folder spreadsheets: {e}", exc_info=True)
            raise
    
    def _list_spreadsheets_sync(self, query: str) -> List[Dict[str, str]]:
        """Synchronous method to list spreadsheets"""
        spreadsheets = []
        page_token = None
        
        try:
            logger.info(f"🔍 Executing query: {query}")
            
            while True:
                results = self.drive_service.files().list(
                    q=query,
                    spaces='drive',
                    pageSize=50,
                    pageToken=page_token,
                    fields='files(id, name, webViewLink, mimeType)',
                    orderBy='name'
                ).execute()
                
                files = results.get('files', [])
                logger.info(f"📊 Found {len(files)} files in this page")
                
                for file in files:
                    logger.info(f"   - {file['name']} (ID: {file['id']}, Type: {file.get('mimeType', 'unknown')})")
                    spreadsheets.append({
                        'id': file['id'],
                        'name': file['name'],
                        'url': f"https://docs.google.com/spreadsheets/d/{file['id']}/edit"
                    })
                
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
        
        except Exception as e:
            logger.error(f"❌ Error listing spreadsheets: {e}", exc_info=True)
            raise
        
        logger.info(f"✅ Total spreadsheets found: {len(spreadsheets)}")
        return spreadsheets
    
    def _list_all_files_sync(self, query: str) -> List[Dict[str, str]]:
        """Synchronous method to list ALL files (for debugging)"""
        files_list = []
        page_token = None
        
        try:
            logger.info(f"🔍 DEBUG: Executing debug query: {query}")
            
            while True:
                results = self.drive_service.files().list(
                    q=query,
                    spaces='drive',
                    pageSize=50,
                    pageToken=page_token,
                    fields='files(id, name, mimeType)',
                    orderBy='name'
                ).execute()
                
                files = results.get('files', [])
                
                for file in files:
                    files_list.append({
                        'id': file['id'],
                        'name': file['name'],
                        'mimeType': file.get('mimeType', 'unknown')
                    })
                
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
        
        except Exception as e:
            logger.error(f"❌ Error listing all files: {e}", exc_info=True)
        
        return files_list

    async def extract_folder_id(self, folder_url: str) -> Optional[str]:
        """
        Extract folder ID from Google Drive folder URL
        
        Supports:
        - https://drive.google.com/drive/folders/FOLDER_ID
        - https://drive.google.com/drive/folders/FOLDER_ID?usp=sharing
        - https://drive.google.com/drive/folders/FOLDER_ID/edit
        - https://drive.google.com/drive/folders/FOLDER_ID/share
        - https://drive.google.com/drive/u/0/folders/FOLDER_ID
        - https://drive.google.com/folders/FOLDER_ID
        - https://drive.google.com/open?id=FOLDER_ID
        """
        import re
        
        # Clean the URL - remove /edit, /share, query params
        folder_url = re.sub(r'(/edit.*|/share.*|\?usp.*)$', '', folder_url)
        
        patterns = [
            r'drive\.google\.com/drive/(?:u/\d+/)?folders/([a-zA-Z0-9-_]+)',
            r'drive\.google\.com/folders/([a-zA-Z0-9-_]+)',
            r'drive\.google\.com/open\?id=([a-zA-Z0-9-_]+)',
            r'drive\.google\.com/drive/folders/([a-zA-Z0-9-_]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, folder_url)
            if match:
                folder_id = match.group(1)
                logger.info(f"✅ Extracted folder ID: {folder_id}")
                return folder_id
        
        logger.warning(f"⚠️ Could not extract folder ID from URL: {folder_url}")
        return None
    
    async def read_spreadsheet(self, sheet_id: str) -> Dict[str, List[List[str]]]:
        """
        Read all worksheets from a spreadsheet
        
        Args:
            sheet_id: Google Sheets ID
            
        Returns:
            Dict mapping sheet names to their data
        """
        try:
            # Use gspread to open spreadsheet
            workbook = await asyncio.to_thread(
                self.gspread_client.open_by_key,
                sheet_id
            )
            
            all_sheets = {}
            
            # Get all worksheets
            worksheets = await asyncio.to_thread(
                workbook.worksheets
            )
            
            for worksheet in worksheets:
                try:
                    # Get all values from worksheet
                    values = await asyncio.to_thread(
                        worksheet.get_all_values
                    )
                    all_sheets[worksheet.title] = values
                    logger.info(f"📊 Read sheet '{worksheet.title}' with {len(values)} rows")
                except Exception as e:
                    logger.warning(f"⚠️ Could not read sheet '{worksheet.title}': {e}")
                    all_sheets[worksheet.title] = []
            
            return all_sheets
            
        except Exception as e:
            logger.error(f"❌ Error reading spreadsheet {sheet_id}: {e}")
            raise
    
    async def read_multiple_spreadsheets(self, sheet_ids: List[str]) -> Dict[str, Dict[str, List[List[str]]]]:
        """
        Read multiple spreadsheets
        
        Args:
            sheet_ids: List of Google Sheets IDs
            
        Returns:
            Dict mapping sheet_id to their worksheets data
        """
        results = {}
        
        for sheet_id in sheet_ids:
            try:
                sheets_data = await self.read_spreadsheet(sheet_id)
                results[sheet_id] = sheets_data
                logger.info(f"✅ Successfully read spreadsheet {sheet_id}")
            except Exception as e:
                logger.error(f"❌ Failed to read spreadsheet {sheet_id}: {e}")
                results[sheet_id] = {}
        
        return results
    
    async def get_spreadsheet_metadata(self, sheet_id: str) -> Dict[str, Any]:
        """
        Get metadata about a spreadsheet (name, sheet count, etc.)
        """
        try:
            metadata = await asyncio.to_thread(
                self.sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute
            )
            
            sheet_count = len(metadata.get('sheets', []))
            
            return {
                'id': sheet_id,
                'title': metadata.get('properties', {}).get('title', 'Unknown'),
                'sheet_count': sheet_count,
                'sheets': [
                    {
                        'id': sheet['properties']['sheetId'],
                        'title': sheet['properties']['title'],
                        'grid_properties': sheet['properties'].get('gridProperties', {})
                    }
                    for sheet in metadata.get('sheets', [])
                ]
            }
        except Exception as e:
            logger.error(f"❌ Error getting spreadsheet metadata: {e}")
            raise


async def get_all_spreadsheets_from_folder(credentials: Credentials, folder_url: str) -> tuple[List[Dict], Optional[str]]:
    """
    Main function to get all spreadsheets from a folder
    
    Returns:
        (spreadsheets_list, error_message)
        - If successful: (list of dicts, None)
        - If error: ([], error_message)
    """
    try:
        drive_service = GoogleDriveService(credentials)
        
        # Extract folder ID from URL
        folder_id = await drive_service.extract_folder_id(folder_url)
        if not folder_id:
            return [], "📁 Klassik Google Drive papka havolasini jo'nating.\nMisol: https://drive.google.com/drive/folders/1ABC123xyz"
        
        # Get all spreadsheets in folder
        spreadsheets = await drive_service.get_folder_spreadsheets(folder_id)
        
        if not spreadsheets:
            logger.warning(f"⚠️ No spreadsheets found in folder {folder_id}")
            return [], "📁 Ushbu papkada spreadsheet topilmadi.\n\n✅ Yechim:\n1. Papka ichiga Google Sheets yoki Excel fayllar joylang\n2. Papka umumiy (Public) bo'lishini tekshiring\n3. Havolani qayta yuboring"
        
        logger.info(f"✅ Found {len(spreadsheets)} spreadsheets in folder")
        return spreadsheets, None
        
    except Exception as e:
        logger.error(f"❌ Error in get_all_spreadsheets_from_folder: {e}")
        return [], f"❌ Xatolik: {str(e)}\n\n💡 Google Drive papka havolasini to'g'ri qilib yuboring."
