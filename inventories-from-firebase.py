import firebase_admin
from firebase_admin import credentials, firestore
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone
import json
import os
from dotenv import load_dotenv
import sys
import codecs
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from typing import List, Dict, Any, Optional, Tuple, Generator
import logging
from functools import lru_cache, partial
import threading
from itertools import islice
import gc
import math

# Configure optimized logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Ensure UTF-8 output
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, 'strict')
os.environ['PYTHONUNBUFFERED'] = '1'
os.environ['PYTHONIOENCODING'] = 'utf-8'

# Load environment variables
load_dotenv()

# Optimized Configuration
BATCH_SIZE = 5000  # Increased batch size for faster processing
MAX_WORKERS = min(32, (os.cpu_count() or 1) * 4)  # More workers for parallel processing
CHUNK_SIZE = 500  # Size of chunks for processing
SHEETS_BATCH_SIZE = 50000  # Maximum batch size for Sheets API

# Connection pooling
_connection_lock = threading.Lock()
_firebase_db = None
_sheets_service = None

# Header definition for Inventories
HEADERS = [
    "Property ID","CP ID","Property Name","QC ID","Asset Type","Sub Type",
    "Plot Size","Carpet (Sq Ft)","SBUA (Sq ft)","Facing","Total Ask Price (Lacs)",
    "Ask Price / Sqft","noOfBedrooms","Micromarket","Community Type","Extra Details","Floor No.",
    "Handover Date","Zone","Map Location","Date of inventory added","Date of status last checked",
    "Last Check","Drive link for more info","Building Khata","Land Khata","Building Age",
    "Age of Inventory","Age of Status","Status","Tenanted or Not",
    "OC Received or not","BDA Approved","BIAPPA Approved","Current Status","Coordinates",
    "Exclusive","Exact Floor","eKhata","Photos","Videos","Documents","Source","listingType",
    "Sold Price (Lacs)","Sold Date","KAM Info"
]

class FastDataProcessor:
    """High-performance data processor using optimized operations"""
    
    @staticmethod
    @lru_cache(maxsize=10000)
    def convert_unix_to_date_cached(ts: float) -> str:
        """Cached date conversion"""
        if not ts:
            return ""
        try:
            # Handle string input
            if isinstance(ts, str):
                try:
                    ts = float(ts)
                except ValueError:
                    return ts
            
            ts_float = float(ts)
            if ts_float > 9999999999:  # Milliseconds
                ts_float = ts_float / 1000
            
            return datetime.fromtimestamp(int(ts_float), tz=timezone.utc).strftime('%Y-%m-%d')
        except:
            return str(ts) if ts else ""

    @staticmethod
    @lru_cache(maxsize=10000)
    def convert_unix_to_datetime_cached(ts: float) -> str:
        """Cached datetime conversion"""
        if not ts:
            return ""
        try:
            # Handle string input
            if isinstance(ts, str):
                try:
                    ts = float(ts)
                except ValueError:
                    return ts
            
            ts_float = float(ts)
            if ts_float > 9999999999:  # Milliseconds
                ts_float = ts_float / 1000
                
            return datetime.fromtimestamp(int(ts_float), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        except:
            return str(ts) if ts else ""
            
    @staticmethod
    def format_price(price_value: Any) -> str:
        """Format price values (optimized)"""
        if not price_value:
            return ""
            
        try:
            # Handle dictionary/list (KAM info/Sold info)
            if isinstance(price_value, dict):
                return str(price_value.get('soldPrice', '') or "")
            if isinstance(price_value, list) and price_value and isinstance(price_value[0], dict):
                return str(price_value[0].get('soldPrice', '') or "")
                
            # Handle strings
            if isinstance(price_value, str):
                cleaned = price_value.replace('₹', '').replace(',', '').replace('Rs', '').replace('INR', '').strip()
                try:
                    return str(float(cleaned))
                except ValueError:
                    return price_value
                    
            # Handle numbers
            return str(float(price_value))
        except:
            return str(price_value) if price_value else ""

    @staticmethod
    def extract_kam_info(sold_data: Any) -> str:
        """Extract KAM information (optimized)"""
        if not sold_data:
            return ""
            
        try:
            data = None
            if isinstance(sold_data, dict):
                data = sold_data
            elif isinstance(sold_data, list) and sold_data and isinstance(sold_data[0], dict):
                data = sold_data[0]
                
            if data:
                kam_name = data.get('kamName', '')
                kam_id = data.get('kamId', '')
                platform = data.get('sellingPlatform', '')
                if kam_name or kam_id:
                    return f"{kam_name} ({kam_id})" + (f" - {platform}" if platform else "")
            
            return ""
        except:
            return ""

    @staticmethod
    def process_chunk(docs_chunk: List[Tuple[str, Dict]]) -> List[List[str]]:
        """Process a chunk of documents into rows"""
        processed = []
        processor = FastDataProcessor
        
        try:
            for idx, item in enumerate(docs_chunk):
                # Handle both tuple and dict formats
                if isinstance(item, tuple) and len(item) >= 2:
                    doc_id, item = item
                elif isinstance(item, dict):
                    pass
                else:
                    continue
                
                if not isinstance(item, dict):
                    continue
                
                # Pre-calculate complex fields
                pricing = item.get("pricing", {}) or {}
                media = item.get("media", {}) or {}
                geoloc = item.get("_geoloc", {}) or {}
                
                row = [
                    str(item.get("propertyId", "") or ""),
                    str(item.get("cpId", "") or ""),
                    str(item.get("propertyName", "") or ""),
                    str(item.get("qcId", "") or ""),
                    str(item.get("assetType", "") or ""),
                    str(item.get("subType", "") or ""),
                    str(item.get("plotArea", "") or ""),
                    str(item.get("carpet", "") or ""),
                    str(item.get("sbua", "") or ""),
                    str(item.get("facing", "") or ""),
                    processor.format_price(pricing.get("totalAskPrice", "")),
                    processor.format_price(pricing.get("pricePerSqft", "")),
                    str(item.get("noOfBedrooms", "") or ""),
                    str(item.get("micromarket", "") or ""),
                    str(item.get("communityType", "") or ""),
                    str(item.get("extraDetails", "") or ""),
                    str(item.get("floorNo", "") or ""),
                    processor.convert_unix_to_date_cached(item.get("handoverDate")),
                    str(item.get("zone", "") or ""),
                    str(item.get("mapLocation", "") or ""),
                    processor.convert_unix_to_date_cached(item.get("added")),
                    processor.convert_unix_to_date_cached(item.get("dateOfLastChecked")),
                    processor.convert_unix_to_datetime_cached(item.get("lastCheck")),
                    str(item.get("driveLink", "") or ""),
                    str(item.get("buildingKhata", "") or ""),
                    str(item.get("landKhata", "") or ""),
                    str(item.get("buildingAge", "") or ""),
                    str(item.get("ageOfInventory", "") or ""),
                    str(item.get("ageOfStatus", "") or ""),
                    str(item.get("status", "") or ""),
                    str(item.get("tenanted", "") or ""),
                    str(item.get("ocReceived", "") or ""),
                    str(item.get("bdaApproved", "") or ""),
                    str(item.get("biappaApproved", "") or ""),
                    str(item.get("currentStatus", "") or ""),
                    f"{geoloc.get('lat','')}, {geoloc.get('lng','')}" if geoloc else "",
                    str(item.get("exclusive", "") or ""),
                    str(item.get("exactFloor", "") or ""),
                    str(item.get("eKhata", "") or ""),
                    ", ".join(media.get("photos", [])) if isinstance(media.get("photos"), list) else str(media.get("photos", "") or ""),
                    ", ".join(media.get("videos", [])) if isinstance(media.get("videos"), list) else str(media.get("videos", "") or ""),
                    ", ".join(media.get("documents", [])) if isinstance(media.get("documents"), list) else str(media.get("documents", "") or ""),
                    str(item.get("source", "") or ""),
                    str(item.get("listingType", "") or ""),
                    processor.format_price(item.get("soldPrice", "")),
                    processor.convert_unix_to_date_cached(item.get("soldDate", "")),
                    processor.extract_kam_info(item.get("soldPrice", ""))
                ]
                
                # Sanitize logic
                sanitized_row = [
                    "" if (isinstance(cell, float) and math.isnan(cell)) or cell is None else str(cell)
                    for cell in row
                ]
                processed.append(sanitized_row)
        
        except Exception as e:
            logger.error(f"Error in process_chunk: {e}", exc_info=True)
            return []
        
        return processed

@lru_cache(maxsize=1)
def get_firebase_credentials() -> Dict:
    """Get Firebase credentials with caching"""
    return {
        "type": "service_account",
        "project_id": os.getenv("FIREBASE_PROJECT_ID"),
        "private_key_id": os.getenv("FIREBASE_PRIVATE_KEY_ID"),
        "private_key": os.getenv("FIREBASE_PRIVATE_KEY", "").replace("\\n", "\n"),
        "client_email": os.getenv("FIREBASE_CLIENT_EMAIL"),
        "client_id": os.getenv("FIREBASE_CLIENT_ID"),
        "token_uri": "https://oauth2.googleapis.com/token"
    }

@lru_cache(maxsize=1)
def get_sheets_credentials() -> Dict:
    """Get Google Sheets credentials with caching"""
    return {
        "type": "service_account",
        "project_id": os.getenv("GSPREAD_PROJECT_ID"),
        "private_key_id": os.getenv("GSPREAD_PRIVATE_KEY_ID"),
        "private_key": os.getenv("GSPREAD_PRIVATE_KEY", "").replace("\\n", "\n"),
        "client_email": os.getenv("GSPREAD_CLIENT_EMAIL"),
        "client_id": os.getenv("GSPREAD_CLIENT_ID"),
        "token_uri": "https://oauth2.googleapis.com/token"
    }

def initialize_firebase() -> firestore.Client:
    """Initialize Firebase with connection reuse"""
    global _firebase_db
    
    with _connection_lock:
        if not firebase_admin._apps:
            firebase_creds = get_firebase_credentials()
            cred = credentials.Certificate(firebase_creds)
            firebase_admin.initialize_app(cred)
            logger.info("✅ Firebase initialized")
        
        if _firebase_db is None:
            _firebase_db = firestore.client()
        
        return _firebase_db

def get_sheets_service():
    """Initialize Google Sheets service with optimizations"""
    global _sheets_service
    
    with _connection_lock:
        if _sheets_service is None:
            sheets_creds = get_sheets_credentials()
            credentials_obj = Credentials.from_service_account_info(
                sheets_creds, 
                scopes=["https://www.googleapis.com/auth/spreadsheets"]
            )
            
            # Build with optimizations
            _sheets_service = build(
                "sheets", "v4", 
                credentials=credentials_obj,
                cache_discovery=False
            )
            logger.info("✅ Sheets API initialized with optimizations")
        
        return _sheets_service

def parallel_fetch_documents(collection_name: str) -> List[Tuple[str, Dict]]:
    """Fetch all documents efficiently using streaming and batching"""
    db = initialize_firebase()
    collection_ref = db.collection(collection_name)
    
    logger.info(f"🚀 Starting optimized fetch from: {collection_name}")
    
    all_documents = []
    fetch_start = time.time()
    processed_count = 0
    
    try:
        # Stream documents efficiently
        doc_stream = collection_ref.stream()
        
        # Process in large batches to minimize overhead
        while True:
            # Fetch a large batch
            batch = list(islice(doc_stream, BATCH_SIZE))
            if not batch:
                break
            
            
            # Convert to tuples immediately (more memory efficient)
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Parallel conversion of documents to dictionaries
                def convert_doc(doc):
                    return (doc.id, doc.to_dict())
                
                # Submit all conversions
                futures = [executor.submit(convert_doc, doc) for doc in batch]
                
                # Collect results
                for future in as_completed(futures):
                    try:
                        all_documents.append(future.result())
                        processed_count += 1
                    except Exception as e:
                        logger.error(f"Error converting document: {e}")
            
            # Progress log
            elapsed = time.time() - fetch_start
            rate = processed_count / elapsed if elapsed > 0 else 0
            logger.info(f"📦 Fetched {processed_count} documents ({rate:.0f} docs/sec)")
            
            # Free memory from batch
            del batch
            
            # Force garbage collection for large datasets
            if processed_count % 10000 == 0:
                gc.collect()
    
    except Exception as e:
        logger.error(f"Fetch error: {e}")
        raise
    
    fetch_duration = time.time() - fetch_start
    if all_documents:
        rate = len(all_documents) / fetch_duration
        logger.info(f"✅ Fetched {len(all_documents)} documents in {fetch_duration:.2f}s ({rate:.0f} docs/sec)")
    else:
        logger.warning("No documents found")
    
    return all_documents

def process_documents_parallel(documents: List[Tuple[str, Dict]]) -> List[List[str]]:
    """Process documents using parallel processing"""
    if not documents:
        return []
    
    logger.info(f"⚡ Processing {len(documents)} documents using {MAX_WORKERS} workers")
    process_start = time.time()
    
    processor = FastDataProcessor
    processed_data = []
    
    # Split documents into optimized chunks
    chunks = [documents[i:i + CHUNK_SIZE] for i in range(0, len(documents), CHUNK_SIZE)]
    total_chunks = len(chunks)
    
    # Store results by index to preserve order
    chunk_results = [None] * total_chunks

    # Process chunks in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_idx = {
            executor.submit(processor.process_chunk, chunk): idx 
            for idx, chunk in enumerate(chunks)
        }
        
        completed = 0
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                chunk_result = future.result()
                chunk_results[idx] = chunk_result or []
                
                completed += 1
                if completed % 10 == 0:
                    logger.info(f"⚡ Processed chunk {completed}/{total_chunks}")
            except Exception as e:
                logger.error(f"Processing error in chunk: {e}", exc_info=True)
                chunk_results[idx] = []

    # Reassemble in order
    for result in chunk_results:
        if result:
            processed_data.extend(result)
    
    process_duration = time.time() - process_start
    logger.info(f"✅ Processed {len(processed_data)} documents in {process_duration:.2f}s")
    
    return processed_data

def ensure_sheet_exists(service, spreadsheet_id: str, sheet_name: str) -> int:
    """Ensure the sheet exists"""
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        for sheet in spreadsheet.get('sheets', []):
            if sheet['properties']['title'] == sheet_name:
                return sheet['properties']['sheetId']
        
        logger.info(f"Sheet '{sheet_name}' not found. Creating it...")
        body = {
            'requests': [{
                'addSheet': {
                    'properties': {'title': sheet_name}
                }
            }]
        }
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
        return response['replies'][0]['addSheet']['properties']['sheetId']
    except Exception as e:
        logger.error(f"Error checking/creating sheet: {e}")
        raise

def write_to_sheets_optimized(data: List[List[str]], spreadsheet_id: str, sheet_name: str):
    """Fast write to Google Sheets"""
    if not data:
        return
    
    service = get_sheets_service()
    write_start = time.time()
    
    logger.info(f"📝 Writing {len(data)} rows to Google Sheets")
    
    try:
        ensure_sheet_exists(service, spreadsheet_id, sheet_name)
        
        # Prepare header + body
        values = [HEADERS] + data
        
        # Determine column range
        num_cols = len(HEADERS)
        
        def get_col_letter(n):
            res = ""
            while n > 0:
                n, rem = divmod(n - 1, 26)
                res = chr(65 + rem) + res
            return res
            
        end_col = get_col_letter(num_cols)
        quoted_sheet = f"'{sheet_name}'"
        
        # Clear only needed columns
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"{quoted_sheet}!A:{end_col}",
            body={}
        ).execute()
        
        # Batch write
        if len(values) <= SHEETS_BATCH_SIZE:
            body = {"values": values, "majorDimension": "ROWS"}
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{quoted_sheet}!A1:{end_col}{len(values)}",
                valueInputOption="USER_ENTERED",
                body=body
            ).execute()
        else:
            # Split batches
            batch_reqs = []
            for i in range(0, len(values), SHEETS_BATCH_SIZE):
                batch = values[i:i + SHEETS_BATCH_SIZE]
                start_row = i + 1
                end_row = start_row + len(batch) - 1
                batch_reqs.append({
                    "range": f"{quoted_sheet}!A{start_row}:{end_col}{end_row}",
                    "values": batch,
                    "majorDimension": "ROWS"
                })
            
            body = {
                "valueInputOption": "USER_ENTERED", 
                "data": batch_reqs, 
                "includeValuesInResponse": False
            }
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()
            
        write_duration = time.time() - write_start
        logger.info(f"⚡ Write completed in {write_duration:.2f}s")
        
    except Exception as e:
        logger.error(f"Write error: {e}")
        raise

def main():
    """Optimized main execution"""
    start_time = time.time()
    try:
        logger.info("="*60)
        logger.info("⚡ ULTRA-FAST FIRESTORE TO SHEETS SYNC (Inventories)")
        logger.info("="*60)
        
        config = {
            "collection": "acnTestProperties",
            "spreadsheet": "1pkGrC3RQRxVwkEcb8AZyhT3KICKadw0IW9udkQsQh5k",
            "sheet": os.getenv("GOOGLE_SHEET_NAME", "Inventories from firebase")
        }
        
        initialize_firebase()
        get_sheets_service()
        
        # Phase 1: Fetch
        documents = parallel_fetch_documents(config["collection"])
        if not documents:
            return 0
            
        # Phase 2: Process
        processed_data = process_documents_parallel(documents)
        del documents
        gc.collect()
        
        # Phase 3: Write
        write_to_sheets_optimized(processed_data, config["spreadsheet"], config["sheet"])
        
        total_time = time.time() - start_time
        logger.info(f"🏁 Total execution time: {total_time:.2f}s")
        return 0
        
    except KeyboardInterrupt:
        logger.warning("Interrupted")
        return 130
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1
    finally:
        gc.collect()

if __name__ == "__main__":
    if sys.platform == "win32":
        try:
            import psutil
            p = psutil.Process(os.getpid())
            p.nice(psutil.HIGH_PRIORITY_CLASS)
        except: pass
    sys.exit(main())