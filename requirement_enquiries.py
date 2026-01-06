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

# Schema definition for Requirement Enquiries
SCHEMA_FIELDS = [
    "added", "assetType", "buyerCpId", "buyerName", "buyerNumber",
    "enquiryId", "isContactShared", "lastModified", "micromarket",
    "propertyId", "propertyName", "propertyType", "requirementId",
    "requirementType", "reviews", "sellerCpId", "sellerName",
    "sellerNumber", "status"
]

BOOLEAN_FIELDS = {"isContactShared"}
TIMESTAMP_FIELDS = {"added", "lastModified"}

class FastDataProcessor:
    """High-performance data processor using optimized operations"""
    
    @staticmethod
    @lru_cache(maxsize=10000)
    def convert_timestamp_cached(ts: float) -> str:
        """Cached timestamp conversion"""
        if not ts:
            return ""
        try:
            if ts > 4102444800:  # Milliseconds
                ts = ts / 1000
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%d/%b/%Y')
        except:
            return ""
    
    @staticmethod
    def convert_timestamp_batch(timestamps: List) -> List[str]:
        """Batch convert timestamps for efficiency"""
        results = []
        for ts in timestamps:
            if not ts:
                results.append("")
                continue
            try:
                # Use cache for common timestamps
                ts_float = float(ts)
                results.append(FastDataProcessor.convert_timestamp_cached(ts_float))
            except:
                results.append("")
        return results
    
    @staticmethod
    def process_chunk(docs_chunk: List[Tuple[str, Dict]]) -> List[Dict]:
        """Process a chunk of documents with optimized operations"""
        processed = []
        
        try:
            # Pre-compile timestamp conversions for the chunk
            timestamp_data = {}
            for field in TIMESTAMP_FIELDS:
                values = []
                for item in docs_chunk:
                    # Handle both tuple and dict formats
                    if isinstance(item, tuple) and len(item) >= 2:
                        values.append(item[1].get(field, 0) if isinstance(item[1], dict) else 0)
                    elif isinstance(item, dict):
                        values.append(item.get(field, 0))
                    else:
                        values.append(0)
                timestamp_data[field] = FastDataProcessor.convert_timestamp_batch(values)
            
            for idx, item in enumerate(docs_chunk):
                # Handle both tuple and dict formats
                if isinstance(item, tuple) and len(item) >= 2:
                    doc_id, doc_data = item
                elif isinstance(item, dict):
                    doc_id = item.get('id', '')
                    doc_data = item
                else:
                    continue
                
                if not isinstance(doc_data, dict):
                    continue
                
                row = {}
                
                # Add sort timestamp for 'added' field
                if "added" in doc_data:
                    row["_sort_timestamp"] = doc_data.get("added", 0)
                
                for field in SCHEMA_FIELDS:
                    value = doc_data.get(field)
                    
                    if field in TIMESTAMP_FIELDS:
                        row[field] = timestamp_data[field][idx]
                    elif field in BOOLEAN_FIELDS:
                        row[field] = bool(value) if value is not None else False
                    elif field == "reviews":
                        if isinstance(value, (list, tuple)):
                            row[field] = ", ".join(str(v) for v in value) if value else ""
                        else:
                            row[field] = str(value) if value else ""
                    elif field in ["buyerNumber", "sellerNumber"]:
                        row[field] = str(value) if value else ""
                    else:
                        row[field] = str(value) if value is not None else ""
                
                processed.append(row)
        
        except Exception as e:
            logger.error(f"Error in process_chunk: {e}", exc_info=True)
            # Return empty list on error to continue processing other chunks
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
                        
                        # Progress update every 5000 documents
                        if processed_count % 5000 == 0:
                            elapsed = time.time() - fetch_start
                            rate = processed_count / elapsed
                            logger.info(f"📦 Fetched {processed_count} documents ({rate:.0f} docs/sec)")
                    except Exception as e:
                        logger.error(f"Error converting document: {e}")
            
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

def process_documents_parallel(documents: List[Tuple[str, Dict]]) -> List[Dict]:
    """Process documents using parallel processing with optimizations"""
    if not documents:
        return []
    
    logger.info(f"⚡ Processing {len(documents)} documents using {MAX_WORKERS} workers")
    process_start = time.time()
    
    processor = FastDataProcessor()
    processed_data = []
    
    # Split documents into optimized chunks
    chunks = [documents[i:i + CHUNK_SIZE] for i in range(0, len(documents), CHUNK_SIZE)]
    total_chunks = len(chunks)
    logger.info(f"📦 Split into {total_chunks} chunks of {CHUNK_SIZE} documents each")
    
    # Process chunks in parallel with progress tracking
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all chunk processing tasks
        future_to_chunk = {
            executor.submit(processor.process_chunk, chunk): idx 
            for idx, chunk in enumerate(chunks)
        }
        
        completed = 0
        failed = 0
        for future in as_completed(future_to_chunk):
            try:
                chunk_result = future.result()
                if chunk_result:  # Only add if there are results
                    processed_data.extend(chunk_result)
                else:
                    failed += 1
                completed += 1
                
                # Progress update
                if completed % 10 == 0 or completed == total_chunks:
                    elapsed = time.time() - process_start
                    rate = len(processed_data) / elapsed if elapsed > 0 else 0
                    logger.info(f"⚡ Processed {len(processed_data)}/{len(documents)} docs ({rate:.0f} docs/sec) - {completed}/{total_chunks} chunks")
                    if failed > 0:
                        logger.warning(f"   ⚠️ {failed} chunks failed processing")
                    
            except Exception as e:
                logger.error(f"Processing error in chunk: {e}", exc_info=True)
                failed += 1
                completed += 1
    
    # Optimized sorting using key function
    logger.info("🔄 Sorting documents...")
    sort_start = time.time()
    
    if processed_data:
        # Sort by timestamp (newest first)
        processed_data.sort(key=lambda x: x.get("_sort_timestamp", 0), reverse=True)
        
        # Remove sort field
        for doc in processed_data:
            doc.pop("_sort_timestamp", None)
    
    sort_duration = time.time() - sort_start
    process_duration = time.time() - process_start
    
    logger.info(f"✅ Processed {len(processed_data)} documents in {process_duration:.2f}s")
    logger.info(f"   Sort time: {sort_duration:.2f}s")
    
    return processed_data

def ensure_sheet_exists(service, spreadsheet_id: str, sheet_name: str) -> int:
    """Ensure the sheet exists, creating it if necessary. Returns the sheet ID."""
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        for sheet in spreadsheet.get('sheets', []):
            if sheet['properties']['title'] == sheet_name:
                return sheet['properties']['sheetId']
        
        # Sheet doesn't exist, create it
        logger.info(f"Sheet '{sheet_name}' not found. Creating it...")
        body = {
            'requests': [{
                'addSheet': {
                    'properties': {
                        'title': sheet_name
                    }
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

def write_to_sheets_optimized(data: List[Dict], spreadsheet_id: str, sheet_name: str):
    """Ultra-fast write to Google Sheets using batch operations"""
    if not data:
        logger.warning("No data to write")
        return
    
    service = get_sheets_service()
    write_start = time.time()
    
    logger.info(f"📝 Writing {len(data)} rows to Google Sheets")
    
    try:
        # Ensure sheet exists first
        ensure_sheet_exists(service, spreadsheet_id, sheet_name)
        
        # Prepare all values in memory first (fastest approach)
        values = [SCHEMA_FIELDS]  # Header row
        
        # Use list comprehension for speed
        values.extend([
            [item.get(field, "") for field in SCHEMA_FIELDS]
            for item in data
        ])
        
        total_cells = len(values) * len(SCHEMA_FIELDS)
        logger.info(f"📊 Prepared {total_cells:,} cells for writing")
        
        # Use quoted sheet name for safety with spaces
        quoted_sheet_name = f"'{sheet_name}'"
        
        # Clear only the target columns (A-S for 19 columns) in one operation
        clear_range = f"{quoted_sheet_name}!A:S"
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=clear_range,
            body={}
        ).execute()
        
        logger.info("🧹 Cleared target columns A-S")
        
        # Determine optimal write strategy based on data size
        if len(values) <= SHEETS_BATCH_SIZE:
            # Single batch write (fastest for most cases)
            update_range = f"{quoted_sheet_name}!A1:S{len(values)}"
            
            body = {
                "values": values,
                "majorDimension": "ROWS"
            }
            
            response = service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=update_range,
                valueInputOption="USER_ENTERED",  # Preserves data types
                body=body
            ).execute()
            
            updated_cells = response.get('updatedCells', 0)
            logger.info(f"✅ Written {updated_cells:,} cells in single batch")
        
        else:
            # Multi-batch write for very large datasets
            logger.info(f"📦 Using batch writes for {len(values)} rows")
            
            # Prepare batch data
            batch_data = []
            
            for i in range(0, len(values), SHEETS_BATCH_SIZE):
                batch = values[i:i + SHEETS_BATCH_SIZE]
                start_row = i + 1
                end_row = start_row + len(batch) - 1
                
                batch_data.append({
                    "range": f"{quoted_sheet_name}!A{start_row}:S{end_row}",
                    "values": batch,
                    "majorDimension": "ROWS"
                })
                
                logger.info(f"📦 Prepared batch: rows {start_row}-{end_row} ({len(batch)} rows)")
            
            # Execute batch update in one API call
            body = {
                "valueInputOption": "USER_ENTERED",
                "data": batch_data,
                "includeValuesInResponse": False
            }
            
            response = service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()
            
            total_updated = sum(r.get('updatedCells', 0) for r in response.get('responses', []))
            logger.info(f"✅ Written {total_updated:,} cells across {len(batch_data)} batches")
        
        # Optional: Auto-resize columns for better readability
        try:
            # Get sheet ID
            spreadsheet = service.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
                fields="sheets.properties"
            ).execute()
            
            sheet_id = None
            for sheet in spreadsheet.get('sheets', []):
                if sheet['properties']['title'] == sheet_name:
                    sheet_id = sheet['properties']['sheetId']
                    break
            
            if sheet_id is not None:
                format_request = {
                    "requests": [{
                        "autoResizeDimensions": {
                            "dimensions": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": 0,
                                "endIndex": 19  # Columns A-S
                            }
                        }
                    }]
                }
                
                service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body=format_request
                ).execute()
                
                logger.info("✅ Column formatting applied")
        except Exception as e:
            logger.debug(f"Column formatting skipped: {e}")
        
        write_duration = time.time() - write_start
        cells_per_second = total_cells / write_duration if write_duration > 0 else 0
        
        logger.info(f"⚡ Write completed in {write_duration:.2f}s ({cells_per_second:,.0f} cells/sec)")
        
    except Exception as e:
        logger.error(f"Write error: {e}")
        raise

def validate_environment() -> bool:
    """Quick environment validation"""
    required = [
        "FIREBASE_PROJECT_ID", "FIREBASE_PRIVATE_KEY", "FIREBASE_CLIENT_EMAIL",
        "GSPREAD_PROJECT_ID", "GSPREAD_PRIVATE_KEY", "GSPREAD_CLIENT_EMAIL"
    ]
    
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        logger.error(f"Missing environment variables: {', '.join(missing)}")
        return False
    
    logger.info("✅ Environment validated")
    return True

def main():
    """Optimized main execution"""
    start_time = time.time()
    
    try:
        logger.info("="*60)
        logger.info("⚡ ULTRA-FAST FIRESTORE TO SHEETS SYNC (Requirements Enquiries)")
        logger.info(f"🔧 Using {MAX_WORKERS} parallel workers")
        logger.info(f"💾 Batch size: {BATCH_SIZE} | Chunk size: {CHUNK_SIZE}")
        logger.info("="*60)
        
        if not validate_environment():
            logger.error("Environment validation failed")
            return 1
        
        # Configuration
        config = {
            "collection": "acnRequirementsEnquiries",
            "spreadsheet": "1pkGrC3RQRxVwkEcb8AZyhT3KICKadw0IW9udkQsQh5k",
            "sheet": "Requirement Enquiries"
        }
        
        logger.info(f"📋 Configuration:")
        logger.info(f"   • Collection: {config['collection']}")
        logger.info(f"   • Sheet: {config['sheet']}")
        logger.info(f"   • Columns: A-S (19 fields)")
        
        # Initialize services
        logger.info("🔌 Initializing services...")
        initialize_firebase()
        get_sheets_service()
        
        # Phase 1: Parallel fetch
        logger.info("\n📥 PHASE 1: Fetching data")
        fetch_start = time.time()
        documents = parallel_fetch_documents(config["collection"])
        fetch_duration = time.time() - fetch_start
        
        if not documents:
            logger.warning("No data found")
            return 0
        
        # Phase 2: Parallel processing
        logger.info("\n⚙️ PHASE 2: Processing data")
        process_start = time.time()
        processed_data = process_documents_parallel(documents)
        process_duration = time.time() - process_start
        
        # Clear memory
        del documents
        gc.collect()
        
        # Phase 3: Optimized write
        logger.info("\n📤 PHASE 3: Writing to Sheets")
        write_start = time.time()
        write_to_sheets_optimized(processed_data, config["spreadsheet"], config["sheet"])
        write_duration = time.time() - write_start
        
        # Summary
        total_duration = time.time() - start_time
        
        logger.info("\n" + "="*60)
        logger.info("🏁 SYNC COMPLETE!")
        logger.info("="*60)
        logger.info(f"📊 Performance Summary:")
        logger.info(f"   • Total records: {len(processed_data):,}")
        logger.info(f"   • Total time: {total_duration:.2f}s")
        logger.info(f"   • Fetch: {fetch_duration:.2f}s ({len(processed_data)/fetch_duration:.0f} docs/sec)")
        logger.info(f"   • Process: {process_duration:.2f}s ({len(processed_data)/process_duration:.0f} docs/sec)" if process_duration > 0 else f"   • Process: {process_duration:.2f}s")
        
        if write_duration > 0:
            logger.info(f"   • Write: {write_duration:.2f}s ({len(processed_data)*19/write_duration:.0f} cells/sec)")
        else:
            logger.info(f"   • Write: {write_duration:.2f}s")
        
        logger.info(f"   • Overall: {len(processed_data)/total_duration:.0f} records/second" if total_duration > 0 else "   • Overall: N/A")
        logger.info("="*60)
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Process interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        return 1
    finally:
        # Cleanup
        gc.collect()

if __name__ == "__main__":
    # Windows-specific optimizations
    if sys.platform == "win32":
        try:
            import psutil
            p = psutil.Process(os.getpid())
            p.nice(psutil.HIGH_PRIORITY_CLASS)
            logger.info("🔧 Process priority elevated (Windows)")
        except:
            pass  # psutil might not be installed
    
    sys.exit(main())
