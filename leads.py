import firebase_admin
from firebase_admin import credentials, firestore
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone
import json
import os
from dotenv import load_dotenv
import sys
import codecs
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from typing import List, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ensure UTF-8 output (fixes UnicodeEncodeError on Windows)
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, 'strict')

# Load environment variables from .env file
load_dotenv()

# Global variables for connection reuse
_firebase_db = None
_sheets_client = None

# Configuration constants
BATCH_SIZE = 1000  # Process documents in batches
MAX_WORKERS = 4    # Number of parallel workers
SHEETS_BATCH_SIZE = 10000  # Google Sheets batch size limit

def get_firebase_credentials():
    """Get Firebase credentials dictionary"""
    return {
        "type": "service_account",
        "project_id": os.getenv("FIREBASE_PROJECT_ID"),
        "private_key_id": os.getenv("FIREBASE_PRIVATE_KEY_ID"),
        "private_key": os.getenv("FIREBASE_PRIVATE_KEY").replace("\\n", "\n"),
        "client_email": os.getenv("FIREBASE_CLIENT_EMAIL"),
        "client_id": os.getenv("FIREBASE_CLIENT_ID"),
        "token_uri": "https://oauth2.googleapis.com/token"
    }

def get_sheets_credentials():
    """Get Google Sheets credentials dictionary"""
    return {
        "type": "service_account",
        "project_id": os.getenv("GSPREAD_PROJECT_ID"),
        "private_key_id": os.getenv("GSPREAD_PRIVATE_KEY_ID"),
        "private_key": os.getenv("GSPREAD_PRIVATE_KEY").replace("\\n", "\n"),
        "client_email": os.getenv("GSPREAD_CLIENT_EMAIL"),
        "client_id": os.getenv("GSPREAD_CLIENT_ID"),
        "token_uri": "https://oauth2.googleapis.com/token"
    }

def initialize_firebase():
    """Initialize Firebase with connection reuse"""
    global _firebase_db
    try:
        if not firebase_admin._apps:  # Prevent re-initialization
            firebase_creds = get_firebase_credentials()
            cred = credentials.Certificate(firebase_creds)
            firebase_admin.initialize_app(cred)
            logger.info("✅ Firebase initialized successfully.")
        
        if _firebase_db is None:
            _firebase_db = firestore.client()
            logger.info("✅ Firestore client created.")
        
        return _firebase_db
    except Exception as e:
        logger.error(f"❌ Error initializing Firebase: {e}")
        raise

def initialize_sheets_client():
    """Initialize Google Sheets client with connection reuse"""
    global _sheets_client
    try:
        if _sheets_client is None:
            sheets_creds = get_sheets_credentials()
            credentials_obj = Credentials.from_service_account_info(
                sheets_creds,
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            _sheets_client = gspread.authorize(credentials_obj)
            logger.info("✅ Google Sheets client initialized.")
        
        return _sheets_client
    except Exception as e:
        logger.error(f"❌ Error initializing Google Sheets client: {e}")
        raise

def convert_unix_to_date(unix_timestamp):
    """Convert Unix timestamp to formatted date string"""
    try:
        if not unix_timestamp or not isinstance(unix_timestamp, (int, float, str)):
            return ""
        # Handle 0 timestamps
        if int(unix_timestamp) == 0:
            return ""
        return datetime.fromtimestamp(int(unix_timestamp), tz=timezone.utc).strftime('%d/%b/%Y')
    except Exception as e:
        logger.warning(f"⚠️ Error converting timestamp {unix_timestamp}: {e}")
        return ""

def clean_phone_number(number):
    """Clean and normalize phone number"""
    if not number:
        return ""
    normalized_number = str(number).replace(" ", "").strip()
    if normalized_number.startswith("+91"):
        normalized_number = normalized_number.replace("+91", "").strip()
    return normalized_number

def flatten_field(value):
    """Handle complex data types while preserving numeric and boolean types"""
    if isinstance(value, list):
        if not value:  # Empty list
            return ""
        return ", ".join(str(v) for v in value)
    elif isinstance(value, dict):
        if not value:  # Empty dict
            return ""
        return ", ".join(f"{k}:{v}" for k, v in value.items())
    elif isinstance(value, bool):
        return value  # Keep boolean as boolean for Google Sheets
    elif isinstance(value, (int, float)):
        return value  # Keep numbers as numbers for Google Sheets
    return str(value) if value is not None else ""

def process_document_batch(docs_batch: List, batch_number: int) -> List[Dict]:
    """Process a batch of documents in parallel"""
    logger.info(f"🔄 Processing batch {batch_number} with {len(docs_batch)} documents...")
    
    processed_docs = []
    
    for doc in docs_batch:
        try:
            item = doc.to_dict()
            if not isinstance(item, dict):
                logger.warning(f"⚠️ Unexpected data format in document {doc.id}: {item}")
                continue

            # Process phone number (keep as string to preserve formatting)
            item["phoneNumber"] = clean_phone_number(item.get("phoneNumber", ""))
            
            # Convert timestamp fields for leads
            timestamp_fields = [
                "added", "lastModified", "lastConnected", "lastTried"
            ]
            
            for field in timestamp_fields:
                if field in item:
                    item[f"{field}_formatted"] = convert_unix_to_date(item.get(field))

            # Flatten array and complex fields for leads
            array_fields = [
                "connectHistory", "notes"
            ]
            
            for field in array_fields:
                if field in item:
                    item[f"{field}_flat"] = flatten_field(item.get(field))

            # Process all fields to ensure they're properly formatted while preserving data types
            processed = {}
            for k, v in item.items():
                processed[k] = flatten_field(v)
            
            processed_docs.append(processed)

        except Exception as doc_error:
            logger.error(f"⚠️ Error processing document {doc.id}: {doc_error}")

    logger.info(f"✅ Completed batch {batch_number}: {len(processed_docs)} documents processed")
    return processed_docs

def fetch_firestore_data_with_pagination(collection_name: str, page_size: int = BATCH_SIZE) -> List[Dict]:
    """Fetch Firestore data with pagination and batch processing - filtered for referral source only"""
    try:
        db = initialize_firebase()
        collection_ref = db.collection(collection_name)
        
        logger.info(f"🔍 Starting paginated fetch from Firestore collection: {collection_name} (referral source only)...")
        
        # Filter for documents where source equals "referral"
        query = collection_ref.where("source", "==", "")
        
        # Get total count for progress tracking
        logger.info("📊 Counting total direct documents...")
        all_docs = list(query.stream())
        total_docs = len(all_docs)
        
        if total_docs == 0:
            logger.warning("⚠️ No direct documents found in Firestore.")
            return []

        logger.info(f"📈 Found {total_docs} direct documents. Processing in batches of {page_size}...")

        # Process documents in batches with parallel processing
        all_processed_data = []
        doc_batches = [all_docs[i:i + page_size] for i in range(0, len(all_docs), page_size)]
        
        # Use ThreadPoolExecutor for parallel batch processing
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all batch processing tasks
            future_to_batch = {
                executor.submit(process_document_batch, batch, i + 1): i + 1
                for i, batch in enumerate(doc_batches)
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_batch):
                batch_number = future_to_batch[future]
                try:
                    batch_result = future.result()
                    all_processed_data.extend(batch_result)
                    
                    # Progress update
                    progress = len(all_processed_data) / total_docs * 100
                    logger.info(f"📊 Progress: {len(all_processed_data)}/{total_docs} ({progress:.1f}%)")
                    
                except Exception as batch_error:
                    logger.error(f"❌ Error processing batch {batch_number}: {batch_error}")

        logger.info(f"✅ Successfully processed {len(all_processed_data)} referral records from Firestore.")
        return all_processed_data

    except Exception as e:
        logger.error(f"❌ Error fetching data from Firestore: {e}")
        return []

def write_to_google_sheet_batch(data: List[Dict], spreadsheet_id: str, sheet_name: str):
    """Write data to Google Sheets with batch processing and optimization"""
    try:
        if not data:
            logger.warning("⚠️ No data to write to Google Sheets.")
            return
        
        gc = initialize_sheets_client()
        spreadsheet = gc.open_by_key(spreadsheet_id)
        
        try:
            sheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="50")
            logger.info(f"✅ New sheet '{sheet_name}' created.")
        
        logger.info(f"✅ Google Sheet '{sheet_name}' opened successfully.")
        
        # Define column order for leads
        fixed_columns = [
            # Basic Info
            "leadId", "name", "phoneNumber", "emailAddress", "verified",
            # Lead Status & Type
            "leadStatus", "contactStatus", "blackListed",
            # Source & Campaign
            "source",
            # KAM Info
            "kamId", "kamName",
            # Community & Broadcast
            "communityJoined", "onBroadcast",
            # History & Notes
            "connectHistory_flat", "notes_flat",
            # Timestamps
            "added", "added_formatted", "lastModified", "lastModified_formatted",
            "lastConnected", "lastConnected_formatted", "lastTried", "lastTried_formatted",
            # Extra Details
            "extraDetails"
        ]
        
        # Define data type fields for optimization
        numeric_fields = {
            "added", "lastModified", "lastConnected", "lastTried"
        }
        
        boolean_fields = {
            "blackListed", "communityJoined", "onBroadcast", "verified"
        }
        
        logger.info(f"🔄 Formatting {len(data)} rows for batch write...")
        
        # Format data efficiently
        formatted_data = []
        for item in data:
            row = []
            for field in fixed_columns:
                value = item.get(field, "")
                
                # Efficient type handling
                if value is None:
                    row.append("")
                elif field in numeric_fields and isinstance(value, (int, float)) and not isinstance(value, bool):
                    row.append(value)
                elif field in boolean_fields and isinstance(value, bool):
                    row.append(value)
                elif isinstance(value, (int, float)) and not isinstance(value, bool):
                    row.append(value)
                elif isinstance(value, bool):
                    row.append(value)
                else:
                    row.append(str(value))
            formatted_data.append(row)
        
        # Prepare data for batch write
        data_to_write = [fixed_columns] + formatted_data
        total_rows = len(data_to_write)
        
        logger.info(f"📊 Writing {total_rows} rows to Google Sheets...")
        
        # Clear sheet first
        sheet.clear()
        
        # Batch write data (Google Sheets handles large datasets efficiently with single update)
        if total_rows <= SHEETS_BATCH_SIZE:
            # Single batch write for normal datasets
            start_time = time.time()
            sheet.update(values=data_to_write, range_name="A1", value_input_option='USER_ENTERED')
            end_time = time.time()
            
            logger.info(f"✅ Data written successfully in {end_time - start_time:.2f} seconds.")
            logger.info(f"📈 Performance: {len(fixed_columns)} columns, {len(formatted_data)} rows")
            logger.info("✅ Used USER_ENTERED mode to preserve numeric formatting (no apostrophes).")
        else:
            # Split into multiple batches for very large datasets
            logger.info(f"📊 Large dataset detected. Splitting into batches of {SHEETS_BATCH_SIZE} rows...")
            
            # Write headers first
            sheet.update(values=[fixed_columns], range_name="A1", value_input_option='USER_ENTERED')
            
            # Write data in batches
            for i in range(0, len(formatted_data), SHEETS_BATCH_SIZE):
                batch_data = formatted_data[i:i + SHEETS_BATCH_SIZE]
                start_row = i + 2  # +2 because headers are in row 1, and sheets are 1-indexed
                end_row = start_row + len(batch_data) - 1
                
                batch_range = f"A{start_row}:{chr(65 + len(fixed_columns) - 1)}{end_row}"
                
                start_time = time.time()
                sheet.update(values=batch_data, range_name=batch_range, value_input_option='USER_ENTERED')
                end_time = time.time()
                
                logger.info(f"✅ Batch {i//SHEETS_BATCH_SIZE + 1} written ({len(batch_data)} rows) in {end_time - start_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"❌ Error writing to Google Sheets: {e}")
        raise

def main():
    """Main execution function with enhanced error handling and performance monitoring"""
    start_time = time.time()
    
    try:
        logger.info("🚀 Starting enhanced Firestore to Google Sheets sync for referral leads...")
        
        # Configuration
        collection_name = "acnLeads"
        spreadsheet_id = "1pkGrC3RQRxVwkEcb8AZyhT3KICKadw0IW9udkQsQh5k"
        sheet_name = "Tried Access"
        
        # Initialize connections
        logger.info("🔧 Initializing connections...")
        initialize_firebase()
        initialize_sheets_client()
        
        # Fetch data with batch processing (filtered for referral source)
        logger.info("📥 Fetching referral leads data from Firestore...")
        fetch_start = time.time()
        data = fetch_firestore_data_with_pagination(collection_name)
        fetch_end = time.time()
        
        logger.info(f"⏱️ Data fetch completed in {fetch_end - fetch_start:.2f} seconds")
        
        if data:
            # Write data with batch processing
            logger.info("📤 Writing data to Google Sheets...")
            write_start = time.time()
            write_to_google_sheet_batch(data, spreadsheet_id, sheet_name)
            write_end = time.time()
            
            logger.info(f"⏱️ Data write completed in {write_end - write_start:.2f} seconds")
            
            # Performance summary
            total_time = time.time() - start_time
            records_per_second = len(data) / total_time if total_time > 0 else 0
            
            logger.info("🎉 Referral leads sync completed successfully!")
            logger.info(f"📊 Performance Summary:")
            logger.info(f"   • Total referral records: {len(data)}")
            logger.info(f"   • Total time: {total_time:.2f} seconds")
            logger.info(f"   • Processing rate: {records_per_second:.1f} records/second")
            logger.info(f"   • Fetch time: {fetch_end - fetch_start:.2f}s")
            logger.info(f"   • Write time: {write_end - write_start:.2f}s")
        else:
            logger.warning("⚠️ No referral leads data to write to Google Sheets.")
            
    except Exception as e:
        logger.error(f"❌ An error occurred: {e}")
        raise
    finally:
        total_time = time.time() - start_time
        logger.info(f"⏱️ Total execution time: {total_time:.2f} seconds")

if __name__ == "__main__":
    main()