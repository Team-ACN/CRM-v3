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
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from typing import List, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ensure UTF-8 output (fixes UnicodeEncodeError on Windows)
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, 'strict')

# Set non-interactive mode to prevent any prompts
os.environ['PYTHONUNBUFFERED'] = '1'
os.environ['PYTHONIOENCODING'] = 'utf-8'

# Load environment variables from .env file
load_dotenv()

# Global variables for connection reuse
_firebase_db = None
_sheets_service = None

# Configuration constants
BATCH_SIZE = 1000  # Process documents in batches
MAX_WORKERS = 4    # Number of parallel workers
SHEETS_BATCH_SIZE = 10000  # Google Sheets batch size limit

# Non-interactive mode flag
NON_INTERACTIVE_MODE = True

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

def get_sheets_service():
    """Initialize Google Sheets service with connection reuse"""
    global _sheets_service
    try:
        if _sheets_service is None:
            sheets_creds = get_sheets_credentials()
            credentials_obj = Credentials.from_service_account_info(
                sheets_creds, scopes=["https://www.googleapis.com/auth/spreadsheets"]
            )
            _sheets_service = build("sheets", "v4", credentials=credentials_obj)
            logger.info("✅ Google Sheets API initialized successfully.")
        
        return _sheets_service
    except Exception as e:
        logger.error(f"❌ Error initializing Google Sheets API: {e}")
        raise

def convert_unix_to_date(unix_timestamp):
    """Convert Unix timestamps (seconds or milliseconds) to human-readable dates"""
    try:
        if unix_timestamp is None:
            return ""
        
        # Handle array-like objects
        if isinstance(unix_timestamp, (list, tuple)):
            if len(unix_timestamp) == 0:
                return ""
            unix_timestamp = unix_timestamp[0]
        elif isinstance(unix_timestamp, np.ndarray):
            if unix_timestamp.size == 0:
                return ""
            unix_timestamp = unix_timestamp.tolist()[0] if unix_timestamp.size > 0 else ""
        
        # Ensure the value is of an acceptable type
        if not isinstance(unix_timestamp, (int, float, str)):
            return ""
        
        # Convert to float for better handling
        ts = float(unix_timestamp)
        if ts == 0:
            return ""
        
        # Detect if timestamp is in milliseconds or seconds
        # If timestamp is greater than year 2100 in seconds (4102444800), 
        # it's likely in milliseconds
        if ts > 4102444800:  # January 1, 2100 in seconds
            # Convert milliseconds to seconds
            ts = ts / 1000
        
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%d/%b/%Y')
    except Exception as e:
        logger.warning(f"⚠️ Error converting timestamp {unix_timestamp}: {e}")
        return ""


def flatten_value(value):
    """Flatten values for proper Google Sheets handling while preserving data types"""
    # Handle pandas NaN values first
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        # pd.isna() might fail for some data types, continue with other checks
        pass
    
    # Handle arrays/lists
    if isinstance(value, (list, tuple, np.ndarray)):
        try:
            if isinstance(value, np.ndarray):
                # Proper way to check if numpy array is empty
                if value.size == 0:
                    return ""
                value = value.tolist()
            elif isinstance(value, (list, tuple)):
                # Check if list/tuple is empty
                if len(value) == 0:
                    return ""
            
            # Convert array to readable string
            return ", ".join(str(item) for item in value)
        except Exception as e:
            logger.warning(f"⚠️ Error processing array value: {e}")
            return str(value)
    
    # Handle dictionaries
    if isinstance(value, dict):
        if not value:  # Empty dict
            return ""
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"⚠️ Error serializing dict: {e}")
            return str(value)
    
    # Preserve numeric and boolean types
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value
    
    return str(value) if value is not None else ""

def process_document_batch(docs_batch: List, batch_number: int) -> List[Dict]:
    """Process a batch of enquiry documents in parallel"""
    logger.info(f"🔄 Processing enquiry batch {batch_number} with {len(docs_batch)} documents...")
    
    processed_docs = []
    
    for doc in docs_batch:
        try:
            item = doc.to_dict()
            if not isinstance(item, dict):
                logger.warning(f"⚠️ Unexpected data format in document {doc.id}: {item}")
                continue

            # Clean phone numbers with error handling (keep original format for schema)
            try:
                if "buyerNumber" in item and item["buyerNumber"]:
                    # Keep original format but ensure it's a string
                    item["buyerNumber"] = str(item["buyerNumber"])
                if "sellerNumber" in item and item["sellerNumber"]:
                    # Keep original format but ensure it's a string  
                    item["sellerNumber"] = str(item["sellerNumber"])
            except Exception as e:
                logger.warning(f"⚠️ Error processing phone numbers for {doc.id}: {e}")
                item["buyerNumber"] = str(item.get("buyerNumber", ""))
                item["sellerNumber"] = str(item.get("sellerNumber", ""))
            
            # Store original timestamps for sorting
            original_added = item.get("added", 0)
            original_lastModified = item.get("lastModified", 0)
            
            # Convert timestamp fields to readable format (overwrite original fields)
            try:
                item["added"] = convert_unix_to_date(original_added)
                item["lastModified"] = convert_unix_to_date(original_lastModified)
            except Exception as e:
                logger.warning(f"⚠️ Error converting timestamps for {doc.id}: {e}")
                item["added"] = ""
                item["lastModified"] = ""
            
            # Store original timestamp for sorting
            item["_sort_timestamp"] = original_added
            
            # Process reviews array with error handling
            try:
                if "reviews" in item:
                    item["reviews"] = flatten_value(item.get("reviews"))
            except Exception as e:
                logger.warning(f"⚠️ Error processing reviews for {doc.id}: {e}")
                item["reviews"] = str(item.get("reviews", ""))
            
            # Process only the schema fields efficiently
            schema_fields = [
                "added", "buyerCpId", "buyerName", "buyerNumber", "enquiryId",
                "isContactShared", "isNew", "lastModified", "propertyId", 
                "propertyName", "reviews", "sellerCpId", "sellerName", 
                "sellerNumber", "status"
            ]
            
            processed = {}
            for field in schema_fields:
                try:
                    value = item.get(field)
                    if value is None:
                        processed[field] = ""
                    elif field in ["isContactShared", "isNew"] and isinstance(value, bool):
                        processed[field] = value  # Keep boolean type
                    elif field in ["added", "lastModified"]:
                        processed[field] = str(value) if value else ""  # Readable dates as strings
                    elif field == "reviews":
                        processed[field] = value  # Already processed above
                    else:
                        processed[field] = str(value)
                except Exception as e:
                    logger.warning(f"⚠️ Error processing field '{field}' for {doc.id}: {e}")
                    processed[field] = ""
            
            # Keep the sort timestamp for sorting
            processed["_sort_timestamp"] = item.get("_sort_timestamp", 0)
            
            processed_docs.append(processed)

        except Exception as doc_error:
            logger.error(f"⚠️ Error processing document {doc.id}: {doc_error}")
            # Continue processing other documents instead of failing completely

    logger.info(f"✅ Completed enquiry batch {batch_number}: {len(processed_docs)} documents processed")
    return processed_docs

def fetch_firestore_data_with_pagination(collection_name: str, page_size: int = BATCH_SIZE) -> List[Dict]:
    """Fetch Firestore enquiry data with pagination and batch processing"""
    try:
        db = initialize_firebase()
        collection_ref = db.collection(collection_name)
        
        logger.info(f"🔍 Starting paginated fetch from Firestore collection: {collection_name}...")
        
        # Get all documents
        logger.info("📊 Fetching all enquiry documents...")
        all_docs = list(collection_ref.stream())
        total_docs = len(all_docs)
        
        if total_docs == 0:
            logger.warning("⚠️ No documents found in Firestore.")
            return []
        
        logger.info(f"📈 Found {total_docs} enquiry documents. Processing in batches of {page_size}...")
        
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

        # Sort data in descending order based on original 'added' timestamp (newest first)
        logger.info("🔄 Sorting enquiries by 'added' date (newest first)...")
        all_processed_data.sort(key=lambda x: x.get("_sort_timestamp", 0), reverse=True)
        
        # Remove the temporary sorting field
        for item in all_processed_data:
            item.pop("_sort_timestamp", None)

        logger.info(f"✅ Successfully processed and sorted {len(all_processed_data)} enquiry records.")
        return all_processed_data

    except Exception as e:
        logger.error(f"❌ Error fetching data from Firestore: {e}")
        return []

def write_to_google_sheet_batch(data: List[Dict], spreadsheet_id: str, sheet_name: str):
    """Write enquiry data to Google Sheets with batch processing and optimization"""
    try:
        if not data:
            logger.warning("⚠️ No data to write to Google Sheets.")
            return
        
        service = get_sheets_service()
        
        logger.info(f"✅ Google Sheet service ready for sheet: {sheet_name}")
        
        # Define column order based on exact schema provided (readable dates only)
        fixed_columns = [
            # Core fields from schema (dates will be in readable format)
            "added",  # Will contain readable date format
            "buyerCpId", 
            "buyerName", 
            "buyerNumber",
            "enquiryId",
            "isContactShared",
            "isNew",
            "lastModified",  # Will contain readable date format
            "propertyId", 
            "propertyName",
            "reviews",
            "sellerCpId", 
            "sellerName", 
            "sellerNumber",
            "status"
        ]
        
        # Use only the fixed columns from schema - no additional fields
        final_columns = fixed_columns
        
        logger.info(f"📊 Using {len(final_columns)} columns from schema")
        
        # Calculate required sheet dimensions
        total_rows_needed = len(data) + 1  # +1 for header
        total_columns_needed = len(final_columns)
        
        logger.info(f"📏 Sheet dimensions needed: {total_rows_needed} rows x {total_columns_needed} columns")
        
        # Get current sheet properties and resize if necessary
        try:
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            
            # Find the target sheet
            target_sheet_id = None
            current_rows = 1000  # Default
            current_cols = 26    # Default
            
            for sheet in spreadsheet.get('sheets', []):
                if sheet['properties']['title'] == sheet_name:
                    target_sheet_id = sheet['properties']['sheetId']
                    current_rows = sheet['properties']['gridProperties'].get('rowCount', 1000)
                    current_cols = sheet['properties']['gridProperties'].get('columnCount', 26)
                    break
            
            # Check if we need to resize the sheet
            if total_rows_needed > current_rows or total_columns_needed > current_cols:
                new_rows = max(total_rows_needed + 100, current_rows)  # Add buffer
                new_cols = max(total_columns_needed + 5, current_cols)  # Add buffer
                
                logger.info(f"📏 Resizing sheet from {current_rows}x{current_cols} to {new_rows}x{new_cols}")
                
                resize_request = {
                    'requests': [{
                        'updateSheetProperties': {
                            'properties': {
                                'sheetId': target_sheet_id,
                                'gridProperties': {
                                    'rowCount': new_rows,
                                    'columnCount': new_cols
                                }
                            },
                            'fields': 'gridProperties.rowCount,gridProperties.columnCount'
                        }
                    }]
                }
                
                service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id, 
                    body=resize_request
                ).execute()
                
                logger.info(f"✅ Sheet resized successfully to {new_rows}x{new_cols}")
            else:
                logger.info(f"✅ Sheet size is sufficient: {current_rows}x{current_cols}")
                
        except Exception as resize_error:
            logger.error(f"⚠️ Error resizing sheet: {resize_error}")
            logger.info("Continuing with existing sheet size...")
        
        # Define data type fields for optimization based on schema
        numeric_fields = set()  # No numeric fields in sheet output (dates are now readable strings)
        
        boolean_fields = {
            "isContactShared", "isNew"
        }
        
        logger.info(f"🔄 Formatting {len(data)} enquiry rows for batch write...")
        
        # Format data efficiently while preserving types
        formatted_data = []
        for item in data:
            row = []
            for field in final_columns:
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
        sheet_data = [final_columns] + formatted_data
        total_rows = len(sheet_data)
        
        logger.info(f"📊 Writing {total_rows} rows to Google Sheets...")
        
        # Clear only columns A to O (15 columns) - preserve any data beyond column O
        start_time = time.time()
        sheet_range = f"{sheet_name}!A1:O"  # Explicitly clear only A to O columns
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=sheet_range
        ).execute()
        logger.info(f"🧹 Cleared only columns A to O: {sheet_range}")
        
        # Batch write data with USER_ENTERED to prevent apostrophes
        if total_rows <= SHEETS_BATCH_SIZE:
            # Single batch write for normal datasets
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption="USER_ENTERED",  # Prevents apostrophes on numbers
                body={"values": sheet_data}
            ).execute()
            end_time = time.time()
            
            logger.info(f"✅ Enquiry data written successfully in {end_time - start_time:.2f} seconds.")
            logger.info(f"📈 Performance: {len(final_columns)} schema columns, {len(formatted_data)} rows")
            logger.info("✅ Used USER_ENTERED mode to preserve numeric formatting (no apostrophes).")
        else:
            # Split into multiple batches for very large datasets
            logger.info(f"📊 Large dataset detected. Splitting into batches of {SHEETS_BATCH_SIZE} rows...")
            
            # Write headers first
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption="USER_ENTERED",
                body={"values": [final_columns]}
            ).execute()
            
            # Write data in batches
            for i in range(0, len(formatted_data), SHEETS_BATCH_SIZE):
                batch_data = formatted_data[i:i + SHEETS_BATCH_SIZE]
                start_row = i + 2  # +2 because headers are in row 1, and sheets are 1-indexed
                
                # Calculate the actual range for this batch
                end_row = start_row + len(batch_data) - 1
                batch_range = f"{sheet_name}!A{start_row}:{chr(65 + len(final_columns) - 1)}{end_row}"
                
                logger.info(f"🔄 Writing batch {i//SHEETS_BATCH_SIZE + 1}: rows {start_row}-{end_row} ({len(batch_data)} rows, columns A-{chr(65 + len(final_columns) - 1)})")
                
                batch_start_time = time.time()
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=batch_range,
                    valueInputOption="USER_ENTERED",
                    body={"values": batch_data}
                ).execute()
                batch_end_time = time.time()
                
                logger.info(f"✅ Batch {i//SHEETS_BATCH_SIZE + 1} written ({len(batch_data)} rows) in {batch_end_time - batch_start_time:.2f} seconds")
            
            end_time = time.time()
            logger.info(f"✅ All enquiry data written successfully in {end_time - start_time:.2f} seconds.")
        
    except Exception as e:
        logger.error(f"❌ Error writing to Google Sheets: {e}")
        raise

def main():
    """Main execution function with enhanced error handling and performance monitoring"""
    start_time = time.time()
    
    try:
        logger.info("🚀 Starting enhanced Enquiries Firestore to Google Sheets sync...")
        
        # Verify non-interactive mode
        if NON_INTERACTIVE_MODE:
            logger.info("✅ Running in non-interactive mode - using predefined settings")
        
        # Configuration for enquiries (predefined - no user input required)
        collection_name = "acnEnquiries"
        spreadsheet_id = "1pkGrC3RQRxVwkEcb8AZyhT3KICKadw0IW9udkQsQh5k"
        sheet_name = "Enquiries from firebase Sheet"
        
        # Validate required environment variables
        required_env_vars = [
            "FIREBASE_PROJECT_ID", "FIREBASE_PRIVATE_KEY_ID", "FIREBASE_PRIVATE_KEY",
            "FIREBASE_CLIENT_EMAIL", "FIREBASE_CLIENT_ID",
            "GSPREAD_PROJECT_ID", "GSPREAD_PRIVATE_KEY_ID", "GSPREAD_PRIVATE_KEY",
            "GSPREAD_CLIENT_EMAIL", "GSPREAD_CLIENT_ID"
        ]
        
        missing_vars = []
        for var in required_env_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            logger.error(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
            logger.error("Please ensure all required environment variables are set in your .env file")
            return
        
        logger.info("✅ All required environment variables are present")
        
        # Initialize connections
        logger.info("🔧 Initializing connections...")
        initialize_firebase()
        get_sheets_service()
        
        # Fetch enquiry data with batch processing
        logger.info("📥 Fetching enquiry data from Firestore...")
        fetch_start = time.time()
        data = fetch_firestore_data_with_pagination(collection_name)
        fetch_end = time.time()
        
        logger.info(f"⏱️ Enquiry data fetch completed in {fetch_end - fetch_start:.2f} seconds")
        
        if data:
            # Write data with batch processing
            logger.info("📤 Writing enquiry data to Google Sheets...")
            write_start = time.time()
            write_to_google_sheet_batch(data, spreadsheet_id, sheet_name)
            write_end = time.time()
            
            logger.info(f"⏱️ Enquiry data write completed in {write_end - write_start:.2f} seconds")
            
            # Performance summary
            total_time = time.time() - start_time
            records_per_second = len(data) / total_time if total_time > 0 else 0
            
            logger.info("🎉 Enquiries sync completed successfully!")
            logger.info(f"📊 Performance Summary:")
            logger.info(f"   • Total enquiry records: {len(data)}")
            logger.info(f"   • Total time: {total_time:.2f} seconds")
            logger.info(f"   • Processing rate: {records_per_second:.1f} records/second")
            logger.info(f"   • Fetch time: {fetch_end - fetch_start:.2f}s")
            logger.info(f"   • Write time: {write_end - write_start:.2f}s")
        else:
            logger.warning("⚠️ No enquiry data to write to Google Sheets.")
            
    except Exception as e:
        logger.error(f"❌ An error occurred: {e}")
        raise
    finally:
        total_time = time.time() - start_time
        logger.info(f"⏱️ Total execution time: {total_time:.2f} seconds")

if __name__ == "__main__":
    main()