import argparse
import os
import math
import time
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
import firebase_admin
from firebase_admin import credentials, firestore
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timezone
from dotenv import load_dotenv
import sys, codecs

# Ensure UTF-8 output (fixes UnicodeEncodeError on Windows)
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, 'strict')

# Load environment variables from .env file
load_dotenv()

# DB toggle: --db new (default) | --db old
_db_parser = argparse.ArgumentParser(add_help=False)
_db_parser.add_argument('--db', choices=['new', 'old'], default='new')
_db_args, _ = _db_parser.parse_known_args()
_DB_PREFIX = "NEW_" if _db_args.db == 'new' else ""

# ---------------------------
# Firebase Configuration
# ---------------------------
FIREBASE_PROJECT_ID       = os.getenv(f"{_DB_PREFIX}FIREBASE_PROJECT_ID")
FIREBASE_PRIVATE_KEY_ID   = os.getenv(f"{_DB_PREFIX}FIREBASE_PRIVATE_KEY_ID")
FIREBASE_PRIVATE_KEY      = os.getenv(f"{_DB_PREFIX}FIREBASE_PRIVATE_KEY", "").replace('\\n', '\n')
FIREBASE_CLIENT_EMAIL     = os.getenv(f"{_DB_PREFIX}FIREBASE_CLIENT_EMAIL")
FIREBASE_CLIENT_ID        = os.getenv(f"{_DB_PREFIX}FIREBASE_CLIENT_ID")

# ---------------------------
# Google Sheets Configuration
# ---------------------------
GSPREAD_PROJECT_ID        = os.getenv("GSPREAD_PROJECT_ID")
GSPREAD_PRIVATE_KEY_ID    = os.getenv("GSPREAD_PRIVATE_KEY_ID")
GSPREAD_PRIVATE_KEY       = os.getenv("GSPREAD_PRIVATE_KEY", "").replace('\\n', '\n')
GSPREAD_CLIENT_EMAIL      = os.getenv("GSPREAD_CLIENT_EMAIL")
GSPREAD_CLIENT_ID         = os.getenv("GSPREAD_CLIENT_ID")
GOOGLE_SHEET_ID           = "1pkGrC3RQRxVwkEcb8AZyhT3KICKadw0IW9udkQsQh5k"
# Sheet name can be set via environment variable or modified directly here
SHEETS_WRITE_BATCH_SIZE   = 500
FETCH_BATCH_SIZE          = 5000
MAX_WORKERS               = min(32, (os.cpu_count() or 1) * 4)
CHUNK_SIZE                = 500
WRITE_BATCH_SIZE          = 50000

# ---------------------------
# Firestore Collection Name & Sheet Name
# ---------------------------
FIRESTORE_COLLECTION_NAME = "acnQCInventoriesTest"
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "QC Inventories")  # Default to Sheet1 if not specified

# ---------------------------
# Initialize Firebase Admin SDK
# ---------------------------
def initialize_firebase():
    try:
        cred_data = {
            "type": "service_account",
            "project_id": FIREBASE_PROJECT_ID,
            "private_key_id": FIREBASE_PRIVATE_KEY_ID,
            "private_key": FIREBASE_PRIVATE_KEY,
            "client_email": FIREBASE_CLIENT_EMAIL,
            "client_id": FIREBASE_CLIENT_ID,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{FIREBASE_CLIENT_EMAIL.replace('@', '%40')}"
        }
        cred = credentials.Certificate(cred_data)
        firebase_admin.initialize_app(cred)
        print("✅ Firebase initialized successfully.")
    except Exception as e:
        print(f"❌ Error initializing Firebase: {e}")

# ---------------------------
# Convert Unix timestamp to ISO date
# ---------------------------
def convert_unix_to_date(unix_timestamp):
    try:
        if not unix_timestamp:
            return ""
        
        # If it's already a string (like "Ready-to-move"), return as-is
        if isinstance(unix_timestamp, str):
            # Try to convert to number, if it fails, return the string
            try:
                unix_timestamp = float(unix_timestamp)
            except ValueError:
                return unix_timestamp  # Return the string as-is
        
        # Handle both integer and float timestamps
        timestamp_num = float(unix_timestamp)
        
        # Check if it's milliseconds (13+ digits) and convert to seconds
        if timestamp_num > 9999999999:  # More than 10 digits = milliseconds
            timestamp_num = timestamp_num / 1000
        
        timestamp_int = int(timestamp_num)
        
        # Return in ISO format to ensure Sheets parses as date
        return datetime.fromtimestamp(timestamp_int, tz=timezone.utc).strftime('%Y-%m-%d')
    except Exception as e:
        print(f"⚠️ Error converting timestamp {unix_timestamp}: {e}")
        return str(unix_timestamp) if unix_timestamp else ""

# ---------------------------
# Convert Unix timestamp to ISO datetime
# ---------------------------
def convert_unix_to_datetime(unix_timestamp):
    try:
        if not unix_timestamp:
            return ""
        
        # If it's already a string, try to convert to number, if it fails, return the string
        if isinstance(unix_timestamp, str):
            try:
                unix_timestamp = float(unix_timestamp)
            except ValueError:
                return unix_timestamp  # Return the string as-is
        
        # Handle both integer and float timestamps
        timestamp_num = float(unix_timestamp)
        
        # Check if it's milliseconds (13+ digits) and convert to seconds
        if timestamp_num > 9999999999:  # More than 10 digits = milliseconds
            timestamp_num = timestamp_num / 1000
        
        timestamp_int = int(timestamp_num)
        
        # Return in ISO format with time
        return datetime.fromtimestamp(timestamp_int, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"⚠️ Error converting timestamp {unix_timestamp}: {e}")
        return str(unix_timestamp) if unix_timestamp else ""

# ---------------------------
# Format price values properly
# ---------------------------
def format_price(price_value):
    """Format price values to ensure proper display in sheets"""
    try:
        if not price_value:
            return ""
        
        # Handle dictionary/object values (like KAM info with soldPrice)
        if isinstance(price_value, dict):
            # If it's a dict, try to extract the soldPrice or return empty
            sold_price = price_value.get('soldPrice', '')
            if sold_price is not None and sold_price != '':
                return str(sold_price)
            return ""
        
        # Handle list values
        if isinstance(price_value, list):
            # If it's a list, try to find soldPrice in the first item or return empty
            if len(price_value) > 0 and isinstance(price_value[0], dict):
                sold_price = price_value[0].get('soldPrice', '')
                if sold_price is not None and sold_price != '':
                    return str(sold_price)
            return ""
        
        # If it's already a string, try to convert to number for formatting
        if isinstance(price_value, str):
            # Remove any currency symbols or commas
            cleaned_price = price_value.replace('₹', '').replace(',', '').replace('Rs', '').replace('INR', '').strip()
            try:
                price_num = float(cleaned_price)
                return str(price_num)  # Return as string to preserve formatting
            except ValueError:
                return price_value  # Return original if can't convert
        
        # If it's a number, format it properly
        return str(float(price_value))
    except Exception as e:
        print(f"⚠️ Error formatting price {price_value}: {e}")
        return str(price_value) if price_value else ""

# ---------------------------
# Extract KAM information from soldPrice data
# ---------------------------
def extract_kam_info(sold_data):
    """Extract KAM information from soldPrice field if it contains KAM data"""
    try:
        if not sold_data:
            return ""
        
        # Handle dictionary values
        if isinstance(sold_data, dict):
            kam_name = sold_data.get('kamName', '')
            kam_id = sold_data.get('kamId', '')
            platform = sold_data.get('sellingPlatform', '')
            if kam_name or kam_id:
                return f"{kam_name} ({kam_id})" + (f" - {platform}" if platform else "")
            return ""
        
        # Handle list values
        if isinstance(sold_data, list) and len(sold_data) > 0:
            first_item = sold_data[0]
            if isinstance(first_item, dict):
                kam_name = first_item.get('kamName', '')
                kam_id = first_item.get('kamId', '')
                platform = first_item.get('sellingPlatform', '')
                if kam_name or kam_id:
                    return f"{kam_name} ({kam_id})" + (f" - {platform}" if platform else "")
        
        return ""
    except Exception as e:
        print(f"⚠️ Error extracting KAM info {sold_data}: {e}")
        return ""

# ---------------------------
# Safely read nested dictionaries
# ---------------------------
def safe_dict(value):
    """Return a dict when value is dict, otherwise an empty dict."""
    return value if isinstance(value, dict) else {}

# ---------------------------
# Fetch data from Firestore
# ---------------------------
def fetch_firestore_data(collection_name):
    """Fetch all docs via stream, convert to_dict in parallel."""
    db = firestore.client()
    collection_ref = db.collection(collection_name)
    print(f"🚀 Streaming from: {collection_name} | workers: {MAX_WORKERS}")
    t0 = time.time()
    raw_docs = []
    count = 0

    doc_stream = collection_ref.stream()
    while True:
        batch = list(islice(doc_stream, FETCH_BATCH_SIZE))
        if not batch:
            break
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = [ex.submit(lambda d: d.to_dict(), doc) for doc in batch]
            for f in as_completed(futures):
                try:
                    raw_docs.append(f.result())
                    count += 1
                    if count % 5000 == 0:
                        print(f"  📦 Fetched {count} docs ({count/(time.time()-t0):.0f}/s)")
                except Exception as e:
                    print(f"Doc conversion error: {e}")
        del batch

    print(f"✅ Fetched {count} docs in {time.time()-t0:.2f}s")
    return raw_docs


# ---------------------------
# Process documents into rows
# ---------------------------
def process_documents(raw_docs):
    """Process docs into rows in parallel chunks."""
    if not raw_docs:
        return []

    def process_chunk(chunk):
        rows = []
        for item in chunk:
            if not item:
                continue
            pricing   = safe_dict(item.get("pricing"))
            geoloc    = safe_dict(item.get("_geoloc"))
            media_raw = item.get("media")
            if isinstance(media_raw, list):
                # New schema: array of {type, url} maps
                photos_str    = ", ".join([m.get("url") or "" for m in media_raw if isinstance(m, dict) and m.get("type") == "image"])
                videos_str    = ", ".join([m.get("url") or "" for m in media_raw if isinstance(m, dict) and m.get("type") == "video"])
                docs_list     = item.get("documents", []) or []
                documents_str = ", ".join([str(d) for d in docs_list if d]) if isinstance(docs_list, list) else ""
            else:
                # Old schema: map with photos/videos/documents arrays
                media         = safe_dict(media_raw)
                mp            = media.get("photos", [])
                mv            = media.get("videos", [])
                md            = media.get("documents", [])
                photos_str    = ", ".join(mp) if isinstance(mp, list) else str(mp or "")
                videos_str    = ", ".join(mv) if isinstance(mv, list) else str(mv or "")
                documents_str = ", ".join(md) if isinstance(md, list) else str(md or "")
            row = [
                item.get("propertyId", ""),
                item.get("cpId", ""),
                item.get("propertyName", ""),
                item.get("qcId", ""),
                item.get("assetType", ""),
                item.get("subType", ""),
                item.get("plotSize", ""),
                item.get("carpet", ""),
                item.get("sbua", ""),
                item.get("facing", ""),
                format_price(pricing.get("totalAskPrice", "")),
                format_price(pricing.get("pricePerSqft", "")),
                item.get("noOfBedrooms", ""),
                item.get("micromarket", ""),
                item.get("communityType", ""),
                item.get("extraDetails", ""),
                item.get("floorNo", ""),
                convert_unix_to_date(item.get("handoverDate")),
                item.get("area", ""),
                item.get("mapLocation", ""),
                convert_unix_to_date(item.get("added")),
                convert_unix_to_date(item.get("dateOfLastChecked")),
                convert_unix_to_datetime(item.get("lastCheck")),
                item.get("driveLink", ""),
                item.get("buildingKhata", ""),
                item.get("landKhata", ""),
                item.get("buildingAge", ""),
                item.get("ageOfInventory", ""),
                item.get("ageOfStatus", ""),
                item.get("status", ""),
                item.get("tenanted", ""),
                item.get("ocReceived", ""),
                item.get("bdaApproved", ""),
                item.get("biappaApproved", ""),
                item.get("currentStatus", ""),
                (f"{geoloc.get('lat','')}, {geoloc.get('lng','')}" if geoloc else ""),
                item.get("exclusive", ""),
                item.get("exactFloor", ""),
                item.get("eKhata", ""),
                photos_str,
                videos_str,
                documents_str,
                item.get("source", ""),
                item.get("builder_name", ""),
                format_price(item.get("soldPrice", "")),
                convert_unix_to_date(item.get("soldDate", "")),
                extract_kam_info(item.get("soldPrice", "")),
                item.get("kamName", ""),
                convert_unix_to_date(item.get("lastModified", "")),
            ]
            rows.append(["" if (isinstance(cell, float) and math.isnan(cell)) or cell is None else str(cell) for cell in row])
        return rows

    chunks = [raw_docs[i:i + CHUNK_SIZE] for i in range(0, len(raw_docs), CHUNK_SIZE)]
    print(f"⚡ Processing {len(raw_docs)} docs in {len(chunks)} chunks | workers: {MAX_WORKERS}")
    t0 = time.time()
    rows = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(process_chunk, c) for c in chunks]
        for f in as_completed(futures):
            try:
                rows.extend(f.result())
            except Exception as e:
                print(f"Chunk error: {e}")

    print(f"✅ Processed {len(rows)} rows in {time.time()-t0:.2f}s")
    return rows

# ---------------------------
# Write data to Google Sheets
# ---------------------------
def write_to_google_sheet(data):
    if not data:
        print("⚠️ No data to write.")
        return
    try:
        headers = [
            "Property ID","CP ID","Property Name","QC ID","Asset Type","Sub Type",
            "Plot Size","Carpet (Sq Ft)","SBUA (Sq ft)","Facing","Total Ask Price (Lacs)",
            "Ask Price / Sqft","Unit Type","Micromarket","Community Type","Extra Details","Floor No.",
            "Handover Date","Area","Map Location","Date of inventory added","Date of status last checked",
            "Last Check","Drive link for more info","Building Khata","Land Khata","Building Age",
            "Age of Inventory","Age of Status","Status","Tenanted or Not",
            "OC Received or not","BDA Approved","BIAPPA Approved","Current Status","Coordinates",
            "Exclusive","Exact Floor","eKhata","Photos","Videos","Documents","Source","Builder Name",
            "Sold Price (Lacs)","Sold Date","KAM Info","KAM Name","Last Modified"
        ]
        creds_data = {
            "type": "service_account",
            "project_id": GSPREAD_PROJECT_ID,
            "private_key_id": GSPREAD_PRIVATE_KEY_ID,
            "private_key": GSPREAD_PRIVATE_KEY,
            "client_email": GSPREAD_CLIENT_EMAIL,
            "client_id": GSPREAD_CLIENT_ID,
            "token_uri": "https://oauth2.googleapis.com/token",
        }
        creds = Credentials.from_service_account_info(
            creds_data, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        quoted = f"'{GOOGLE_SHEET_NAME}'"
        t0 = time.time()

        all_rows = [headers] + data
        total = len(all_rows)

        print(f"🧹 Clearing sheet...")
        service.spreadsheets().values().clear(
            spreadsheetId=GOOGLE_SHEET_ID, range=f"{quoted}!A:AW", body={}
        ).execute()

        print(f"📝 Writing {total} rows via batchUpdate...")
        batch_data = []
        for i in range(0, total, WRITE_BATCH_SIZE):
            chunk = all_rows[i:i + WRITE_BATCH_SIZE]
            start_row = i + 1
            batch_data.append({
                "range": f"{quoted}!A{start_row}:AW{start_row + len(chunk) - 1}",
                "values": chunk,
                "majorDimension": "ROWS"
            })

        service.spreadsheets().values().batchUpdate(
            spreadsheetId=GOOGLE_SHEET_ID,
            body={"valueInputOption": "USER_ENTERED", "data": batch_data, "includeValuesInResponse": False}
        ).execute()

        print(f"✅ Written {total} rows in {time.time()-t0:.2f}s")
    except Exception as e:
        print(f"❌ Error writing to Google Sheets: {e}")

# Main
def main():
    start = time.time()
    print("="*60)
    print("⚡ ULTRA-FAST FIRESTORE TO SHEETS SYNC (QC Inventories)")
    print(f"🔧 {MAX_WORKERS} workers | fetch batch: {FETCH_BATCH_SIZE} | chunk: {CHUNK_SIZE}")
    print(f"🔑 DB: {'NEW' if _DB_PREFIX else 'OLD'} | Collection: {FIRESTORE_COLLECTION_NAME}")
    print("="*60)

    initialize_firebase()

    print(f"\n📥 PHASE 1: Fetching data from '{FIRESTORE_COLLECTION_NAME}'")
    raw_docs = fetch_firestore_data(FIRESTORE_COLLECTION_NAME)
    if not raw_docs:
        print("⚠️ No documents found.")
        return

    print(f"\n⚙️  PHASE 2: Processing {len(raw_docs)} documents")
    rows = process_documents(raw_docs)
    del raw_docs
    gc.collect()

    print(f"\n📤 PHASE 3: Writing to Sheets '{GOOGLE_SHEET_NAME}'")
    write_to_google_sheet(rows)

    print(f"\n🎉 Total time: {time.time()-start:.2f}s | {len(rows)} records")

if __name__ == "__main__":
    main()