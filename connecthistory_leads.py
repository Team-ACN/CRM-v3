import os
import math
import firebase_admin
from firebase_admin import credentials, firestore
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone
from dotenv import load_dotenv
import sys, codecs
import time
from concurrent.futures import ThreadPoolExecutor

# Ensure UTF-8 output
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, 'strict')

# Load environment variables
load_dotenv()

# ---------------------------
# Configuration
# ---------------------------
FIREBASE_PROJECT_ID       = os.getenv("FIREBASE_PROJECT_ID")
FIREBASE_PRIVATE_KEY_ID   = os.getenv("FIREBASE_PRIVATE_KEY_ID")
FIREBASE_PRIVATE_KEY      = os.getenv("FIREBASE_PRIVATE_KEY", "").replace('\\n', '\n')
FIREBASE_CLIENT_EMAIL     = os.getenv("FIREBASE_CLIENT_EMAIL")
FIREBASE_CLIENT_ID        = os.getenv("FIREBASE_CLIENT_ID")

GOOGLE_SHEET_ID           = "1pkGrC3RQRxVwkEcb8AZyhT3KICKadw0IW9udkQsQh5k"
SHEET_NAME                = "Connect History Leads"
FIRESTORE_COLLECTION_NAME = "acnLeads"

MAX_WORKERS = 8  # Increased workers for purely CPU/memory processing of fetched docs

# ---------------------------
# Initialize Firebase Admin SDK
# ---------------------------
def initialize_firebase():
    try:
        if not firebase_admin._apps:
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
        sys.exit(1)

# ---------------------------
# Helper Functions
# ---------------------------
def convert_unix_to_date(unix_timestamp):
    try:
        if not unix_timestamp: return ""
        return datetime.fromtimestamp(int(unix_timestamp), tz=timezone.utc).strftime('%m/%d/%Y')
    except: return ""

def convert_unix_to_time(unix_timestamp):
    try:
        if not unix_timestamp: return ""
        return datetime.fromtimestamp(int(unix_timestamp), tz=timezone.utc).strftime('%H:%M:%S')
    except: return ""

def sanitize_str(value):
    if isinstance(value, str):
        return value.lstrip("'")
    return str(value) if value is not None else ""

# ---------------------------
# Process Single Document
# ---------------------------
def process_doc(doc_snapshot):
    """
    Extracts connect history from a single Firestore document snapshot.
    Returns a list of rows (lists).
    """
    rows = []
    try:
        data = doc_snapshot.to_dict()
        if not data:
            return []

        connect_history = data.get("connectHistory", [])
        if not connect_history or not isinstance(connect_history, list):
            return []

        lead_id = sanitize_str(data.get("leadId", ""))
        name = sanitize_str(data.get("name", ""))
        kam_name = sanitize_str(data.get("kamName", ""))

        for entry in connect_history:
            if isinstance(entry, dict):
                timestamp = entry.get("timestamp", 0)
                row = {
                    'leadId': lead_id,
                    'name': name,
                    'kamName': kam_name,
                    'date': convert_unix_to_date(timestamp),
                    'time': convert_unix_to_time(timestamp),
                    'connection': sanitize_str(entry.get("connection", "")),
                    'connectMedium': sanitize_str(entry.get("connectMedium", "")),
                    'direction': sanitize_str(entry.get("direction", "")),
                    'connectBy': sanitize_str(entry.get("connectBy", "")),
                    'timestamp_raw': timestamp # Used for sorting later
                }
                rows.append(row)
    except Exception:
        pass # Skip malformed docs silently
    return rows

# ---------------------------
# Fetch and Process Data
# ---------------------------
def fetch_and_process_data():
    db = firestore.client()
    print(f"🔍 Fetching all documents from {FIRESTORE_COLLECTION_NAME}...")
    
    # Stream all documents (efficient for memory)
    docs_stream = db.collection(FIRESTORE_COLLECTION_NAME).stream()
    
    # Materialize list to split into chunks for parallel processing if needed, 
    # but for simple processing, a direct iteration or thread pool map is fine.
    # We use the stream directly with the executor to avoid loading all into memory.
    total_docs = "Unknown (Streaming)" 
    print(f"📄 Processing documents stream...")

    all_history_rows = []
    
    # Use ThreadPoolExecutor to process documents in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = executor.map(process_doc, docs_stream)
        
        for res in results:
            all_history_rows.extend(res)

    if not all_history_rows:
        print("⚠️ No connection history found.")
        return []

    print(f"✅ Extracted {len(all_history_rows)} history entries. Sorting...")
    
    # Sort by timestamp descending
    all_history_rows.sort(key=lambda x: x['timestamp_raw'], reverse=True)

    # Convert to final list of lists for Sheets
    final_rows = []
    for item in all_history_rows:
        final_rows.append([
            item['leadId'],
            item['name'],
            item['kamName'],
            item['date'],
            item['time'],
            item['connection'],
            item['connectMedium'],
            item['direction'],
            item['connectBy']
        ])
        
    return final_rows

# ---------------------------
# Write to Google Sheets
# ---------------------------
def write_to_sheet(rows):
    if not rows:
        return

    try:
        creds_data = {
            "type": "service_account",
            "project_id": os.getenv("GSPREAD_PROJECT_ID"),
            "private_key_id": os.getenv("GSPREAD_PRIVATE_KEY_ID"),
            "private_key": os.getenv("GSPREAD_PRIVATE_KEY", "").replace('\\n', '\n'),
            "client_email": os.getenv("GSPREAD_CLIENT_EMAIL"),
            "client_id": os.getenv("GSPREAD_CLIENT_ID"),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{os.getenv('GSPREAD_CLIENT_EMAIL').replace('@', '%40')}"
        }
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_info(creds_data, scopes=scopes)
        gc = gspread.authorize(creds)
        
        spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)
        
        try:
            sheet = spreadsheet.worksheet(SHEET_NAME)
            print(f"✅ Found existing sheet '{SHEET_NAME}'.")
        except gspread.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=1000, cols=10)
            print(f"✅ Created new sheet '{SHEET_NAME}'.")

        headers = [
            "Lead ID", "Lead Name", "KAM Name", "Date", "Time", 
            "Connection Status", "Connect Medium", "Direction", "Connect By"
        ]
        
        payload = [headers] + rows
        
        # Batch update
        print(f"📝 Writing {len(payload)} rows to Sheets...")
        sheet.clear()
        sheet.update(range_name="A1", values=payload, value_input_option='USER_ENTERED')
        print("✅ Data write complete.")
        
    except Exception as e:
        print(f"❌ Error writing to Google Sheet: {e}")

# ---------------------------
# Main Execution
# ---------------------------
def main():
    start_time = time.time()
    initialize_firebase()
    data = fetch_and_process_data()
    if data:
        write_to_sheet(data)
    
    elapsed = time.time() - start_time
    print(f"🎉 Done in {elapsed:.2f} seconds.")

if __name__ == "__main__":
    main()
