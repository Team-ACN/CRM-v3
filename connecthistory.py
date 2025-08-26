import os
import math
import firebase_admin
from firebase_admin import credentials, firestore
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone
from dotenv import load_dotenv
import sys, codecs

# Ensure UTF-8 output
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, 'strict')

# Load environment variables
load_dotenv()

# ---------------------------
# Firebase Configuration
# ---------------------------
FIREBASE_PROJECT_ID       = os.getenv("FIREBASE_PROJECT_ID")
FIREBASE_PRIVATE_KEY_ID   = os.getenv("FIREBASE_PRIVATE_KEY_ID")
FIREBASE_PRIVATE_KEY      = os.getenv("FIREBASE_PRIVATE_KEY", "").replace('\\n', '\n')
FIREBASE_CLIENT_EMAIL     = os.getenv("FIREBASE_CLIENT_EMAIL")
FIREBASE_CLIENT_ID        = os.getenv("FIREBASE_CLIENT_ID")

# ---------------------------
# Google Sheets Configuration
# ---------------------------
GOOGLE_SHEET_ID = "1pkGrC3RQRxVwkEcb8AZyhT3KICKadw0IW9udkQsQh5k"
SHEET_NAME      = "Connect History from firebase"

# ---------------------------
# Firestore Collection Name
# ---------------------------
FIRESTORE_COLLECTION_NAME = os.getenv("FIRESTORE_AGENTS_COLLECTION_NAME", "acnAgents")

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
# Convert Unix timestamp to separate date and time
# ---------------------------
def convert_unix_to_date(unix_timestamp):
    try:
        if not unix_timestamp:
            return ""
        return datetime.fromtimestamp(int(unix_timestamp), tz=timezone.utc).strftime('%m/%d/%Y')
    except Exception as e:
        print(f"⚠️ Error converting timestamp to date {unix_timestamp}: {e}")
        return ""

def convert_unix_to_time(unix_timestamp):
    try:
        if not unix_timestamp:
            return ""
        return datetime.fromtimestamp(int(unix_timestamp), tz=timezone.utc).strftime('%H:%M:%S')
    except Exception as e:
        print(f"⚠️ Error converting timestamp to time {unix_timestamp}: {e}")
        return ""

# ---------------------------
# Sanitize strings (remove leading apostrophes)
# ---------------------------
def sanitize_str(value):
    if isinstance(value, str):
        return value.lstrip("'")
    return str(value) if value is not None else ""

# ---------------------------
# Fetch connect history data from Firestore agents collection
# ---------------------------
def fetch_connect_history_data(collection_name):
    try:
        db = firestore.client()
        print(f"🔍 Fetching Firestore collection: {collection_name}")
        docs = list(db.collection(collection_name).stream())
        if not docs:
            print("⚠️ No documents found in Firestore.")
            return []

        print(f"📄 Found {len(docs)} documents.")
        
        # Create a list to store all connection history entries with timestamps for sorting
        all_connections = []
        
        for doc in docs:
            try:
                agent_data = doc.to_dict()
                connect_history = agent_data.get("connectHistory", [])
                
                # Skip agents with no connection history
                if not connect_history or not isinstance(connect_history, list):
                    continue
                
                # Process each connection history entry
                for history_entry in connect_history:
                    if isinstance(history_entry, dict):
                        connection_data = {
                            'cpId': sanitize_str(agent_data.get("cpId", "")),
                            'name': sanitize_str(agent_data.get("name", "")),
                            'kamName': sanitize_str(agent_data.get("kamName", "")),
                            'connectMedium': sanitize_str(history_entry.get("connectMedium", "")),
                            'connection': sanitize_str(history_entry.get("connection", "")),
                            'direction': sanitize_str(history_entry.get("direction", "")),
                            'timestamp': history_entry.get("timestamp", 0),
                            'date': convert_unix_to_date(history_entry.get("timestamp", 0)),
                            'time': convert_unix_to_time(history_entry.get("timestamp", 0))
                        }
                        all_connections.append(connection_data)
            
            except Exception as doc_err:
                print(f"⚠️ Error processing document {doc.id}: {doc_err}")
        
        if not all_connections:
            print("⚠️ No connection history found in any documents.")
            return []
        
        # Sort by timestamp in descending order (newest first)
        all_connections.sort(key=lambda x: x['timestamp'], reverse=True)
        print(f"✅ Found {len(all_connections)} connection history entries, sorted by timestamp (newest first).")
        
        # Convert to rows format
        rows = []
        for conn in all_connections:
            row = [
                conn['cpId'],
                conn['name'],
                conn['kamName'],
                conn['date'],
                conn['time'],
                conn['connection'],
                conn['connectMedium'],
                conn['direction']
            ]
            rows.append(row)
        
        print(f"✅ Successfully processed {len(rows)} connection history records.")
        return rows
        
    except Exception as e:
        print(f"❌ Error fetching data from Firestore: {e}")
        return []

# ---------------------------
# Write data to Google Sheet
# ---------------------------
def write_to_google_sheet(data):
    try:
        if not data:
            print("⚠️ No data to write to Google Sheets.")
            return

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
        print("✅ Google Sheets API authenticated.")

        # Try to open existing sheet, create if it doesn't exist
        try:
            spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)
            try:
                sheet = spreadsheet.worksheet(SHEET_NAME)
                print(f"✅ Opened existing sheet '{SHEET_NAME}'.")
            except gspread.WorksheetNotFound:
                sheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=1000, cols=20)
                print(f"✅ Created new sheet '{SHEET_NAME}'.")
        except Exception as e:
            print(f"❌ Error accessing Google Sheet: {e}")
            return

        headers = [
            "CP ID",
            "Agent Name", 
            "KAM Name",
            "Date",
            "Time",
            "Connection Status",
            "Connect Medium",
            "Direction"
        ]
        payload = [headers] + data

        # Sanitize data
        sanitized = []
        for row in payload:
            sanitized_row = [str(cell) if cell != "nan" and cell is not None else "" for cell in row]
            sanitized.append(sanitized_row)

        # Clear and update sheet
        sheet.clear()
        print("✅ Sheet cleared.")
        sheet.update("A1", sanitized, value_input_option='USER_ENTERED')
        print(f"✅ Data written successfully. Total rows: {len(sanitized)}")
        
    except Exception as e:
        print(f"❌ Error writing to Google Sheets: {e}")

# ---------------------------
# Main
# ---------------------------
def main():
    initialize_firebase()
    data = fetch_connect_history_data(FIRESTORE_COLLECTION_NAME)
    if data:
        write_to_google_sheet(data)
    else:
        print("⚠️ No connection history data to write.")

if __name__ == "__main__":
    main()