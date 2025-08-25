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
SHEET_NAME      = "Requirements from firebase"

# ---------------------------
# Firestore Collection Name
# ---------------------------
FIRESTORE_COLLECTION_NAME = os.getenv("FIRESTORE_REQUIREMENTS_COLLECTION_NAME", "acnRequirements")

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
# Convert Unix timestamp to readable date
# ---------------------------
def convert_unix_to_date(unix_timestamp):
    try:
        if not unix_timestamp:
            return ""
        return datetime.fromtimestamp(int(unix_timestamp), tz=timezone.utc).strftime('%d/%b/%Y')
    except Exception as e:
        print(f"⚠️ Error converting timestamp {unix_timestamp}: {e}")
        return ""

# ---------------------------
# Sanitize strings (remove leading apostrophes)
# ---------------------------
def sanitize_str(value):
    if isinstance(value, str):
        return value.lstrip("'")
    return str(value) if value is not None else ""

# ---------------------------
# Process array fields (convert to comma-separated string)
# ---------------------------
def process_array_field(array_field):
    try:
        if isinstance(array_field, list):
            return ", ".join([str(item) for item in array_field])
        return ""
    except Exception as e:
        print(f"⚠️ Error processing array field: {e}")
        return ""

# ---------------------------
# Fetch data from Firestore requirements collection
# ---------------------------
def fetch_requirements_data(collection_name):
    try:
        db = firestore.client()
        print(f"🔍 Fetching Firestore collection: {collection_name}")
        docs = list(db.collection(collection_name).stream())
        if not docs:
            print("⚠️ No documents found in Firestore.")
            return []

        print(f"📄 Found {len(docs)} documents.")
        
        # Create a list to store documents with their data for sorting
        doc_data = []
        for doc in docs:
            try:
                item = doc.to_dict()
                # Store the document data along with the 'added' timestamp for sorting
                doc_data.append({
                    'data': item,
                    'added_timestamp': item.get("added", 0)  # Default to 0 if no 'added' field
                })
            except Exception as doc_err:
                print(f"⚠️ Error processing document {doc.id}: {doc_err}")
        
        # Sort by 'added' field in descending order (newest first)
        doc_data.sort(key=lambda x: x['added_timestamp'], reverse=True)
        print(f"✅ Documents sorted by 'added' field (newest first).")
        
        # Process the sorted documents
        rows = []
        for doc_item in doc_data:
            try:
                item = doc_item['data']
                budget = item.get("budget", {}) or {}
                
                row = [
                    sanitize_str(item.get("requirementId", "")),
                    sanitize_str(item.get("cpId", "")),  # Changed from agentCpid to cpId
                    sanitize_str(item.get("agentName", "")),  # New field
                    sanitize_str(item.get("agentPhoneNumber", "")),  # New field
                    sanitize_str(item.get("propertyName", "")),
                    sanitize_str(item.get("assetType", "")),
                    sanitize_str(item.get("configuration", "")),
                    sanitize_str(item.get("bedrooms", "")),  # New field
                    sanitize_str(item.get("bathrooms", "")),  # New field
                    sanitize_str(item.get("parking", "")),  # New field
                    sanitize_str(convert_unix_to_date(item.get("added"))),
                    sanitize_str(convert_unix_to_date(item.get("lastModified"))),
                    sanitize_str(item.get("area", "")),
                    sanitize_str(budget.get("from", "")),
                    sanitize_str(budget.get("to", "")),
                    sanitize_str(item.get("marketValue", "")),
                    sanitize_str(item.get("extraDetails", "")),  # Changed from requirementDetails to extraDetails
                    sanitize_str(item.get("micromarket", "")),  # New field
                    sanitize_str(item.get("status", "")),
                    sanitize_str(item.get("requirementStatus", "")),  # New field
                    sanitize_str(item.get("internalStatus", "")),  # New field
                    sanitize_str(item.get("kamId", "")),  # New field
                    sanitize_str(item.get("kamName", "")),  # New field
                    sanitize_str(item.get("kamPhoneNumber", "")),  # New field
                    process_array_field(item.get("matchingProperties", [])),  # New array field
                    process_array_field(item.get("notes", []))  # New array field
                ]
                rows.append(row)
            except Exception as doc_err:
                print(f"⚠️ Error processing sorted document: {doc_err}")
        
        print(f"✅ Successfully fetched and sorted {len(rows)} records.")
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

        sheet = gc.open_by_key(GOOGLE_SHEET_ID).worksheet(SHEET_NAME)
        print(f"✅ Opened sheet '{SHEET_NAME}'.")

        headers = [
            "Requirement ID", "CP ID", "Agent Name", "Agent Phone Number", "Property Name", 
            "Asset Type", "Configuration", "Bedrooms", "Bathrooms", "Parking",
            "Added Date", "Last Modified Date", "Area", "Budget From", "Budget To",
            "Market Value", "Extra Details", "Micromarket", "Status", 
            "Requirement Status", "Internal Status", "KAM ID", "KAM Name", 
            "KAM Phone Number", "Matching Properties", "Notes"
        ]
        payload = [headers] + data

        sanitized = []
        for row in payload:
            sanitized_row = [cell if cell != "nan" else "" for cell in row]
            sanitized.append(sanitized_row)

        sheet.clear()
        print("✅ Sheet cleared.")
        # Use USER_ENTERED so dates and numbers are parsed
        sheet.update("A1", sanitized, value_input_option='USER_ENTERED')
        print("✅ Data written successfully.")
    except Exception as e:
        print(f"❌ Error writing to Google Sheets: {e}")

# ---------------------------
# Main
# ---------------------------
def main():
    initialize_firebase()
    data = fetch_requirements_data(FIRESTORE_COLLECTION_NAME)
    if data:
        write_to_google_sheet(data)
    else:
        print("⚠️ No data to write.")

if __name__ == "__main__":
    main()