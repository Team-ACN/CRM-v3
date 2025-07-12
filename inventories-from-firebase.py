import os
import math
import firebase_admin
from firebase_admin import credentials, firestore
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone
from dotenv import load_dotenv
import sys, codecs

# Ensure UTF-8 output (fixes UnicodeEncodeError on Windows)
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, 'strict')

# Load environment variables from .env file
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
GSPREAD_PROJECT_ID        = os.getenv("GSPREAD_PROJECT_ID")
GSPREAD_PRIVATE_KEY_ID    = os.getenv("GSPREAD_PRIVATE_KEY_ID")
GSPREAD_PRIVATE_KEY       = os.getenv("GSPREAD_PRIVATE_KEY", "").replace('\\n', '\n')
GSPREAD_CLIENT_EMAIL      = os.getenv("GSPREAD_CLIENT_EMAIL")
GSPREAD_CLIENT_ID         = os.getenv("GSPREAD_CLIENT_ID")
GOOGLE_SHEET_ID           = "1o6KI4tXt5yfIOHYQ9JH9RI1NK35DKrJRPHNf0srDnuo"

# ---------------------------
# Firestore Collection Name
# ---------------------------
FIRESTORE_COLLECTION_NAME = "ACN123"

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
        # Return in ISO format to ensure Sheets parses as date
        return datetime.fromtimestamp(int(unix_timestamp), tz=timezone.utc).strftime('%Y-%m-%d')
    except Exception as e:
        print(f"⚠️ Error converting timestamp {unix_timestamp}: {e}")
        return ""

# ---------------------------
# Fetch data from Firestore
# ---------------------------
def fetch_firestore_data(collection_name):
    try:
        db = firestore.client()
        print(f"🔍 Checking Firestore collection: {collection_name}")
        docs = list(db.collection(collection_name).stream())
        if not docs:
            print("⚠️ No documents found in Firestore.")
            return []
        print(f"📄 Found {len(docs)} documents.")
        rows = []
        for doc in docs:
            item = doc.to_dict() or {}
            row = [
                item.get("propertyId", ""),
                item.get("cpCode", ""),
                item.get("nameOfTheProperty", ""),
                item.get("assetType", ""),
                item.get("subType", ""),
                item.get("plotSize", ""),
                item.get("carpet", ""),
                item.get("sbua", ""),
                item.get("facing", ""),
                item.get("totalAskPrice", ""),
                item.get("askPricePerSqft", ""),
                item.get("unitType", ""),
                item.get("micromarket", ""),
                item.get("extraDetails", ""),
                item.get("floorNo", ""),
                item.get("handoverDate", ""),
                item.get("area", ""),
                item.get("mapLocation", ""),
                convert_unix_to_date(item.get("dateOfInventoryAdded")),
                convert_unix_to_date(item.get("dateOfStatusLastChecked")),
                item.get("driveLink", ""),
                item.get("buildingKhata", ""),
                item.get("landKhata", ""),
                item.get("buildingAge", ""),
                item.get("ageOfInventory", ""),
                item.get("ageOfStatus", ""),
                item.get("status", ""),
                item.get("tenanted", ""),
                item.get("ocReceived", ""),
                item.get("currentStatus", ""),
                (f"{item.get('_geoloc', {}).get('lat','')}, {item.get('_geoloc', {}).get('lng','')}" if isinstance(item.get('_geoloc'), dict) else ""),
                item.get("exclusive", ""),
                item.get("exactFloor", ""),
                item.get("eKhata", ""),
                ", ".join(item.get("photo", [])) if isinstance(item.get("photo"), list) else item.get("photo", ""),
                ", ".join(item.get("video", [])) if isinstance(item.get("video"), list) else item.get("video", ""),
                ", ".join(item.get("document", [])) if isinstance(item.get("document"), list) else item.get("document", ""),
                item.get("builder_name", ""),
                
            ]
            rows.append(row)
        print(f"✅ Successfully fetched {len(rows)} records from Firestore.")
        return rows
    except Exception as e:
        print(f"❌ Error fetching data from Firestore: {e}")
        return []

# ---------------------------
# Write data to Google Sheets
# ---------------------------
def write_to_google_sheet(data):
    try:
        if not data:
            print("⚠️ No data to write to Google Sheets.")
            return
        creds_data = {
            "type": "service_account",
            "project_id": GSPREAD_PROJECT_ID,
            "private_key_id": GSPREAD_PRIVATE_KEY_ID,
            "private_key": GSPREAD_PRIVATE_KEY,
            "client_email": GSPREAD_CLIENT_EMAIL,
            "client_id": GSPREAD_CLIENT_ID,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{GSPREAD_CLIENT_EMAIL.replace('@','%40')}"
        }
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_info(creds_data, scopes=scopes)
        gc = gspread.authorize(creds)
        sheet = gc.open_by_key(GOOGLE_SHEET_ID).sheet1
        # Headers
        headers = [
            "Property ID","CP Code","Name of The property","Asset Type","Sub Type",
            "Plot Size","Carpet (Sq Ft)","SBUA (Sq ft)","Facing","Total Ask Price (Lacs)",
            "Ask Price / Sqft","Unit Type","Micromarket","Extra Details","Floor No.",
            "Handover Date","Area","Map Location","Date of inventory added","Date of status last checked",
            "Drive link for more info","Building Khata","Land Khata","Building Age",
            "Age of Inventory","Age of Status","Status","Tenanted or Not",
            "OC Received or not","Current Status","Coordinates","Exclusive","Exact Floor",
            "eKhata","Photo","Video","Document","Builder Name"
        ]
        payload = [headers] + data
        # Sanitize
        sanitized = []
        for row in payload:
            new_row = ["" if (isinstance(cell, float) and math.isnan(cell)) or cell is None else str(cell) for cell in row]
            sanitized.append(new_row)
        # Clear then update with USER_ENTERED
        sheet.clear()
        sheet.update("A1", sanitized, value_input_option='USER_ENTERED')
        print("✅ Data written successfully (dates parsed as dates).")
    except Exception as e:
        print(f"❌ Error writing to Google Sheets: {e}")

# Main
def main():
    initialize_firebase()
    data = fetch_firestore_data(FIRESTORE_COLLECTION_NAME)
    if data:
        write_to_google_sheet(data)
    else:
        print("⚠️ No data to write to Google Sheets.")

if __name__ == "__main__":
    main()