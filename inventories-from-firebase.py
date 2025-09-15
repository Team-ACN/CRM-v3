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
GOOGLE_SHEET_ID           = "1pkGrC3RQRxVwkEcb8AZyhT3KICKadw0IW9udkQsQh5k"
# Sheet name can be set via environment variable or modified directly here

# ---------------------------
# Firestore Collection Name & Sheet Name
# ---------------------------
FIRESTORE_COLLECTION_NAME = "acnTestProperties"
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Inventories from firebase")  # Default to Sheet1 if not specified

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
                item.get("cpId", ""),  # Updated from cpCode
                item.get("propertyName", ""),  # Updated from nameOfTheProperty
                item.get("qcId", ""),  # New field
                item.get("assetType", ""),
                item.get("subType", ""),
                item.get("plotSize", ""),
                item.get("carpet", ""),
                item.get("sbua", ""),
                item.get("facing", ""),
                format_price(item.get("totalAskPrice", "")),  # Format price properly
                format_price(item.get("askPricePerSqft", "")),  # Format price properly
                item.get("unitType", ""),
                item.get("micromarket", ""),
                item.get("communityType", ""),  # New field
                item.get("extraDetails", ""),
                item.get("floorNo", ""),
                convert_unix_to_date(item.get("handoverDate")),  # Now treating as timestamp
                item.get("area", ""),
                item.get("mapLocation", ""),
                convert_unix_to_date(item.get("added")),
                convert_unix_to_date(item.get("dateOfLastChecked")),
                convert_unix_to_datetime(item.get("lastCheck")),  # New timestamp field
                item.get("driveLink", ""),
                item.get("buildingKhata", ""),
                item.get("landKhata", ""),
                item.get("buildingAge", ""),
                item.get("ageOfInventory", ""),
                item.get("ageOfStatus", ""),
                item.get("status", ""),
                item.get("tenanted", ""),
                item.get("ocReceived", ""),  # Now boolean in schema
                item.get("bdaApproved", ""),  # New field
                item.get("biappaApproved", ""),  # New field
                item.get("currentStatus", ""),
                (f"{item.get('_geoloc', {}).get('lat','')}, {item.get('_geoloc', {}).get('lng','')}" if isinstance(item.get('_geoloc'), dict) else ""),
                item.get("exclusive", ""),  # Keeping from original script
                item.get("exactFloor", ""),  # Keeping from original script
                item.get("eKhata", ""),  # Keeping from original script
                ", ".join(item.get("photo", [])) if isinstance(item.get("photo"), list) else item.get("photo", ""),
                ", ".join(item.get("video", [])) if isinstance(item.get("video"), list) else item.get("video", ""),
                ", ".join(item.get("document", [])) if isinstance(item.get("document"), list) else item.get("document", ""),
                item.get("builder_name", ""),  # Keeping from original script
                format_price(item.get("soldPrice", "")),  # Format sold price properly
                convert_unix_to_date(item.get("soldDate", "")),  # Format sold date properly
                extract_kam_info(item.get("soldPrice", "")),  # Extract KAM information
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
        spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)
        
        # Try to get the specified sheet, create if it doesn't exist
        try:
            sheet = spreadsheet.worksheet(GOOGLE_SHEET_NAME)
            print(f"📊 Using existing sheet: {GOOGLE_SHEET_NAME}")
        except gspread.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=GOOGLE_SHEET_NAME, rows="1000", cols="50")
            print(f"📊 Created new sheet: {GOOGLE_SHEET_NAME}")
        
        # Updated Headers to match new schema - FIXED: Added missing Sold Price and Sold Date headers
        headers = [
            "Property ID","CP ID","Property Name","QC ID","Asset Type","Sub Type",
            "Plot Size","Carpet (Sq Ft)","SBUA (Sq ft)","Facing","Total Ask Price (Lacs)",
            "Ask Price / Sqft","Unit Type","Micromarket","Community Type","Extra Details","Floor No.",
            "Handover Date","Area","Map Location","Date of inventory added","Date of status last checked",
            "Last Check","Drive link for more info","Building Khata","Land Khata","Building Age",
            "Age of Inventory","Age of Status","Status","Tenanted or Not",
            "OC Received or not","BDA Approved","BIAPPA Approved","Current Status","Coordinates",
            "Exclusive","Exact Floor","eKhata","Photo","Video","Document","Builder Name",
            "Sold Price (Lacs)","Sold Date","KAM Info"  # FIXED: Added the missing headers and KAM info
        ]
        payload = [headers] + data
        # Sanitize
        sanitized = []
        for row in payload:
            new_row = ["" if (isinstance(cell, float) and math.isnan(cell)) or cell is None else str(cell) for cell in row]
            sanitized.append(new_row)
        
        # Only update the data range without clearing the entire sheet
        # This preserves any additional content outside the data range
        num_rows = len(sanitized)
        num_cols = len(sanitized[0]) if sanitized else 0
        
        # Handle column calculation for ranges beyond Z
        def get_column_letter(col_num):
            """Convert column number to Excel-style column letter (1=A, 26=Z, 27=AA, etc.)"""
            result = ""
            while col_num > 0:
                col_num -= 1  # Adjust for 0-based indexing
                result = chr(65 + (col_num % 26)) + result
                col_num //= 26
            return result
        
        end_column = get_column_letter(num_cols)
        data_range = f"A1:{end_column}{num_rows}"
        
        print(f"📝 Updating range {data_range} with {num_rows} rows and {num_cols} columns")
        sheet.update(values=sanitized, range_name=data_range, value_input_option='USER_ENTERED')
        print(f"✅ Data written successfully to sheet '{GOOGLE_SHEET_NAME}' (dates and prices parsed correctly).")
    except Exception as e:
        print(f"❌ Error writing to Google Sheets: {e}")

# Main
def main():
    print(f"🚀 Starting sync from Firestore collection '{FIRESTORE_COLLECTION_NAME}' to Google Sheet '{GOOGLE_SHEET_NAME}'")
    initialize_firebase()
    data = fetch_firestore_data(FIRESTORE_COLLECTION_NAME)
    if data:
        write_to_google_sheet(data)
    else:
        print("⚠️ No data to write to Google Sheets.")

if __name__ == "__main__":
    main()