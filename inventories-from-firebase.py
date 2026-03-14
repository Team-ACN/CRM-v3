import firebase_admin
from firebase_admin import credentials, firestore
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
import logging
import math

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

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

def format_date(ts):
    if not ts: return ""
    try:
        ts = float(ts)
        if ts > 9999999999: ts /= 1000  # Convert ms to seconds
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime('%Y-%m-%d')
    except:
        return str(ts)

def format_datetime(ts):
    if not ts: return ""
    try:
        ts = float(ts)
        if ts > 9999999999: ts /= 1000
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return str(ts)

def format_price(price_value):
    if not price_value: return ""
    try:
        if isinstance(price_value, dict):
            return str(price_value.get('soldPrice', ''))
        if isinstance(price_value, list) and price_value and isinstance(price_value[0], dict):
            return str(price_value[0].get('soldPrice', ''))
        if isinstance(price_value, str):
            cleaned = price_value.replace('₹', '').replace(',', '').replace('Rs', '').replace('INR', '').strip()
            return str(float(cleaned))
        return str(float(price_value))
    except:
        return str(price_value)

def extract_kam_info(sold_data):
    if not sold_data: return ""
    try:
        data = sold_data[0] if isinstance(sold_data, list) and sold_data else sold_data
        if isinstance(data, dict):
            kam_name = data.get('kamName', '')
            kam_id = data.get('kamId', '')
            platform = data.get('sellingPlatform', '')
            if kam_name or kam_id:
                return f"{kam_name} ({kam_id})" + (f" - {platform}" if platform else "")
    except:
        pass
    return ""

def process_single_doc(item):
    """Processes a single dictionary into a flat list of strings for Sheets."""
    pricing = item.get("pricing", {}) or {}
    media = item.get("media", {}) or {}
    geoloc = item.get("_geoloc", {}) or {}
    
    row = [
        item.get("propertyId", ""), item.get("cpId", ""), item.get("propertyName", ""),
        item.get("qcId", ""), item.get("assetType", ""), item.get("subType", ""),
        item.get("plotArea", ""), item.get("carpet", ""), item.get("sbua", ""),
        item.get("facing", ""), format_price(pricing.get("totalAskPrice")),
        format_price(pricing.get("pricePerSqft")), item.get("noOfBedrooms", ""),
        item.get("micromarket", ""), item.get("communityType", ""), item.get("extraDetails", ""),
        item.get("floorNo", ""), format_date(item.get("handoverDate")), item.get("zone", ""),
        item.get("mapLocation", ""), format_date(item.get("added")),
        format_date(item.get("dateOfLastChecked")), format_datetime(item.get("lastCheck")),
        item.get("driveLink", ""), item.get("buildingKhata", ""), item.get("landKhata", ""),
        item.get("buildingAge", ""), item.get("ageOfInventory", ""), item.get("ageOfStatus", ""),
        item.get("status", ""), item.get("tenanted", ""), item.get("ocReceived", ""),
        item.get("bdaApproved", ""), item.get("biappaApproved", ""), item.get("currentStatus", ""),
        f"{geoloc.get('lat','')}, {geoloc.get('lng','')}" if geoloc else "",
        item.get("exclusive", ""), item.get("exactFloor", ""), item.get("eKhata", ""),
        ", ".join(media.get("photos", [])) if isinstance(media.get("photos"), list) else media.get("photos", ""),
        ", ".join(media.get("videos", [])) if isinstance(media.get("videos"), list) else media.get("videos", ""),
        ", ".join(media.get("documents", [])) if isinstance(media.get("documents"), list) else media.get("documents", ""),
        item.get("source", ""), item.get("listingType", ""),
        format_price(item.get("soldPrice")), format_date(item.get("soldDate")),
        extract_kam_info(item.get("soldPrice", ""))
    ]
    
    # Sanitize Nones and NaNs
    return ["" if (isinstance(cell, float) and math.isnan(cell)) or cell is None else str(cell) for cell in row]

def sync_firestore_to_sheets():
    # 1. Initialize Firebase
    creds_dict = {
        "type": "service_account",
        "project_id": os.getenv("FIREBASE_PROJECT_ID"),
        "private_key_id": os.getenv("FIREBASE_PRIVATE_KEY_ID"),
        "private_key": os.getenv("FIREBASE_PRIVATE_KEY", "").replace("\\n", "\n"),
        "client_email": os.getenv("FIREBASE_CLIENT_EMAIL"),
        "token_uri": "https://oauth2.googleapis.com/token"
    }
    if not firebase_admin._apps:
        firebase_admin.initialize_app(credentials.Certificate(creds_dict))
    
    db = firestore.client()
    
    # 2. Batch Fetch Data from Firestore (Prevent Timeouts)
    logger.info("Fetching documents from Firestore in batches...")
    collection_ref = db.collection("acnTestProperties")
    
    processed_data = []
    count = 0
    batch_size = 1000
    last_doc = None
    
    while True:
        query = collection_ref.order_by("__name__").limit(batch_size)
        if last_doc:
            query = query.start_after(last_doc)
            
        docs = list(query.stream())
        if not docs:
            break
            
        for doc in docs:
            processed_data.append(process_single_doc(doc.to_dict()))
            count += 1
        
        last_doc = docs[-1]
        logger.info(f"Processed {count} documents...")
            
    logger.info(f"Finished processing {count} documents.")

    if not processed_data:
        logger.info("No data found to sync.")
        return

    # 3. Initialize Sheets
    sheets_creds_dict = {
        "type": "service_account",
        "project_id": os.getenv("GSPREAD_PROJECT_ID"),
        "private_key": os.getenv("GSPREAD_PRIVATE_KEY", "").replace("\\n", "\n"),
        "client_email": os.getenv("GSPREAD_CLIENT_EMAIL"),
        "token_uri": "https://oauth2.googleapis.com/token"
    }
    creds = Credentials.from_service_account_info(sheets_creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    
    spreadsheet_id = "1pkGrC3RQRxVwkEcb8AZyhT3KICKadw0IW9udkQsQh5k"
    sheet_name = os.getenv("GOOGLE_SHEET_NAME", "Inventories from firebase")
    
    # 4. Write to Sheets
    logger.info("Writing to Google Sheets...")
    values = [HEADERS] + processed_data
    
    # Clear the existing sheet completely
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!A:AW" # AW accommodates the 47 columns
    ).execute()
    
    # Update with new data
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!A1",
        valueInputOption="USER_ENTERED",
        body={"values": values}
    ).execute()
    
    logger.info("✅ Sync completed successfully!")

if __name__ == "__main__":
    try:
        sync_firestore_to_sheets()
    except Exception as e:
        logger.error(f"Error during sync: {e}", exc_info=True)