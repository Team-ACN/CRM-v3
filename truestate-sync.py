import firebase_admin
from firebase_admin import credentials, firestore
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
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
    "Project ID","Project Name", "Latitude", "Longitude", "Asset Type", "Developer Name", "Micro Market",
    "Promoter Name", "RERA Status", "Status", "QC Percentage"
]

def process_single_doc(item):
    """Processes a single dictionary into a flat list of strings for Sheets."""
    geoloc = item.get("_geoloc", {}) or {}
    
    row = [
        item.get("projectId", ""),
        item.get("projectName", ""),
        geoloc.get("lat", ""),
        geoloc.get("lng", ""),
        item.get("assetType", ""),
        item.get("developerName", ""),
        item.get("microMarket", ""),
        item.get("promoterName", ""),
        item.get("reraStatus", ""),
        item.get("status", ""),
        item.get("qcPercentage", "")
    ]
    
    # Sanitize Nones and NaNs
    return ["" if (isinstance(cell, float) and math.isnan(cell)) or cell is None else str(cell) for cell in row]

def sync_truestate_to_sheets():
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
    collection_ref = db.collection("truEstateApex")
    
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
    
    spreadsheet_id = "1cdFXaTks_PSFkfWbXSBvEeZZIad9CK7JrVhwKavioJc"
    sheet_name = "Data"
    
    # 4. Write to Sheets
    logger.info("Writing to Google Sheets...")
    values = [HEADERS] + processed_data
    
    # Clear the existing sheet completely
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!A:J" 
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
        sync_truestate_to_sheets()
    except Exception as e:
        logger.error(f"Error during sync: {e}", exc_info=True)
