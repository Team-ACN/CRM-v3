import argparse
import firebase_admin
from firebase_admin import credentials, firestore
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
import logging
import math
import time
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# DB toggle: --db new (default) | --db old
_db_parser = argparse.ArgumentParser(add_help=False)
_db_parser.add_argument('--db', choices=['new', 'old'], default='new')
_db_args, _ = _db_parser.parse_known_args()
_DB_PREFIX = "NEW_" if _db_args.db == 'new' else ""

# Performance config
FETCH_BATCH_SIZE = 5000
MAX_WORKERS      = min(32, (os.cpu_count() or 1) * 4)
CHUNK_SIZE       = 500
WRITE_BATCH_SIZE = 50000

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

def process_single_doc_new(item):
    """Processes a new-schema Properties document into a flat list of strings for Sheets.
    Schema changes vs old:
    - media: array of {cloudinaryUrl, firebaseUrl, type, url} maps (not {photos,videos,documents})
    - pricing removed: totalAskPrice, pricePerSqft now top-level
    - field renames: floorNo→floorNumber, noOfBedrooms→bedroom, buildingAge→ageOfBuilding,
                     exclusive→isExclusive, bdaApproved→isBdaApproved,
                     biappaApproved→isBiapaApproved, eKhata→hasEKhata,
                     mapLocation→location, communityType→societyType,
                     subType→apartmentSubType, currentStatus→dataStatus
    - documents now top-level array
    """
    geoloc = item.get("_geoloc", {}) or {}
    media_list = item.get("media", []) or []
    if not isinstance(media_list, list):
        media_list = []

    photos = [m.get("url") or "" for m in media_list if isinstance(m, dict) and m.get("type") == "image"]
    videos = [m.get("url") or "" for m in media_list if isinstance(m, dict) and m.get("type") == "video"]
    docs_list = item.get("documents", []) or []
    documents = [str(d) for d in docs_list if d] if isinstance(docs_list, list) else []

    kam_name = item.get("kamName", "")
    kam_id = item.get("kamId", "")
    kam_info = f"{kam_name} ({kam_id})" if (kam_name or kam_id) else ""

    row = [
        item.get("propertyId", ""),            # Property ID
        item.get("cpId", ""),                  # CP ID
        item.get("propertyName", ""),          # Property Name
        item.get("qcId", ""),                  # QC ID
        item.get("assetType", ""),             # Asset Type
        item.get("apartmentSubType", ""),      # Sub Type
        item.get("plotArea", ""),              # Plot Size
        item.get("carpet", ""),                # Carpet (Sq Ft)
        item.get("sbua", ""),                  # SBUA (Sq ft)
        item.get("facing", ""),                # Facing
        format_price(item.get("totalAskPrice")),   # Total Ask Price (Lacs)
        format_price(item.get("pricePerSqft")),    # Ask Price / Sqft
        item.get("bedroom", ""),               # noOfBedrooms
        item.get("micromarket", ""),           # Micromarket
        item.get("societyType", ""),           # Community Type
        item.get("extraDetails", ""),          # Extra Details
        item.get("floorNumber", ""),           # Floor No.
        item.get("possession", ""),            # Handover Date
        item.get("zone", ""),                  # Zone
        item.get("location", ""),              # Map Location
        format_date(item.get("added")),        # Date of inventory added
        format_date(item.get("dateOfLastChecked")),  # Date of status last checked
        format_datetime(item.get("lastModified")),   # Last Check
        item.get("driveLink", ""),             # Drive link for more info
        item.get("buildingKhata", ""),         # Building Khata
        item.get("landKhata", ""),             # Land Khata
        item.get("ageOfBuilding", ""),         # Building Age
        item.get("ageOfInventory", ""),        # Age of Inventory
        item.get("ageOfStatus", ""),           # Age of Status
        item.get("status", ""),                # Status
        item.get("tenanted", ""),              # Tenanted or Not
        str(item.get("ocReceived", "")),       # OC Received or not
        str(item.get("isBdaApproved", "")),    # BDA Approved
        str(item.get("isBiapaApproved", "")), # BIAPPA Approved
        item.get("dataStatus", ""),            # Current Status
        f"{geoloc.get('lat','')}, {geoloc.get('lng','')}" if geoloc else "",  # Coordinates
        str(item.get("isExclusive", "")),      # Exclusive
        item.get("referredFloorNumber", ""),   # Exact Floor
        str(item.get("hasEKhata", "")),        # eKhata
        ", ".join(photos),                     # Photos
        ", ".join(videos),                     # Videos
        ", ".join(documents),                  # Documents
        item.get("source", ""),                # Source
        item.get("listingType", ""),           # listingType
        "",                                    # Sold Price (Lacs) — not in new schema
        "",                                    # Sold Date — not in new schema
        kam_info,                              # KAM Info
    ]

    return ["" if (isinstance(cell, float) and math.isnan(cell)) or cell is None else str(cell) for cell in row]


def _init_firebase():
    creds_dict = {
        "type": "service_account",
        "project_id": os.getenv(f"{_DB_PREFIX}FIREBASE_PROJECT_ID"),
        "private_key_id": os.getenv(f"{_DB_PREFIX}FIREBASE_PRIVATE_KEY_ID"),
        "private_key": os.getenv(f"{_DB_PREFIX}FIREBASE_PRIVATE_KEY", "").replace("\\n", "\n"),
        "client_email": os.getenv(f"{_DB_PREFIX}FIREBASE_CLIENT_EMAIL"),
        "token_uri": "https://oauth2.googleapis.com/token"
    }
    if not firebase_admin._apps:
        firebase_admin.initialize_app(credentials.Certificate(creds_dict))
    return firestore.client(database_id="default")


def _init_sheets():
    sheets_creds = {
        "type": "service_account",
        "project_id": os.getenv("GSPREAD_PROJECT_ID"),
        "private_key": os.getenv("GSPREAD_PRIVATE_KEY", "").replace("\\n", "\n"),
        "client_email": os.getenv("GSPREAD_CLIENT_EMAIL"),
        "token_uri": "https://oauth2.googleapis.com/token"
    }
    creds = Credentials.from_service_account_info(
        sheets_creds, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _fetch_and_process(db, collection_name: str) -> list:
    """Fetch + convert in one pass — never stores raw docs, minimises peak RAM."""
    collection_ref = db.collection(collection_name)
    PAGE_SIZE = 1000
    processor = process_single_doc_new if _db_args.db == 'new' else process_single_doc

    def fetch_page(last_doc):
        q = collection_ref.order_by("__name__").limit(PAGE_SIZE)
        if last_doc is not None:
            q = q.start_after(last_doc)
        return list(q.stream())

    logger.info(f"🚀 Fetching+processing '{collection_name}' | page={PAGE_SIZE}")
    t0 = time.time()
    rows = []
    count = 0

    with ThreadPoolExecutor(max_workers=2) as ex:
        future = ex.submit(fetch_page, None)
        while True:
            page = future.result()
            if not page:
                break
            future = ex.submit(fetch_page, page[-1]) if len(page) == PAGE_SIZE else None
            for doc in page:
                item = doc.to_dict()
                if item:
                    rows.append(processor(item))
                count += 1
            if count % 5000 == 0:
                logger.info(f"  📦 {count} docs ({count / (time.time() - t0):.0f}/s)")
            if future is None:
                break

    logger.info(f"✅ Fetched+processed {count} docs → {len(rows)} rows in {time.time()-t0:.2f}s")
    return rows


def _write_sheets(service, rows: list, spreadsheet_id: str, sheet_name: str):
    """Write all rows via single batchUpdate call."""
    all_rows = [HEADERS] + rows
    total = len(all_rows)
    quoted = f"'{sheet_name}'"
    t0 = time.time()

    logger.info(f"🧹 Clearing sheet...")
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id, range=f"{quoted}!A:AW", body={}
    ).execute()

    logger.info(f"📝 Writing {total} rows via batchUpdate...")
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
        spreadsheetId=spreadsheet_id,
        body={"valueInputOption": "USER_ENTERED", "data": batch_data, "includeValuesInResponse": False}
    ).execute()

    logger.info(f"✅ Written {total} rows in {time.time()-t0:.2f}s")


def sync_firestore_to_sheets():
    start = time.time()
    logger.info("="*60)
    logger.info("⚡ ULTRA-FAST FIRESTORE TO SHEETS SYNC (Inventories)")
    logger.info(f"🔧 {MAX_WORKERS} workers | fetch batch: {FETCH_BATCH_SIZE} | chunk: {CHUNK_SIZE}")
    logger.info(f"🔑 DB: {'NEW' if _DB_PREFIX else 'OLD'} | Project: {os.getenv(f'{_DB_PREFIX}FIREBASE_PROJECT_ID')}")
    logger.info("="*60)

    collection_name = "Properties" if _db_args.db == 'new' else "acnTestProperties"
    spreadsheet_id  = "1pkGrC3RQRxVwkEcb8AZyhT3KICKadw0IW9udkQsQh5k"
    sheet_name      = os.getenv("GOOGLE_SHEET_NAME", "Inventories from firebase")

    logger.info("\n📥 Fetching + processing data")
    db = _init_firebase()
    rows = _fetch_and_process(db, collection_name)
    if not rows:
        logger.warning("No documents found.")
        return

    # Phase 3: write
    logger.info("\n📤 PHASE 3: Writing to Sheets")
    service = _init_sheets()
    _write_sheets(service, rows, spreadsheet_id, sheet_name)

    logger.info(f"\n🎉 Total time: {time.time()-start:.2f}s | {len(rows)} records")

if __name__ == "__main__":
    try:
        sync_firestore_to_sheets()
    except Exception as e:
        logger.error(f"Error during sync: {e}", exc_info=True)