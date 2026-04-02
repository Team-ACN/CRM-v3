import os
import math
import logging
import firebase_admin
from firebase_admin import credentials, firestore
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone
from dotenv import load_dotenv
import sys, codecs

# Ensure UTF-8 output (fixes UnicodeEncodeError on Windows)
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, 'strict')

# Load environment variables from .env file
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------
# Firebase Configuration
# ---------------------------
FIREBASE_PROJECT_ID       = os.getenv("FIREBASE_PROJECT_ID")
FIREBASE_PRIVATE_KEY_ID   = os.getenv("FIREBASE_PRIVATE_KEY_ID")
FIREBASE_PRIVATE_KEY      = os.getenv("FIREBASE_PRIVATE_KEY", "").replace('\\n', '\n')
FIREBASE_CLIENT_EMAIL     = os.getenv("FIREBASE_CLIENT_EMAIL")

# ---------------------------
# Google Sheets Configuration
# ---------------------------
GSPREAD_PROJECT_ID        = os.getenv("GSPREAD_PROJECT_ID")
GSPREAD_PRIVATE_KEY       = os.getenv("GSPREAD_PRIVATE_KEY", "").replace('\\n', '\n')
GSPREAD_CLIENT_EMAIL      = os.getenv("GSPREAD_CLIENT_EMAIL")
GOOGLE_SHEET_ID           = "1pkGrC3RQRxVwkEcb8AZyhT3KICKadw0IW9udkQsQh5k"
# Match inventories-from-firebase.py Firestore pagination
FIRESTORE_BATCH_SIZE      = 1000

# ---------------------------
# Firestore Collection Name & Sheet Name
# ---------------------------
FIRESTORE_COLLECTION_NAME = "acnTestProperties"
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Inventories new")

# ---------------------------
# Initialize Firebase (same pattern as inventories-from-firebase.py)
# ---------------------------
def initialize_firebase():
    creds_dict = {
        "type": "service_account",
        "project_id": FIREBASE_PROJECT_ID,
        "private_key_id": FIREBASE_PRIVATE_KEY_ID,
        "private_key": FIREBASE_PRIVATE_KEY,
        "client_email": FIREBASE_CLIENT_EMAIL,
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    if not firebase_admin._apps:
        firebase_admin.initialize_app(credentials.Certificate(creds_dict))
    logger.info("Firebase initialized.")


# ---------------------------
# Helpers
# ---------------------------
def convert_unix_to_date(unix_timestamp):
    """Return YYYY-MM-DD for numeric timestamps (seconds or ms)."""
    try:
        if unix_timestamp in (None, "", 0, "0"):
            return ""
        if isinstance(unix_timestamp, str):
            try:
                unix_timestamp = float(unix_timestamp)
            except ValueError:
                return unix_timestamp
        ts = float(unix_timestamp)
        if ts > 9999999999:  # milliseconds
            ts = ts / 1000
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime('%Y-%m-%d')
    except Exception as e:
        print(f"⚠️ Error converting timestamp {unix_timestamp}: {e}")
        return str(unix_timestamp) if unix_timestamp else ""


def convert_unix_to_datetime(unix_timestamp):
    """Return YYYY-MM-DD HH:MM:SS for numeric timestamps (seconds or ms)."""
    try:
        if unix_timestamp in (None, "", 0, "0"):
            return ""
        if isinstance(unix_timestamp, str):
            try:
                unix_timestamp = float(unix_timestamp)
            except ValueError:
                return unix_timestamp
        ts = float(unix_timestamp)
        if ts > 9999999999:  # milliseconds
            ts = ts / 1000
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"⚠️ Error converting timestamp {unix_timestamp}: {e}")
        return str(unix_timestamp) if unix_timestamp else ""


def format_price(price_value):
    """Normalize price-like values to a clean string."""
    try:
        if price_value in (None, "", "null"):
            return ""
        if isinstance(price_value, dict):
            # If nested price objects arrive, pick common price fields
            for key in ("soldPrice", "totalAskPrice", "pricePerSqft", "value"):
                if key in price_value and price_value.get(key) not in (None, ""):
                    return str(price_value.get(key))
            return ""
        if isinstance(price_value, list):
            if len(price_value) > 0:
                return format_price(price_value[0])
            return ""
        if isinstance(price_value, str):
            cleaned_price = price_value.replace('₹', '').replace(',', '').replace('Rs', '').replace('INR', '').strip()
            try:
                return str(float(cleaned_price))
            except ValueError:
                return price_value
        return str(float(price_value))
    except Exception as e:
        print(f"⚠️ Error formatting price {price_value}: {e}")
        return str(price_value) if price_value else ""


def format_list(value):
    """Join arrays to comma-separated string."""
    if not value:
        return ""
    if isinstance(value, list):
        return ", ".join([str(v) for v in value if v is not None and v != ""])
    return str(value)


def safe_dict(value):
    """Use when Firestore may store null or non-dict for nested objects."""
    return value if isinstance(value, dict) else {}


def column_index_to_letter(col_num):
    """1-based column index to Excel column letters (1=A, 27=AA)."""
    result = ""
    while col_num > 0:
        col_num -= 1
        result = chr(65 + (col_num % 26)) + result
        col_num //= 26
    return result


def sheet_a1_range(sheet_name, a1_suffix):
    """Sheets API range with proper quoting for names with spaces or quotes."""
    safe = sheet_name.replace("'", "''")
    return f"'{safe}'!{a1_suffix}"


UNIFIED_HEADERS = [
    # Core identifiers
    "propertyId",
    "listingType",
    "propertyType",
    "assetType",
    "propertyCategory",
    "commercialPropertyType",
    "commercialSubType",
    # Stakeholder info
    "cpId",
    "agentName",
    "agentPhoneNumber",
    "kamId",
    "kamName",
    # Location
    "propertyName",
    "micromarket",
    "area",
    "zone",
    "communityType",
    "lat",
    "lng",
    "mapLocation",
    "driveLink",
    # Physical characteristics
    "sbua",
    "carpetArea",
    "plotArea",
    "facing",
    # Residential specifics
    "apartmentType",
    "structure",
    "noOfBedrooms",
    "extraRooms",
    "noOfBathrooms",
    "noOfBalconies",
    "balconyFacing",
    # Floor data
    "floorNumber",
    "referredFloorNumber",
    "totalFloors",
    # Commercial specifics
    "noOfSeats",
    "totalRooms",
    "waterSupply",
    "typeOfWaterSupply",
    # Plot specifics
    "plotNo",
    "plotLength",
    "plotBreadth",
    "oddSized",
    # Furnishing & building
    "furnishing",
    "ageOfTheBuilding",
    "possession",
    "availableFrom",
    "readyToMove",
    "handOverDate",
    # Financials
    "totalAskPrice",
    "pricePerSqft",
    # Rental info
    "rent",
    "deposit",
    "maintenance",
    "maintenanceAmount",
    "commissionType",
    "rentalIncome",
    "currentDeposit",
    "startDate",
    "endDate",
    # Tenant preferences
    "preferredTenants",
    "petsAllowed",
    "nonVegAllowed",
    # Features
    "cornerUnit",
    "exclusive",
    "ocReceived",
    # Legal
    "landKhata",
    "buildingKhata",
    "eKhata",
    "biappaApproved",
    "bdaApproved",
    # Amenities / extras
    "amenities",
    "parking",
    "uds",
    "suitableFor",
    # Media
    "photos",
    "videos",
    "documents",
    # Misc
    "extraDetails",
    "unitNumber",
    "status",
    "kamStatus",
    "dataStatus",
    "stage",
    # System metadata
    "added",
    "dateOfLastChecked",
    "lastModified",
    "ageOfInventory",
    "ageOfStatus",
    "source",
]


def build_row(item: dict):
    """Flatten a property dict into the unified header order."""
    geoloc = safe_dict(item.get("_geoloc"))
    pricing = safe_dict(item.get("pricing"))
    rental = safe_dict(item.get("rentalInfo"))
    tenant = safe_dict(item.get("tenantPreferences"))
    features = safe_dict(item.get("features"))
    legal = safe_dict(item.get("legalInfo"))
    media = safe_dict(item.get("media"))

    row_map = {
        "propertyId": item.get("propertyId", ""),
        "listingType": item.get("listingType", ""),
        "propertyType": item.get("propertyType", ""),
        "assetType": item.get("assetType", ""),
        "propertyCategory": item.get("propertyCategory", ""),
        "commercialPropertyType": item.get("commercialPropertyType", ""),
        "commercialSubType": item.get("commercialSubType", ""),
        "cpId": item.get("cpId", ""),
        "agentName": item.get("agentName", ""),
        "agentPhoneNumber": item.get("agentPhoneNumber", ""),
        "kamId": item.get("kamId", ""),
        "kamName": item.get("kamName", ""),
        "propertyName": item.get("propertyName", ""),
        "micromarket": item.get("micromarket", ""),
        "area": item.get("area", ""),
        "zone": item.get("zone", ""),
        "communityType": item.get("communityType", ""),
        "lat": geoloc.get("lat", ""),
        "lng": geoloc.get("lng", ""),
        "mapLocation": item.get("mapLocation", ""),
        "driveLink": item.get("driveLink", ""),
        "sbua": item.get("sbua", ""),
        "carpetArea": item.get("carpetArea", ""),
        "plotArea": item.get("plotArea", ""),
        "facing": item.get("facing", ""),
        "apartmentType": item.get("apartmentType", ""),
        "structure": item.get("structure", ""),
        "noOfBedrooms": item.get("noOfBedrooms", ""),
        "extraRooms": item.get("extraRooms", ""),
        "noOfBathrooms": item.get("noOfBathrooms", ""),
        "noOfBalconies": item.get("noOfBalconies", ""),
        "balconyFacing": item.get("balconyFacing", ""),
        "floorNumber": item.get("floorNumber", item.get("floorNo", "")),
        "referredFloorNumber": item.get("referredFloorNumber", ""),
        "totalFloors": item.get("totalFloors", ""),
        "noOfSeats": item.get("noOfSeats", ""),
        "totalRooms": item.get("totalRooms", ""),
        "waterSupply": item.get("waterSupply", ""),
        "typeOfWaterSupply": item.get("typeOfWaterSupply", ""),
        "plotNo": item.get("plotNo", ""),
        "plotLength": item.get("plotLength", ""),
        "plotBreadth": item.get("plotBreadth", ""),
        "oddSized": item.get("oddSized", ""),
        "furnishing": item.get("furnishing", ""),
        "ageOfTheBuilding": item.get("ageOfTheBuilding", ""),
        "possession": item.get("possession", ""),
        "availableFrom": convert_unix_to_date(item.get("availableFrom")),
        "readyToMove": item.get("readyToMove", ""),
        "handOverDate": convert_unix_to_date(item.get("handOverDate")),
        "totalAskPrice": format_price(pricing.get("totalAskPrice", "")),
        "pricePerSqft": format_price(pricing.get("pricePerSqft", "")),
        "rent": format_price(rental.get("rent", "")),
        "deposit": format_price(rental.get("deposit", "")),
        "maintenance": rental.get("maintenance", ""),
        "maintenanceAmount": format_price(rental.get("maintenanceAmount", "")),
        "commissionType": rental.get("commissionType", ""),
        "rentalIncome": format_price(rental.get("rentalIncome", "")),
        "currentDeposit": format_price(rental.get("currentDeposit", "")),
        "startDate": convert_unix_to_date(rental.get("startDate")),
        "endDate": convert_unix_to_date(rental.get("endDate")),
        "preferredTenants": format_list(tenant.get("preferredTenants")),
        "petsAllowed": tenant.get("petsAllowed", ""),
        "nonVegAllowed": tenant.get("nonVegAllowed", ""),
        "cornerUnit": features.get("cornerUnit", ""),
        "exclusive": features.get("exclusive", ""),
        "ocReceived": features.get("ocReceived", ""),
        "landKhata": legal.get("landKhata", ""),
        "buildingKhata": legal.get("buildingKhata", ""),
        "eKhata": legal.get("eKhata", ""),
        "biappaApproved": legal.get("biappaApproved", ""),
        "bdaApproved": legal.get("bdaApproved", ""),
        "amenities": format_list(item.get("amenities")),
        "parking": item.get("parking", ""),
        "uds": item.get("uds", ""),
        "suitableFor": item.get("suitableFor", ""),
        "photos": format_list(media.get("photos")),
        "videos": format_list(media.get("videos")),
        "documents": format_list(media.get("documents")),
        "extraDetails": item.get("extraDetails", ""),
        "unitNumber": item.get("unitNumber", ""),
        "status": item.get("status", ""),
        "kamStatus": item.get("kamStatus", ""),
        "dataStatus": item.get("dataStatus", ""),
        "stage": item.get("stage", ""),
        "added": convert_unix_to_datetime(item.get("added")),
        "dateOfLastChecked": convert_unix_to_datetime(item.get("dateOfLastChecked")),
        "lastModified": convert_unix_to_datetime(item.get("lastModified")),
        "ageOfInventory": item.get("ageOfInventory", ""),
        "ageOfStatus": item.get("ageOfStatus", ""),
        "source": item.get("source", ""),
    }
    return [row_map.get(key, "") for key in UNIFIED_HEADERS]


# ---------------------------
# Fetch data from Firestore (batched pagination, same as inventories-from-firebase.py)
# ---------------------------
def fetch_firestore_data(collection_name):
    try:
        db = firestore.client()
        logger.info("Fetching documents from Firestore in batches...")
        collection_ref = db.collection(collection_name)
        rows = []
        count = 0
        last_doc = None
        while True:
            query = collection_ref.order_by("__name__").limit(FIRESTORE_BATCH_SIZE)
            if last_doc:
                query = query.start_after(last_doc)
            docs = list(query.stream())
            if not docs:
                break
            for doc in docs:
                item = doc.to_dict() or {}
                rows.append(build_row(item))
                count += 1
            last_doc = docs[-1]
            logger.info("Processed %s documents...", count)
        if not rows:
            logger.info("No documents found in Firestore.")
            return []
        logger.info("Finished processing %s documents.", count)
        return rows
    except Exception as e:
        logger.error("Error fetching data from Firestore: %s", e, exc_info=True)
        return []


# ---------------------------
# Write data to Google Sheets (Sheets API v4: clear + update, same as inventories-from-firebase.py)
# ---------------------------
def write_to_google_sheet(data):
    if not data:
        logger.info("No data to write to Google Sheets.")
        return
    try:
        sheets_creds_dict = {
            "type": "service_account",
            "project_id": GSPREAD_PROJECT_ID,
            "private_key": GSPREAD_PRIVATE_KEY,
            "client_email": GSPREAD_CLIENT_EMAIL,
            "token_uri": "https://oauth2.googleapis.com/token",
        }
        creds = Credentials.from_service_account_info(
            sheets_creds_dict,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)

        headers = UNIFIED_HEADERS
        payload = [headers] + data
        sanitized = []
        for row in payload:
            new_row = [
                ""
                if (isinstance(cell, float) and math.isnan(cell)) or cell is None
                else str(cell)
                for cell in row
            ]
            sanitized.append(new_row)

        num_cols = len(UNIFIED_HEADERS)
        end_col = column_index_to_letter(num_cols)
        clear_range = sheet_a1_range(GOOGLE_SHEET_NAME, f"A:{end_col}")
        update_range = sheet_a1_range(GOOGLE_SHEET_NAME, "A1")

        logger.info("Writing to Google Sheets...")
        service.spreadsheets().values().clear(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=clear_range,
        ).execute()
        service.spreadsheets().values().update(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=update_range,
            valueInputOption="USER_ENTERED",
            body={"values": sanitized},
        ).execute()
        logger.info("Sync completed successfully for sheet '%s'.", GOOGLE_SHEET_NAME)
    except Exception as e:
        logger.error("Error writing to Google Sheets: %s", e, exc_info=True)

# Main
def main():
    print(
        f"🚀 Starting sync from Firestore collection '{FIRESTORE_COLLECTION_NAME}' "
        f"to Google Sheet '{GOOGLE_SHEET_NAME}'"
    )
    try:
        initialize_firebase()
        data = fetch_firestore_data(FIRESTORE_COLLECTION_NAME)
        if data:
            write_to_google_sheet(data)
        else:
            logger.info("No data to sync.")
    except Exception as e:
        logger.error("Error during sync: %s", e, exc_info=True)


if __name__ == "__main__":
    main()