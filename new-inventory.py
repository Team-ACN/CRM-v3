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
if hasattr(sys.stdout, 'buffer'):
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
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Inventories new")

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
    geoloc = item.get("_geoloc", {}) or {}
    pricing = item.get("pricing", {}) or {}
    rental = item.get("rentalInfo", {}) or {}
    tenant = item.get("tenantPreferences", {}) or {}
    features = item.get("features", {}) or {}
    legal = item.get("legalInfo", {}) or {}
    media = item.get("media", {}) or {}

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
# Fetch data from Firestore
# ---------------------------
def fetch_firestore_data(collection_name):
    try:
        db = firestore.client()
        print(f"🔍 Checking Firestore collection: {collection_name}")
        rows = []
        count = 0
        for doc in db.collection(collection_name).stream():
            item = doc.to_dict() or {}
            rows.append(build_row(item))
            count += 1
        if count == 0:
            print("⚠️ No documents found in Firestore.")
            return []
        print(f"✅ Successfully fetched {count} records from Firestore.")
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
            sheet = spreadsheet.add_worksheet(title=GOOGLE_SHEET_NAME, rows="1000", cols="150")
            print(f"📊 Created new sheet: {GOOGLE_SHEET_NAME}")
        
        headers = UNIFIED_HEADERS
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