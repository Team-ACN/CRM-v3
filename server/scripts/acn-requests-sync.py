import argparse
import os
import firebase_admin
from firebase_admin import credentials, firestore
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import sys, codecs
import time

# Ensure UTF-8 output
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, 'strict')

# Load environment variables
load_dotenv()

# DB toggle: --db new (default) | --db old
_db_parser = argparse.ArgumentParser(add_help=False)
_db_parser.add_argument('--db', choices=['new', 'old'], default='new')
_db_args, _ = _db_parser.parse_known_args()
_DB_PREFIX = "NEW_" if _db_args.db == 'new' else ""

# ---------------------------
# Configuration
# ---------------------------
FIREBASE_PROJECT_ID       = os.getenv(f"{_DB_PREFIX}FIREBASE_PROJECT_ID")
FIREBASE_PRIVATE_KEY_ID   = os.getenv(f"{_DB_PREFIX}FIREBASE_PRIVATE_KEY_ID")
FIREBASE_PRIVATE_KEY      = os.getenv(f"{_DB_PREFIX}FIREBASE_PRIVATE_KEY", "").replace('\\n', '\n')
FIREBASE_CLIENT_EMAIL     = os.getenv(f"{_DB_PREFIX}FIREBASE_CLIENT_EMAIL")
FIREBASE_CLIENT_ID        = os.getenv(f"{_DB_PREFIX}FIREBASE_CLIENT_ID")

GOOGLE_SHEET_ID = "1Nsjk5OOwm1OCdJqnr-mLO_lEPYmzW8weLVoh2MaBmuw"

# collection -> (sheet name, header row, row-mapper fn)
IST = timezone(timedelta(hours=5, minutes=30))


def convert_unix_to_date(unix_timestamp):
    try:
        if not unix_timestamp:
            return ""
        return datetime.fromtimestamp(int(unix_timestamp), tz=IST).strftime('%m/%d/%Y')
    except Exception:
        return ""


def convert_unix_to_time(unix_timestamp):
    try:
        if not unix_timestamp:
            return ""
        return datetime.fromtimestamp(int(unix_timestamp), tz=IST).strftime('%H:%M:%S')
    except Exception:
        return ""


def sanitize_str(value):
    if isinstance(value, str):
        return value.lstrip("'")
    return str(value) if value is not None else ""


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
            print("Firebase initialized successfully.")
    except Exception as e:
        print(f"Error initializing Firebase: {e}")
        sys.exit(1)


# ---------------------------
# Loan Requests
# ---------------------------
LOAN_COLLECTION = "acnLoanRequests"
LOAN_SHEET_NAME = "Loan"
LOAN_HEADERS = [
    "Request ID", "CP ID", "Agent Name", "Agent Phone", "Bank Key", "Loan Amount",
    "Loan Amount Label", "Bank Payout", "ACN Payout", "Extra Percent",
    "Status", "Source", "Created At", "Updated At"
]


def map_loan_row(data):
    return [
        sanitize_str(data.get("requestId", "")),
        sanitize_str(data.get("cpId", "")),
        sanitize_str(data.get("agentName", "")),
        sanitize_str(data.get("agentPhone", "")),
        sanitize_str(data.get("bankKey", "")),
        data.get("loanAmount", ""),
        sanitize_str(data.get("loanAmountLabel", "")),
        data.get("bankPayout", ""),
        data.get("acnPayout", ""),
        data.get("extraPercent", ""),
        sanitize_str(data.get("status", "")),
        sanitize_str(data.get("source", "")),
        convert_unix_to_date(data.get("createdAt")) + " " + convert_unix_to_time(data.get("createdAt")),
        convert_unix_to_date(data.get("updatedAt")) + " " + convert_unix_to_time(data.get("updatedAt")),
    ]


# ---------------------------
# Service Requests
# ---------------------------
SERVICE_COLLECTION = "acnServiceRequests"
SERVICE_SHEET_NAME = "Legal"
SERVICE_HEADERS = [
    "Request ID", "CP ID", "Agent Name", "Agent Phone", "Service Key",
    "Payout Label", "Status", "Source", "Created At", "Updated At"
]


def map_service_row(data):
    return [
        sanitize_str(data.get("requestId", "")),
        sanitize_str(data.get("cpId", "")),
        sanitize_str(data.get("agentName", "")),
        sanitize_str(data.get("agentPhone", "")),
        sanitize_str(data.get("serviceKey", "")),
        sanitize_str(data.get("payoutLabel", "")),
        sanitize_str(data.get("status", "")),
        sanitize_str(data.get("source", "")),
        convert_unix_to_date(data.get("createdAt")) + " " + convert_unix_to_time(data.get("createdAt")),
        convert_unix_to_date(data.get("updatedAt")) + " " + convert_unix_to_time(data.get("updatedAt")),
    ]


# ---------------------------
# Fetch collection rows
# ---------------------------
def fetch_rows(collection_name, row_mapper):
    db = firestore.client()
    print(f"Fetching documents from {collection_name}...")

    rows = []
    for doc in db.collection(collection_name).stream():
        data = doc.to_dict()
        if not data:
            continue
        rows.append((data.get("createdAt", 0), row_mapper(data)))

    rows.sort(key=lambda item: item[0] or 0, reverse=True)
    print(f"Fetched {len(rows)} docs from {collection_name}.")
    return [row for _, row in rows]


# ---------------------------
# Write to Google Sheets
# ---------------------------
def get_gspread_client():
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
    return gspread.authorize(creds)


def write_to_sheet(gc, sheet_name, headers, rows):
    if not rows:
        print(f"No rows for '{sheet_name}', skipping write.")
        return

    try:
        spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)

        try:
            sheet = spreadsheet.worksheet(sheet_name)
            print(f"Found existing sheet '{sheet_name}'.")
        except gspread.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=len(headers))
            print(f"Created new sheet '{sheet_name}'.")

        payload = [headers] + rows

        print(f"Writing {len(payload)} rows to '{sheet_name}'...")
        sheet.clear()
        sheet.update(range_name="A1", values=payload, value_input_option='USER_ENTERED')
        print(f"'{sheet_name}' write complete.")

    except Exception as e:
        print(f"Error writing to sheet '{sheet_name}': {e}")


# ---------------------------
# Main Execution
# ---------------------------
def main():
    start_time = time.time()
    initialize_firebase()
    gc = get_gspread_client()

    loan_rows = fetch_rows(LOAN_COLLECTION, map_loan_row)
    write_to_sheet(gc, LOAN_SHEET_NAME, LOAN_HEADERS, loan_rows)

    service_rows = fetch_rows(SERVICE_COLLECTION, map_service_row)
    write_to_sheet(gc, SERVICE_SHEET_NAME, SERVICE_HEADERS, service_rows)

    elapsed = time.time() - start_time
    print(f"Done in {elapsed:.2f} seconds.")


if __name__ == "__main__":
    main()
