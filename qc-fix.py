import os
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
from dotenv import load_dotenv
import sys, codecs
import csv

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
# Firestore Collection Name
# ---------------------------
FIRESTORE_COLLECTION_NAME = "acnQCInventoriesTest"

# ---------------------------
# CSV Output Configuration
# ---------------------------
OUTPUT_CSV_FILE = f"qc_inventories_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

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
            row = {
                "propertyId": item.get("propertyId", ""),
                "stage": item.get("stage", ""),
                "status": item.get("status", ""),
                "qcStatus": item.get("qcStatus", "")
            }
            rows.append(row)
        print(f"✅ Successfully fetched {len(rows)} records from Firestore.")
        return rows
    except Exception as e:
        print(f"❌ Error fetching data from Firestore: {e}")
        return []

# ---------------------------
# Write data to CSV
# ---------------------------
def write_to_csv(data, filename):
    try:
        if not data:
            print("⚠️ No data to write to CSV.")
            return
        
        # Define headers
        headers = ["propertyId", "stage", "status", "qcStatus"]
        
        # Write to CSV
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)
        
        print(f"✅ Data written successfully to {filename}")
        print(f"📁 Total records exported: {len(data)}")
    except Exception as e:
        print(f"❌ Error writing to CSV: {e}")

# Main
def main():
    print(f"🚀 Starting CSV export from Firestore collection '{FIRESTORE_COLLECTION_NAME}'")
    initialize_firebase()
    data = fetch_firestore_data(FIRESTORE_COLLECTION_NAME)
    if data:
        write_to_csv(data, OUTPUT_CSV_FILE)
    else:
        print("⚠️ No data to export.")

if __name__ == "__main__":
    main()