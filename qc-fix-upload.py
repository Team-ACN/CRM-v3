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
# CSV Input Configuration
# ---------------------------
INPUT_CSV_FILE = "/Users/samarth/Documents/development/crm v3/Untitled spreadsheet - Sheet1.csv"

# ---------------------------
# Batch Configuration
# ---------------------------
BATCH_SIZE = 500  # Firestore limit is 500 operations per batch

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
# Read data from CSV
# ---------------------------
def read_from_csv(filename):
    try:
        if not os.path.exists(filename):
            print(f"❌ CSV file not found: {filename}")
            return []
        
        data = []
        with open(filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                data.append(row)
        
        print(f"✅ Successfully read {len(data)} records from {filename}")
        return data
    except Exception as e:
        print(f"❌ Error reading CSV file: {e}")
        return []

# ---------------------------
# Fetch all documents and create propertyId mapping
# ---------------------------
def fetch_all_documents(collection_name):
    try:
        db = firestore.client()
        print(f"🔍 Fetching all documents from Firestore collection: {collection_name}...")
        
        docs = list(db.collection(collection_name).stream())
        
        # Create a mapping of propertyId -> document reference
        property_map = {}
        for doc in docs:
            doc_data = doc.to_dict()
            property_id = doc_data.get('propertyId')
            if property_id:
                property_map[property_id] = doc.reference
        
        print(f"✅ Fetched {len(docs)} documents, mapped {len(property_map)} properties")
        return property_map
    except Exception as e:
        print(f"❌ Error fetching documents: {e}")
        return {}

# ---------------------------
# Upload data to Firestore using batch updates
# ---------------------------
def upload_to_firestore(data, collection_name, property_map):
    try:
        if not data:
            print("⚠️ No data to upload.")
            return
        
        db = firestore.client()
        
        success_count = 0
        error_count = 0
        not_found_count = 0
        
        print(f"📤 Preparing batch updates...")
        
        # Prepare all update operations
        update_operations = []
        
        for idx, row in enumerate(data, 1):
            property_id = row.get('propertyId', '').strip()
            
            if not property_id:
                print(f"⚠️ Row {idx}: Skipping - No propertyId found")
                error_count += 1
                continue
            
            # Look up document reference from map
            doc_ref = property_map.get(property_id)
            
            if not doc_ref:
                print(f"⚠️ Row {idx}: Property {property_id} not found in Firestore")
                not_found_count += 1
                continue
            
            # Prepare update data (only update non-empty values)
            update_data = {}
            
            if row.get('stage', '').strip():
                update_data['stage'] = row['stage'].strip()
            
            if row.get('status', '').strip():
                update_data['status'] = row['status'].strip()
            
            if row.get('qcStatus', '').strip():
                update_data['qcStatus'] = row['qcStatus'].strip()
            
            # Add update timestamp
            update_data['updatedAt'] = firestore.SERVER_TIMESTAMP
            
            if update_data:
                update_operations.append({
                    'doc_ref': doc_ref,
                    'data': update_data,
                    'property_id': property_id,
                    'row': idx
                })
                print(f"✓ Row {idx}: Prepared update for {property_id}")
            else:
                print(f"⚠️ Row {idx}: No fields to update for {property_id}")
                error_count += 1
        
        # Now perform batch updates
        if update_operations:
            print(f"\n📝 Executing batch updates for {len(update_operations)} documents...")
            
            # Process in batches of BATCH_SIZE
            total_batches = (len(update_operations) + BATCH_SIZE - 1) // BATCH_SIZE
            
            for i in range(0, len(update_operations), BATCH_SIZE):
                batch = db.batch()
                batch_ops = update_operations[i:i + BATCH_SIZE]
                batch_num = i // BATCH_SIZE + 1
                
                print(f"\n🔄 Processing batch {batch_num}/{total_batches} ({len(batch_ops)} operations)...")
                
                for op in batch_ops:
                    batch.update(op['doc_ref'], op['data'])
                
                try:
                    batch.commit()
                    success_count += len(batch_ops)
                    print(f"✅ Batch {batch_num}: Successfully committed {len(batch_ops)} updates")
                    
                except Exception as e:
                    print(f"❌ Batch {batch_num}: Error committing batch: {e}")
                    error_count += len(batch_ops)
        
        print(f"\n{'='*60}")
        print(f"📊 Upload Summary:")
        print(f"   ✅ Successfully updated: {success_count}")
        print(f"   ⚠️ Not found: {not_found_count}")
        print(f"   ❌ Errors: {error_count}")
        print(f"   📝 Total processed: {len(data)}")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"❌ Error uploading to Firestore: {e}")

# Main
def main():
    print(f"🚀 Starting CSV import to Firestore collection '{FIRESTORE_COLLECTION_NAME}'")
    print(f"📁 Reading from: {INPUT_CSV_FILE}")
    
    initialize_firebase()
    
    # Step 1: Read CSV
    data = read_from_csv(INPUT_CSV_FILE)
    
    if not data:
        print("⚠️ No data to upload.")
        return
    
    # Step 2: Fetch all documents and create mapping
    property_map = fetch_all_documents(FIRESTORE_COLLECTION_NAME)
    
    if not property_map:
        print("❌ Could not fetch documents from Firestore.")
        return
    
    # Step 3: Ask for confirmation
    print(f"\n⚠️  About to update {len(data)} records in Firestore using batch operations.")
    confirm = input("Do you want to proceed? (yes/no): ").strip().lower()
    
    if confirm in ['yes', 'y']:
        upload_to_firestore(data, FIRESTORE_COLLECTION_NAME, property_map)
    else:
        print("❌ Upload cancelled by user.")

if __name__ == "__main__":
    main()