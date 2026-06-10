import argparse
import csv
import os
import time
import logging
from datetime import datetime

import firebase_admin
from firebase_admin import credentials, firestore
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

UPDATABLE_FIELDS = ["propertyName", "projectId", "listingType", "societyType", "status", "kamName"]
BATCH_SIZE = 500

parser = argparse.ArgumentParser(description="Update Firebase properties from CSV (matched by propertyId)")
parser.add_argument('--db', choices=['new', 'old'], default='new')
parser.add_argument('--input', default='', help='Input CSV path')
parser.add_argument('--dry-run', action='store_true', help='Preview changes without writing to Firebase')
args = parser.parse_args()

_DB_PREFIX = "NEW_" if args.db == 'new' else ""
COLLECTION  = "Properties" if args.db == 'new' else "acnTestProperties"


def _init_firebase():
    creds_dict = {
        "type": "service_account",
        "project_id":      os.getenv(f"{_DB_PREFIX}FIREBASE_PROJECT_ID"),
        "private_key_id":  os.getenv(f"{_DB_PREFIX}FIREBASE_PRIVATE_KEY_ID"),
        "private_key":     os.getenv(f"{_DB_PREFIX}FIREBASE_PRIVATE_KEY", "").replace("\\n", "\n"),
        "client_email":    os.getenv(f"{_DB_PREFIX}FIREBASE_CLIENT_EMAIL"),
        "token_uri":       "https://oauth2.googleapis.com/token",
    }
    if not firebase_admin._apps:
        firebase_admin.initialize_app(credentials.Certificate(creds_dict))
    return firestore.client(database_id="default")


def read_csv(path: str) -> dict[str, dict]:
    """Read CSV, return dict keyed by propertyId. Only include non-empty updatable fields."""
    updates = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = row.get("propertyId", "").strip()
            if not pid:
                continue
            fields = {
                k: row[k].strip()
                for k in UPDATABLE_FIELDS
                if k in row and row[k].strip()
            }
            if fields:
                updates[pid] = fields
    return updates


def update_properties(db, updates: dict[str, dict], dry_run: bool):
    col = db.collection(COLLECTION)
    total = len(updates)
    written = 0
    t0 = time.time()

    logger.info(f"CSV rows to process: {total} | dry_run={dry_run}")

    items = list(updates.items())
    for chunk_start in range(0, total, BATCH_SIZE):
        chunk = items[chunk_start:chunk_start + BATCH_SIZE]

        if dry_run:
            for pid, fields in chunk:
                logger.info(f"[DRY-RUN] {pid} → {fields}")
            written += len(chunk)
        else:
            batch = db.batch()
            for pid, fields in chunk:
                batch.update(col.document(pid), fields)
            batch.commit()
            written += len(chunk)
            logger.info(f"  Flushed batch — {written}/{total} written")

    elapsed = time.time() - t0
    logger.info(f"Done in {elapsed:.2f}s | written={written if not dry_run else 0}")


def main():
    csv_path = args.input.strip() or input("Enter CSV path: ").strip().strip("'\"")
    args.input = csv_path

    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Input CSV not found: {args.input}")

    logger.info(f"DB: {args.db} | Collection: {COLLECTION} | Input: {args.input}")
    updates = read_csv(args.input)
    logger.info(f"Loaded {len(updates)} unique propertyId(s) from CSV")

    if not updates:
        logger.warning("No updatable rows found in CSV. Exiting.")
        return

    db = _init_firebase()
    update_properties(db, updates, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
