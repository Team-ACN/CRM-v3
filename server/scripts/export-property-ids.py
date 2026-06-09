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

parser = argparse.ArgumentParser()
parser.add_argument('--db', choices=['new', 'old'], default='new')
parser.add_argument('--output', default='', help='Output CSV path (default: auto-named in <root>/data/)')
args = parser.parse_args()

_DB_PREFIX = "NEW_" if args.db == 'new' else ""
COLLECTION  = "Properties" if args.db == 'new' else "acnTestProperties"
PAGE_SIZE   = 1000

ROOT_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR    = os.path.join(ROOT_DIR, "data")


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


def fetch_rows(db) -> list[dict]:
    col = db.collection(COLLECTION)
    rows = []
    last_doc = None
    count = 0
    t0 = time.time()

    while True:
        q = col.order_by("__name__").limit(PAGE_SIZE)
        if last_doc is not None:
            q = q.start_after(last_doc)

        page = list(q.stream())
        if not page:
            break

        for doc in page:
            item = doc.to_dict() or {}
            rows.append({
                "propertyId":    item.get("propertyId", ""),
                "propertyName":  item.get("propertyName", ""),
                "projectId":     item.get("projectId", ""),
                "listingType":   item.get("listingType", ""),
                "societyType":   item.get("societyType", ""),
                "status":        item.get("status", ""),
                "kamName":       item.get("kamName", ""),
            })
            count += 1

        last_doc = page[-1]
        if count % 5000 == 0:
            logger.info(f"  {count} docs fetched ({count / (time.time() - t0):.0f}/s)")

        if len(page) < PAGE_SIZE:
            break

    logger.info(f"Done: {count} docs in {time.time() - t0:.2f}s")
    return rows


def write_csv(rows: list[dict], path: str):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["propertyId", "propertyName", "projectId", "listingType", "societyType", "status", "kamName"])
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Wrote {len(rows)} rows → {path}")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    output_path = args.output or os.path.join(DATA_DIR, f"property-ids-{args.db}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv")

    logger.info(f"DB: {args.db} | Collection: {COLLECTION}")
    db   = _init_firebase()
    rows = fetch_rows(db)
    write_csv(rows, output_path)
    print(output_path)


if __name__ == "__main__":
    main()
