import json
import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def normalize_amount(val):
    try:
        if val is None or val == "":
            return None

        if isinstance(val, str):
            val = val.replace("$", "").strip()
            if "." not in val:
                return float(val) / 100
            return float(val)

        if isinstance(val, (int, float)):
            return float(val) / 100

    except Exception:
        return None

    return None


def process_data():

    # 1️ MongoDB Connection
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        raise ValueError("MONGO_URI not found in .env file.")

    client = MongoClient(mongo_uri)
    db = client.quickcart_archive
    collection = db.raw_logs

    # Create indexes on nested fields
    collection.create_index("event.id")
    collection.create_index("entity.payment.id")

    archived_count = 0
    cleaned_count = 0
    clean_data = []

    file_path = "quickcart_data/raw_data.jsonl"

    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found.")
        return

    # 2️ Read JSONL
    with open(file_path, "r") as f:
        for line in f:
            if not line.strip():
                continue

            record = json.loads(line)

            # Archive raw log
            collection.insert_one(record)
            archived_count += 1

            # 3️ Extract Fields
            event = record.get("event", {})
            entity = record.get("entity", {})
            payload = record.get("payload", {})

            event_id = event.get("id")
            timestamp = event.get("ts")

            payment_id = entity.get("payment", {}).get("id")
            amount_raw = payload.get("Amount")
            status = payload.get("status")

            # 4️ Business Rule Filtering
            if status != "SUCCESS":
                continue

            amount_usd = normalize_amount(amount_raw)

            if not payment_id or amount_usd is None:
                continue

            is_test_flag = payload.get("flags") == "sandbox"
            is_test_metadata = payload.get("metadata", {}).get("is_test", False)
            
            if is_test_flag or is_test_metadata:
                continue 

            if status != "SUCCESS":
                continue


            # 5️ Clean Output Structure
            clean_data.append({
                "transaction_id": payment_id,
                "event_id": event_id,
                "amount_usd": amount_usd,
                "timestamp": timestamp
            })

            cleaned_count += 1

    # 6️ Export CSV
    if clean_data:
        df = pd.DataFrame(clean_data)
        df.to_csv("clean_transactions.csv", index=False)
    else:
        print("No valid records found.")

    print("====================================")
    print("PROCESS COMPLETE")
    print(f"Total archived: {archived_count}")
    print(f"Total cleaned: {cleaned_count}")
    print("Exported: clean_transactions.csv")
    print("====================================")


if __name__ == "__main__":
    process_data()
