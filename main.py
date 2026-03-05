# main.py

import os
import time
import logging
import requests
import json
from typing import Dict, Any

from pymongo import MongoClient
from dotenv import load_dotenv

from pipeline import Pipeline


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


PUBCHEM_URL = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/{cid}/JSON"


def load_cids(file_path: str) -> list:
    """
    Load CID list from file.
    """
    cids = []
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    cids.append(int(line))
                except ValueError:
                    logger.warning(f"Invalid CID skipped: {line}")
    return cids


def fetch_pubchem_record(cid: int) -> Dict[str, Any]:
    """
    Fetch PubChem PUG View JSON for a given CID.
    """
    url = PUBCHEM_URL.format(cid=cid)

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Failed to fetch CID {cid}: {e}")
        return None


def fetch_all_records(cids: list) -> Dict[int, Dict[str, Any]]:
    """
    Fetch PubChem records for all CIDs with rate limiting.
    """
    records = {}

    for cid in cids:
        data = fetch_pubchem_record(cid)

        if data:
            records[cid] = data

        time.sleep(0.5)  # rate limiting

    return records


def main():
    """
    Main execution script.
    """
    load_dotenv()

    GEMINI_API_KEY = str(os.getenv("GEMINI_API_KEY"))
    # MONGO_URI = str(os.getenv("MONGO_URI"))
    # PUBCHEM_DB = str(os.getenv("PUBCHEM_DB"))
    # PUBCHEM_COLLECTION = str(os.getenv("PUBCHEM_COLLECTION"))
    MONGO_URI = "mongodb://localhost:27017"
    PUBCHEM_DB = "scimplifyDB"
    PUBCHEM_COLLECTION = "chemical_product"

    if not all([GEMINI_API_KEY, MONGO_URI, PUBCHEM_DB, PUBCHEM_COLLECTION]):
        raise EnvironmentError("Missing required environment variables")

    client = MongoClient(MONGO_URI)
    db = client[PUBCHEM_DB]
    collection = db[PUBCHEM_COLLECTION]

    logger.info("MongoDB connected")

    pipeline = Pipeline(GEMINI_API_KEY)

    cids = load_cids("cids.txt")

    logger.info(f"Loaded {len(cids)} CIDs")

    raw_records = fetch_all_records(cids)

    logger.info(f"Fetched {len(raw_records)} PubChem records")

    processed_records = pipeline.process_batch(raw_records)

    logger.info(f"Processed {len(processed_records)} records")

    # for record in processed_records:
    #     cid = record.get("cid")

    #     try:
    #         collection.insert_one(
    #             record
    #         )
    #         logger.info(f"Upsert success for CID {cid}")

    #     except Exception as e:
    #         logger.error(f"MongoDB upsert failed for CID {cid}: {e}")
    output_file = "result.txt"

    for record in processed_records:
        cid = record.get("cid")

        try:
            with open(output_file, "a", encoding="utf-8") as f:
                json.dump(record, f)
                f.write("\n")  # newline so each record is on a new line

            logger.info(f"Write success for CID {cid}")

        except Exception as e:
            logger.error(f"File write failed for CID {cid}: {e}")



if __name__ == "__main__":
    main()