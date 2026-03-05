# pipeline.py

import logging
from typing import Dict, List, Any
import time

from pubchem_parser import PubChemParser
from llm_extractor import LLMExtractor
from validator import Validator


class Pipeline:
    """
    End-to-end pipeline for processing PubChem records and filling
    the MongoDB chemical schema using:
    - PubChemParser
    - LLMExtractor (Gemini)
    - Validator
    """

    IDENTITY_SCHEMA = {
        "iupac_name": None,
        "cas_number": None,
        "ec_number": None,
        "molecular_formula": None,
        "molecular_weight": None,
        "smiles": None,
        "inchi_key": None,
        "synonyms": [],
    }

    PHYSICOCHEMICAL_SCHEMA = {
        "melting_point_c": None,
        "boiling_point_c": None,
        "density_g_cm3": None,
        "flash_point_c": None,
        "vapor_pressure": None,
        "pka": None,
        "logp": None,
        "solubility": {
            "water": None,
            "organic_solvents": [],
            "ph_dependent": None,
        },
    }

    SAFETY_SCHEMA = {
        "ghs_classification": [],
        "hazard_statements": [],
        "un_number": None,
        "transport_class": None,
        "flammable": None,
        "toxic": None,
        "corrosive": None,
        "explosive": None,
        "environmental_hazard": None,
        "voc_category": None,
        "biodegradability": None,
    }

    REGULATORY_SCHEMA = {
        "reach_registered": None,
        "tsca_listed": None,
        "fda_status": None,
        "gmp_required": None,
        "pharmacopoeia_standards": [],
        "controlled_substance": None,
        "export_restrictions": [],
    }

    STABILITY_SCHEMA = {
        "temperature_sensitive": None,
        "cold_chain_required": None,
        "compatible_packaging_types": [],
        "incompatible_with": [],
        "decomposition_byproducts": [],
    }

    COMMERCIAL_SCHEMA = {
        "market_type": None,
        "functional_roles": [],
        "industry_segments": [],
        "application_tags": [],
        "substitutes": [],
        "function": None,
        "description": None,
    }

    def __init__(self, gemini_api_key: str):
        """
        Initialize pipeline components.
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        self.llm = LLMExtractor(gemini_api_key)
        self.validator = Validator()

        self.logger.info("Pipeline initialized successfully")

    def process_cid(self, cid: int, raw_record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single PubChem CID record.
        """
        self.logger.info(f"[CID {cid}] Starting processing")

        start_total = time.time()

        # ---------------- PARSER ----------------
        self.logger.info(f"[CID {cid}] Initializing PubChemParser")
        parser = PubChemParser(raw_record)

        self.logger.info(f"[CID {cid}] Extracting PubChem sections")
        sections = parser.extract_all_sections()
        self.logger.info(f"[CID {cid}] Section extraction complete")

        identity_text = sections.get("Names and Identifiers", "")
        physchem_text = sections.get("Chemical and Physical Properties", "")

        safety_text = "\n".join([
            sections.get("Safety and Hazards", ""),
            sections.get("Toxicity", "")
        ])

        regulatory_text = "\n".join([
            sections.get("Drug and Medication Information", ""),
            sections.get("Regulatory Information", "")
        ])

        stability_text = sections.get("Safety and Hazards", "")

        commercial_text = "\n".join([
            sections.get("Use and Manufacturing", ""),
            sections.get("Pharmacology and Biochemistry", "")
        ])

        # ---------------- LLM EXTRACTION ----------------

        self.logger.info(f"[CID {cid}] Extracting identity via LLM")
        t = time.time()
        identity = self.llm.extract(
            "Names and Identifiers",
            identity_text,
            self.IDENTITY_SCHEMA,
        )
        self.logger.info(f"[CID {cid}] Identity extracted ({time.time() - t:.2f}s)")

        self.logger.info(f"[CID {cid}] Extracting physicochemical properties via LLM")
        t = time.time()
        physicochemical = self.llm.extract(
            "Chemical and Physical Properties",
            physchem_text,
            self.PHYSICOCHEMICAL_SCHEMA,
        )
        self.logger.info(f"[CID {cid}] Physicochemical extracted ({time.time() - t:.2f}s)")

        self.logger.info(f"[CID {cid}] Extracting safety data via LLM")
        t = time.time()
        safety = self.llm.extract(
            "Safety and Hazards + Toxicity",
            safety_text,
            self.SAFETY_SCHEMA,
        )
        self.logger.info(f"[CID {cid}] Safety extracted ({time.time() - t:.2f}s)")

        self.logger.info(f"[CID {cid}] Extracting regulatory data via LLM")
        t = time.time()
        regulatory = self.llm.extract(
            "Drug and Medication Information + Regulatory Information",
            regulatory_text,
            self.REGULATORY_SCHEMA,
        )
        self.logger.info(f"[CID {cid}] Regulatory extracted ({time.time() - t:.2f}s)")

        self.logger.info(f"[CID {cid}] Extracting stability/logistics via LLM")
        t = time.time()
        stability = self.llm.extract(
            "Safety and Hazards",
            stability_text,
            self.STABILITY_SCHEMA,
        )
        self.logger.info(f"[CID {cid}] Stability extracted ({time.time() - t:.2f}s)")

        self.logger.info(f"[CID {cid}] Extracting commercial data via LLM")
        t = time.time()
        commercial = self.llm.extract(
            "Use and Manufacturing + Pharmacology and Biochemistry",
            commercial_text,
            self.COMMERCIAL_SCHEMA,
        )
        self.logger.info(f"[CID {cid}] Commercial extracted ({time.time() - t:.2f}s)")

        # ---------------- RECORD BUILD ----------------

        self.logger.info(f"[CID {cid}] Building final record")

        record = {
            "cid": cid,
            "identity": identity,
            "physicochemical_properties": physicochemical,
            "safety": safety,
            "regulatory": regulatory,
            "stability_logistics": stability,
            "commercial": commercial,
        }

        # ---------------- VALIDATION ----------------

        self.logger.info(f"[CID {cid}] Running validation")
        record = self.validator.validate_full(record)

        self.logger.info(f"[CID {cid}] Validation complete")

        total_time = time.time() - start_total
        self.logger.info(f"[CID {cid}] Processing finished in {total_time:.2f}s")

        return record

    def process_batch(self, cid_records: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process multiple CID records. Errors are logged per CID
        without stopping the batch.
        """
        self.logger.info(f"Starting batch processing for {len(cid_records)} CIDs")

        results = []

        for cid, raw_record in cid_records.items():
            try:
                processed = self.process_cid(cid, raw_record)
                results.append(processed)
                self.logger.info(f"[CID {cid}] Successfully processed")
            except Exception as e:
                self.logger.error(f"[CID {cid}] Failed: {e}", exc_info=True)

        self.logger.info(f"Batch processing complete. {len(results)} records processed successfully")

        return results