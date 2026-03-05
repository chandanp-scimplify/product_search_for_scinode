# validator.py

import logging
from typing import Dict, List, Any


class Validator:
    """
    Validator for enforcing controlled vocabulary (literals) in extracted schema data.
    """

    def __init__(self):
        """
        Initialize literal vocabularies and logger.
        """
        self.logger = logging.getLogger(__name__)

        self.literals = {
            "functional_roles": [
                "active_ingredient", "intermediate", "building_block", "reagent",
                "catalyst", "solvent", "co_solvent", "stabilizer", "preservative",
                "antioxidant", "plasticizer", "surfactant", "emulsifier", "dispersant",
                "binder", "thickening_agent", "colorant", "pigment", "fragrance_component",
                "flavoring_agent", "flame_retardant_additive", "chelating_agent",
                "crosslinking_agent", "curing_agent", "extraction_agent",
                "lubricant_additive"
            ],
            "industry_segments": [
                "api_manufacturing", "drug_formulation", "veterinary_products",
                "crop_protection", "herbicide_formulation", "pesticide_formulation",
                "food_additives", "nutraceuticals", "cosmetic_formulation",
                "skin_care_products", "hair_care_products", "fine_chemicals",
                "bulk_chemicals", "petrochemicals", "polymer_industry",
                "coatings_industry", "textile_industry", "mining_chemicals",
                "water_treatment", "paint_manufacturing"
            ],
            "application_tags": [
                "hplc_grade", "gc_grade", "pharma_grade", "food_grade",
                "cosmetic_grade", "technical_grade", "analytical_standard",
                "process_scale_synthesis", "lab_scale_synthesis",
                "controlled_release_formulation", "resin_formulation",
                "adhesive_formulation", "uv_curing_system",
                "thermal_stabilization", "antimicrobial_formulation",
                "ph_adjustment", "metal_cleaning", "flame_retardant_system",
                "plastic_compounding", "rubber_processing"
            ],
            "market_type": [
                "pharmaceutical", "agrochemical", "flavor_fragrance", "industrial",
                "personal_care", "food_nutrition", "dyes_pigments",
                "oleochemicals", "flame_retardants", "metallurgy"
            ]
        }

    def validate_literals(self, data: Dict[str, Any], field: str) -> List[str]:
        """
        Filters a list field to only keep values present in the allowed literals.
        Logs a warning for any removed value.
        """
        allowed = set(self.literals.get(field, []))
        values = data.get(field, [])

        if not isinstance(values, list):
            self.logger.warning(f"{field} expected list but got {type(values)}. Resetting to empty list.")
            return []

        cleaned = []
        for value in values:
            if value in allowed:
                cleaned.append(value)
            else:
                self.logger.warning(f"Removed invalid literal from {field}: {value}")

        return cleaned

    def validate_commercial(self, commercial: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validates commercial section fields.
        """
        if not isinstance(commercial, dict):
            return {}

        commercial["functional_roles"] = self.validate_literals(commercial, "functional_roles")
        commercial["industry_segments"] = self.validate_literals(commercial, "industry_segments")
        commercial["application_tags"] = self.validate_literals(commercial, "application_tags")

        market_type = commercial.get("market_type")

        if market_type not in self.literals["market_type"]:
            if market_type is not None:
                self.logger.warning(f"Invalid market_type removed: {market_type}")
            commercial["market_type"] = None

        return commercial

    def validate_full(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Runs validation on the full schema record.
        """
        if not isinstance(record, dict):
            raise TypeError("record must be a dictionary")

        commercial = record.get("commercial")

        if commercial:
            record["commercial"] = self.validate_commercial(commercial)

        return record