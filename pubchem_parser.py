# pubchem_parser.py

from typing import Dict, List, Any


class PubChemParser:
    """
    Parser for PubChem PUG View JSON response.

    Responsibilities:
    - Traverse nested Section structure
    - Extract text from Information → Value → StringWithMarkup → String
    - Provide section-wise extracted text
    """

    TARGET_HEADINGS = [
        "Names and Identifiers",
        "Chemical and Physical Properties",
        "Safety and Hazards",
        "Toxicity",
        "Pharmacology and Biochemistry",
        "Use and Manufacturing",
        "Drug and Medication Information",
        "Regulatory Information",
        "Classification",
    ]

    def __init__(self, record: dict):
        """
        Initialize parser with full PubChem JSON response for one CID.

        Parameters:
        - record (dict): Full JSON response (must contain top-level "Record")
        """
        if not isinstance(record, dict):
            raise TypeError("record must be a dictionary")

        self.record = record.get("Record", {})
        self.sections = self.record.get("Section", [])

    def _extract_text_from_information(self, information_list: List[dict]) -> List[str]:
        """
        Extracts all string values from:
        Information → Value → StringWithMarkup → String
        """
        collected_strings = []

        for info in information_list or []:
            value = info.get("Value", {})

            # Extract StringWithMarkup
            string_with_markup = value.get("StringWithMarkup", [])
            for entry in string_with_markup:
                string_value = entry.get("String")
                if string_value:
                    collected_strings.append(str(string_value).strip())

            # Extract direct String (if present)
            direct_string = value.get("String")
            if direct_string:
                collected_strings.append(str(direct_string).strip())

            # Extract Number (if present)
            number_value = value.get("Number")
            if number_value is not None:
                collected_strings.append(str(number_value))

        return collected_strings

    def _collect_section_text(self, section: dict) -> List[str]:
        """
        Recursively collects text from a section and all nested subsections.
        """
        collected = []

        # Extract text from current section
        information = section.get("Information", [])
        collected.extend(self._extract_text_from_information(information))

        # Recursively process nested sections
        for subsection in section.get("Section", []) or []:
            collected.extend(self._collect_section_text(subsection))

        return collected

    def _find_sections_by_heading(self, sections: List[dict], heading: str) -> List[dict]:
        """
        Recursively finds all sections matching a given TOCHeading.
        """
        matched = []

        for section in sections or []:
            if section.get("TOCHeading") == heading:
                matched.append(section)

            # Search nested sections
            nested = section.get("Section", [])
            matched.extend(self._find_sections_by_heading(nested, heading))

        return matched

    def get_section_text(self, heading: str) -> str:
        """
        Recursively searches all nested sections for a matching TOCHeading
        and concatenates all extracted text into a single string.
        """
        matched_sections = self._find_sections_by_heading(self.sections, heading)

        collected_text = []
        for section in matched_sections:
            collected_text.extend(self._collect_section_text(section))

        # Remove empty strings and join cleanly
        cleaned = [text for text in collected_text if text]
        return "\n".join(cleaned).strip()

    def extract_all_sections(self) -> Dict[str, str]:
        """
        Extracts predefined top-level sections and returns:
        {
            heading: extracted_text
        }
        """
        extracted = {}

        for heading in self.TARGET_HEADINGS:
            extracted[heading] = self.get_section_text(heading)

        return extracted