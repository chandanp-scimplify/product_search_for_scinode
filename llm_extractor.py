# llm_extractor.py

import json
from typing import Dict, Any, Optional

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage


class LLMExtractor:
    """
    Uses Google Gemini (gemini-1.5-flash) via LangChain to extract
    structured chemical data from PubChem section text into JSON schema.
    """

    def __init__(self, api_key: str):
        """
        Initialize Gemini client using LangChain.
        """
        if not api_key:
            raise ValueError("API key must be provided")

        self.model = ChatGoogleGenerativeAI(
            model="gemini-3.1-pro-preview",
            google_api_key=api_key,
            temperature=0,
        )

    def extract(
        self,
        section_name: str,
        section_text: str,
        schema: Dict[str, Any],
        literals: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Extract structured data for a given section using Gemini.

        Parameters:
        - section_name: Name of the PubChem section.
        - section_text: Raw extracted text from parser.
        - schema: Target JSON schema (dict) to be filled.
        - literals: Optional dict of allowed literal values.

        Returns:
        - Dict representing the filled schema section.
        """

        if not isinstance(schema, dict):
            raise TypeError("schema must be a dictionary")

        literals_str = json.dumps(literals, indent=2) if literals else "None"

        prompt = f"""
You are a chemical data extraction expert.

Below is raw text extracted from PubChem for a chemical compound, specifically from the section: {section_name}

RAW TEXT:
{section_text}

Your task is to extract and fill ONLY the following JSON schema fields:
{json.dumps(schema, indent=2)}

Rules:
- Return ONLY valid JSON
- No markdown
- No explanation
- If value not found → null
- Boolean fields → true or false
- Arrays → [] if empty
- Allowed literals → ONLY use values from this list: {literals_str}
- Do not hallucinate information

Return valid JSON only.
""".strip()

        response = self.model.invoke([HumanMessage(content=prompt)])

        raw_output = response.content.strip()

        # Sometimes LLM wraps JSON in ```json ... ```
        if raw_output.startswith("```"):
            raw_output = raw_output.strip("`")
            raw_output = raw_output.replace("json", "", 1).strip()

        try:
            parsed_output = json.loads(raw_output)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Invalid JSON returned by Gemini for section '{section_name}': {raw_output}"
            ) from e

        if not isinstance(parsed_output, dict):
            raise ValueError(
                f"Expected JSON object for section '{section_name}', got: {type(parsed_output)}"
            )

        return parsed_output