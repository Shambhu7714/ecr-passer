import json
import os
from typing import Dict, List, Optional, Any, Tuple
import google.generativeai as genai
from core.logger import get_logger

logger = get_logger()

class MappingAgent:
    """
    Intelligent Mapping Agent that uses LLM to match Excel row/column labels
    to series codes and concepts from the mapping configuration.
    
    This agent handles 'new patterns' by reasoning about semantics rather than
    relying solely on strict string matching.
    """
    
    def __init__(self, model_name: str = None):
        self.model_name = model_name or os.getenv("LLM_MODEL_NAME", "gemini-2.0-flash-exp")
        api_key = os.getenv("GEMINI_API_KEY")
        self.ai_available = False
        if api_key:
            try:
                genai.configure(api_key=api_key)
                self.model = genai.GenerativeModel(
                    model_name=self.model_name,
                    generation_config={
                        "temperature": 0,
                        "response_mime_type": "application/json"
                    }
                )
                self.ai_available = True
            except Exception as e:
                logger.warning(f"MappingAgent: LLM initialization failed: {e}")
        
    def reason_mappings(self, 
                       labels: List[str], 
                       mapping_metadata: Dict[str, Any], 
                       context: Dict[str, Any] = None,
                       country_filter: str = None) -> Dict[str, str]:
        """
        Reasons about which labels in the source file match which series codes
        in the mapping metadata.
        
        Args:
            labels: List of unique labels (row or column headers) found in the sheet.
            mapping_metadata: Dictionary of series metadata from the mapping file.
            context: Optional context (filename, sheet name, country, etc.)
            
        Returns:
            Dict mapping source label to series_code.
        """
        if not self.ai_available:
            logger.warning("MappingAgent: LLM not available, skipping intelligent mapping.")
            return {}

        # Apply country filter if provided
        if country_filter:
            cf_up = country_filter.upper()
            filtered_metadata = {}
            for k, v in mapping_metadata.items():
                c_val = str(v.get("country", "")).upper()
                scode = str(v.get("series_code", "")).upper()
                # Match if country matches, or code starts with country code,
                # or if the country filter is a substring of the full country name
                if c_val == cf_up or scode.startswith(cf_up) or \
                   (cf_up == 'CO' and c_val == 'COLOMBIA') or \
                   (cf_up == 'AR' and c_val == 'ARGENTINA'):
                    filtered_metadata[k] = v
            
            if filtered_metadata:
                mapping_metadata = filtered_metadata
                logger.info(f"[MappingAgent] Filtered to {len(mapping_metadata)} entries for country {country_filter}")

        logger.info(f"[MappingAgent] Reasoning about {len(labels)} labels against {len(mapping_metadata)} mapping entries")
        
        # Prepare mini-mapping descriptors for the LLM to save tokens
        mapping_descriptions = []
        for key, meta in mapping_metadata.items():
            desc = {
                "id": key,
                "code": meta.get("series_code"),
                "primary": meta.get("primary_concept"),
                "secondary": meta.get("secondary_concept"),
                "desc": meta.get("description", "")
            }
            mapping_descriptions.append(desc)

        prompt = f"""
        You are a Data Mapping Specialist. Your goal is to match raw row/column labels from an economic data Excel sheet to canonical series codes.
        
        CONTEXT:
        {json.dumps(context, indent=2) if context else "No additional context."}
        
        RAW LABELS FROM SHEET:
        {json.dumps(labels, indent=2)}
        
        AVAILABLE MAPPING TARGETS (SERIES):
        {json.dumps(mapping_descriptions, indent=2)}
        
        TASK:
        1. For each RAW LABEL, find the best matching MAPPING TARGET.
        2. Use semantic reasoning: "Unemployment Rate" matches "Unmp. Rate", "GDP growth" matches "PIB var", etc.
        3. Only return matches with high confidence.
        4. If a label matches multiple targets, choose the most specific one.
        
        RETURN FORMAT (JSON):
        {{
            "matches": {{
                "raw_label_1": "series_code_A",
                "raw_label_2": "series_code_B"
            }},
            "reasoning": {{
                "raw_label_1": "Reason for match...",
                "raw_label_2": "Reason for match..."
            }}
        }}
        """
        
        try:
            response = self.model.generate_content(prompt)
            result = json.loads(response.text)
            matches = result.get("matches", {})
            logger.info(f"[MappingAgent] Successfully reasoned {len(matches)} mappings via LLM.")
            return matches
        except Exception as e:
            logger.error(f"[MappingAgent] Error during LLM reasoning: {e}")
            return {}

    def analyze_conditions(self, sheet_names: List[str], mapping_entries: List[Dict]) -> Dict:
        """
        Analyzes mapping conditions (like sheet names or country codes) to determine
        which sheets should be processed and which mapping rules apply.
        """
        if not self.ai_available:
            return {}

        logger.info(f"[MappingAgent] Analyzing conditions for {len(sheet_names)} sheets")
        
        prompt = f"""
        You are an Orchestration Agent. You have a list of physical sheets in an Excel file and a list of logical mapping entries.
        
        PHYSICAL SHEETS:
        {json.dumps(sheet_names)}
        
        LOGICAL MAPPING ENTRIES (SAMPLES):
        {json.dumps(mapping_entries[:10], indent=2)}
        
        TASK:
        1. Identify which PHYSICAL SHEETS match which logical "TAB" or "CONDITION" in the mappings.
        2. Suggest a plan: "Process sheet X with mapping entries for Colombia", "Process sheet Y with mapping entries for Argentina", etc.
        
        RETURN FORMAT (JSON):
        {{
            "sheet_plan": {{
                "sheet_name": {{
                    "mapping_group": "group_id",
                    "reason": "..."
                }}
            }},
            "recommended_country": "CO" or "AR" or "BR", etc.
        }}
        """
        
        try:
            response = self.model.generate_content(prompt)
            return json.loads(response.text)
        except Exception:
            return {}
