import os
import json
import google.generativeai as genai
from dotenv import load_dotenv
from core.pattern_library import PatternLibrary

load_dotenv()

class IntelligenceLayer:
    def __init__(self, pattern_file=None):
        # Configure to use Gemini via OpenAI-compatible endpoint if needed, 
        # Configure Gemini API
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY not found in environment variables")
        
        genai.configure(api_key=api_key)
        
        model_name = os.getenv("LLM_MODEL_NAME", "gemini-2.0-flash-exp")
        self.model = genai.GenerativeModel(
            model_name=model_name,
            generation_config={
                "temperature": 0,
                "response_mime_type": "application/json"
            }
        )
        
        # Load pattern library
        self.pattern_lib = PatternLibrary(pattern_file)
        self.pattern_lib.load_patterns()

    def _ask_llm(self, prompt, system_prompt="You are a helpful assistant."):
        full_prompt = f"{system_prompt}\n\n{prompt}"
        response = self.model.generate_content(full_prompt)
        return json.loads(response.text)
    
    def layout_detection(self, headers, sample_data):
        """LayoutDetectionAgent: Detects table structure, header depth, and orientation."""
        prompt = f"""
        Analyze the following table headers and sample data:
        Headers: {headers}
        Sample Data: {sample_data}
        
        Task:
        - Identify header depth (number of rows used for headers).
        - Detect orientation (row-wise or column-wise).
        - Identify time axis (which column/row contains dates/months).
        - Identify numeric series columns.
        
        Return JSON structure:
        {{
            "header_depth": int,
            "orientation": "row-wise" | "column-wise",
            "time_axis_index": int,
            "numeric_columns_indices": [int, ...],
            "confidence": float
        }}
        """
        print("🧠 Running LayoutDetectionAgent...")
        return self._ask_llm(prompt, "You are a layout analysis agent.")

    def pattern_matcher(self, layout, headers, sample_data, vector_mapper=None, source_file=None):
        """
        PatternMatcherAgent: Suggests reuse of known layouts.
        Uses the pattern library to find matching patterns.
        """
        print("🧠 Running PatternMatcherAgent...")
        # First, try vector-based pattern matching (ChromaDB) if available
        try:
            if vector_mapper is not None:
                # Build a lightweight file_structure for matching
                file_structure = {
                    'sheet_name': layout.get('sheet_name') if isinstance(layout, dict) else None,
                    'rows': len(sample_data),
                    'cols': len(headers),
                    'headers': headers[:10]
                }
                vmatch = vector_mapper.match_pattern(file_structure, source_file=source_file) if hasattr(vector_mapper, 'match_pattern') else None
                if vmatch:
                    print(f"✅ Matched Vector Pattern: {vmatch.get('pattern_name', vmatch.get('id', 'unknown'))} (confidence: {vmatch.get('confidence', 0):.2f})")
                    return {
                        'suggested_layout_id': vmatch.get('pattern_name', vmatch.get('id', 'vector_pattern')),
                        'pattern_type': vmatch.get('pattern_type', 'vector'),
                        'description': vmatch.get('description', ''),
                        'match_score': float(vmatch.get('confidence', 0))
                    }
        except Exception as e:
            print(f"⚠️ Vector pattern matching failed: {e}")

        # Fallback to PatternLibrary (Excel-based)
        matched_pattern = self.pattern_lib.match_pattern(headers, sample_data)

        if matched_pattern:
            print(f"✅ Matched Pattern {matched_pattern['id']}: {matched_pattern['description']}")
            return {
                "suggested_layout_id": matched_pattern['id'],
                "pattern_type": matched_pattern['pattern_type'],
                "description": matched_pattern['description'],
                "match_score": 0.85
            }
        else:
            print("⚠️ No matching pattern found. Using default layout.")
            return {
                "suggested_layout_id": "default",
                "pattern_type": "unknown",
                "match_score": 0.5
            }

    def label_normalizer(self, labels):
        """LabelNormalizerAgent: Normalizes synonyms (e.g., 'Jan' -> 'January')."""
        prompt = f"""
        Normalize the following time labels to a canonical format (YYYY-MM-DD or standard month names):
        Labels: {labels}
        
        Return JSON mapping:
        {{
            "original_label": "normalized_label",
            ...
        }}
        """
        print("🧠 Running LabelNormalizerAgent...")
        return self._ask_llm(prompt, "You are a data normalization agent.")