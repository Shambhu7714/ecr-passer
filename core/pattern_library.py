import pandas as pd
import os

class PatternLibrary:
    """
    Loads and manages known data layout patterns.
    This helps the PatternMatcherAgent reuse known structures.
    """
    def __init__(self, pattern_file=None):
        self.pattern_file = pattern_file or "config/patterns.xlsx"
        self.patterns = []
        
    def load_patterns(self):
        """Load pattern definitions from Excel file."""
        # If no pattern file specified, skip loading
        if not self.pattern_file:
            print(f"[INFO] No pattern file specified. Skipping pattern library.")
            return []
        
        if not os.path.exists(self.pattern_file):
            print(f"[WARN] Pattern file not found: {self.pattern_file}. Skipping pattern library.")
            return []
        
        if not os.path.exists(self.pattern_file) or os.path.getsize(self.pattern_file) == 0:
            print(f"[WARN] Pattern file is empty: {self.pattern_file}. Skipping pattern library.")
            return []
            
        try:
            df = pd.read_excel(self.pattern_file, engine='openpyxl')
            
            # Expected columns: "New Pattern", "Dicription", "Sample Update Name"
            for _, row in df.iterrows():
                pattern = {
                    "id": row.get("New Pattern", ""),
                    "description": row.get("Dicription", ""),
                    "sample_name": row.get("Sample Update Name", ""),
                    "pattern_type": self._classify_pattern(row.get("Dicription", ""))
                }
                self.patterns.append(pattern)
                
            print(f"[INFO] Loaded {len(self.patterns)} patterns from library.")
        except Exception as e:
            print(f"[WARN] Could not load pattern file: {str(e)}. Continuing without patterns.")
        
        return self.patterns
    
    def _classify_pattern(self, description):
        """Classify pattern based on description keywords."""
        desc_lower = str(description).lower()
        
        if "vertical" in desc_lower or "multiple tables in vertical" in desc_lower:
            return "vertical_multi_table"
        elif "horizontal" in desc_lower or "multiple tables in horizontal" in desc_lower:
            return "horizontal_multi_table"
        elif "rows and columns have time series" in desc_lower:
            return "standard_time_series"
        elif "wider format" in desc_lower:
            return "wide_format"
        elif "edge case" in desc_lower or "both in rows" in desc_lower:
            return "edge_case_row_based"
        else:
            return "standard"
    
    def match_pattern(self, headers, sample_data):
        """
        Attempts to match input data structure to a known pattern.
        Returns best matching pattern or None.
        """
        # Simple heuristic matching (can be enhanced with LLM)
        num_cols = len(headers)
        
        # Check for wide format (many columns)
        if num_cols > 10:
            return self._find_pattern_by_type("wide_format")
        
        # Check for standard time series (few columns with date + values)
        if num_cols <= 5:
            return self._find_pattern_by_type("standard_time_series")
            
        return None
    
    def _find_pattern_by_type(self, pattern_type):
        """Find first pattern matching the given type."""
        for pattern in self.patterns:
            if pattern["pattern_type"] == pattern_type:
                return pattern
        return None
    
    def get_all_patterns(self):
        """Returns all loaded patterns."""
        return self.patterns
    
    def add_pattern(self, pattern_name, update_name, num_concepts, concepts_desc, 
                    index_col, time_series_cols, orientation):
        """
        Add a new pattern to the library.
        
        Args:
            pattern_name: File name or pattern identifier
            update_name: Update Name from mapping.xlsm
            num_concepts: Number of concept columns (e.g., 2, 3, 4)
            concepts_desc: Description of concepts (Primary, Secondary, Third, Fourth)
            index_col: Description of index/metadata column or None
            time_series_cols: Description of time series columns
            orientation: "row-wise" or "column-wise"
        """
        new_pattern = {
            "New Pattern": pattern_name,
            "Update Name": update_name,
            "Number of Concepts": num_concepts,
            "Concepts Description": concepts_desc,
            "Index Column": index_col if index_col else "None",
            "Time Series Columns": time_series_cols,
            "Orientation": orientation
        }
        
        # Load existing patterns
        existing_patterns = []
        if os.path.exists(self.pattern_file) and os.path.getsize(self.pattern_file) > 0:
            try:
                df = pd.read_excel(self.pattern_file, engine='openpyxl')
                existing_patterns = df.to_dict('records')
            except Exception as e:
                print(f"[WARN] Could not load existing patterns: {str(e)}")
        
        for pattern in existing_patterns:
            if pattern.get("New Pattern") == pattern_name:
                print(f"[INFO] Pattern '{pattern_name}' already exists. Skipping.")
                return False
        
        # Add new pattern
        existing_patterns.append(new_pattern)
        
        # Save to Excel
        df = pd.DataFrame(existing_patterns)
        df.to_excel(self.pattern_file, index=False, engine='openpyxl')
        print(f"[OK] Added pattern '{pattern_name}' to library.")
        print(f"   Update Name: {update_name}")
        print(f"   Concepts: {num_concepts}")
        return True