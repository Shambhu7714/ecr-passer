import pandas as pd
import json
import os

class MapLoader:
    """
    Loads comprehensive mapping configuration with metadata.
    Supports both simple JSON mappings and complex Excel-based configurations.
    """
    def __init__(self, mapping_file):
        from core.logger import get_logger
        self.logger = get_logger()
        self.mapping_file = mapping_file
        self.mapping_data = {}
        self.metadata = {}
        self.validation_rules = {}
        
    def load(self):
        """Loads the mapping configuration."""
        print(f"📂 Loading mapping file: {self.mapping_file}")
        
        if self.mapping_file.endswith('.json'):
            return self._load_json()
        elif self.mapping_file.endswith('.xlsx') or self.mapping_file.endswith('.xls') or self.mapping_file.endswith('.xlsm'):
            return self._load_excel()
        else:
            raise ValueError("Unsupported mapping file format. Use JSON or Excel.")
    
    def _load_json(self):
        """Load JSON mapping with nested metadata structure."""
        with open(self.mapping_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Check if it's simple mapping or nested structure
        if data and isinstance(list(data.values())[0], dict):
            # Nested structure with metadata
            for series_name, series_info in data.items():
                # Extract series code
                series_code = series_info.get('source_id', series_name)
                self.mapping_data[series_name] = series_code
                
                # Store metadata
                self.metadata[series_name] = series_info
                
                # Store validation rules
                self.validation_rules[series_name] = {
                    'threshold': series_info.get('threshold', ''),
                    'no_change_action': series_info.get('no_change_action', 'Stop'),
                    'gaps_action': series_info.get('gaps_action', 'Alert'),
                }
        else:
            # Simple mapping (header -> series code)
            self.mapping_data = data
        
        print(f"✅ JSON mapping loaded with {len(self.mapping_data)} definitions.")
        return self.mapping_data
    
    def _load_excel(self):
        """
        Load comprehensive Excel mapping with multiple sheets.
        Expected sheets: 'Mapping Rules + Checks', 'Definitions', etc.
        """
        xl_file = pd.ExcelFile(self.mapping_file)
        
        # Try to load the main mapping sheet
        sheet_name = self._find_mapping_sheet(xl_file.sheet_names)
        
        if not sheet_name:
            raise ValueError("Could not find mapping sheet in Excel file.")
        
        df = pd.read_excel(self.mapping_file, sheet_name=sheet_name)
        
        # Parse the comprehensive mapping structure
        self._parse_comprehensive_mapping(df)
        
        print(f"✅ Excel mapping loaded with {len(self.mapping_data)} series definitions.")
        return self.mapping_data
    
    def _find_mapping_sheet(self, sheet_names):
        """Find the sheet containing mapping rules."""
        priority_names = ['Mapping Rules + Checks', 'Sheet1', 'Definitions', 'Mapping']
        for name in priority_names:
            if name in sheet_names:
                return name
        return sheet_names[0] if sheet_names else None
    
    def _parse_comprehensive_mapping(self, df):
        """
        Parse the comprehensive mapping structure from Excel.
        Extracts:
        - Series mappings (Update Name -> Series Code)
        - Metadata (Region, Country, Table Name, etc.)
        - Validation rules (Thresholds, Gaps, Duplicates)
        """
        for idx, row in df.iterrows():
            # Extract series identifier
            update_name = row.get('Update Name', '')
            series_code = row.get('SERIES CODE', '')
            primary_concept = row.get('PRIMARY CONCEPT', '')
            
            # Create a unique identifier for this row
            # If Update Name is the same for multiple rows, use PRIMARY CONCEPT to differentiate
            if pd.isna(update_name) or update_name == '':
                if pd.isna(primary_concept) or primary_concept == '':
                    continue
                else:
                    identifier = str(primary_concept)
            else:
                # If multiple rows have the same Update Name, append PRIMARY CONCEPT to make unique
                if pd.notna(primary_concept) and primary_concept != '':
                    identifier = f"{update_name}_{primary_concept}"
                else:
                    identifier = update_name
            
            # Build mapping entry
            self.mapping_data[identifier] = series_code if not pd.isna(series_code) else identifier
            
            # Extract metadata
            self.metadata[identifier] = {
                'update_name': update_name,  # Store original Update Name
                'region': row.get('Region', ''),
                'country': row.get('Country', ''),
                'table_name': row.get('Table Name', ''),
                'source_file': row.get('SOURCE FILE', ''),
                'tab': row.get('TAB', ''),
                'base_period': row.get('Base Period', ''),
                'unit_scale': row.get('Unit/Scale', ''),
                'classification': row.get('Classification', ''),
                'seasonality': row.get('Seasonality', 'NSA'),
                'primary_concept': row.get('PRIMARY CONCEPT', ''),
                'secondary_concept': row.get('SECONDARY CONCEPT', ''),
                'third_concept': row.get('THIRD CONCEPT', ''),
                'fourth_concept': row.get('FOURTH CONCEPT', ''),
                'source_id': row.get('Source ID', '') if not pd.isna(row.get('Source ID', '')) else series_code,
                'series_code': series_code if not pd.isna(series_code) else '',
                'factor': row.get('FACTOR', 1),
                'date_format': row.get('Date Format', ''),
            }
            
            # Extract validation rules
            self.validation_rules[identifier] = {
                'threshold': row.get('Threshold', ''),
                'future_value': row.get('Future Value', ''),
                'duplicate_threshold': row.get('Duplicate Value', ''),
                'no_change_action': row.get('No Change in Value for X Loaded Dates', 'Stop'),
                'gaps_action': row.get('Gaps Between Change in Start/End Dates', 'Alert'),
                'removal_threshold': row.get('Removal of Threshold File', ''),
                'previously_highest': row.get('Previously Highest Value', ''),
            }
    
    def validate(self, headers):
        """Validates if required columns from mapping exist in headers."""
        missing = [col for col in self.mapping_data.keys() if col not in headers]
        if missing:
            print(f"⚠️ Warning: Missing mapped columns in input: {missing[:5]}...")  # Show first 5
        return len(missing) == 0
    
    def get_metadata(self, series_name):
        """Get metadata for a specific series."""
        return self.metadata.get(series_name, {})
    
    def get_validation_rules(self, series_name):
        """Get validation rules for a specific series."""
        return self.validation_rules.get(series_name, {})
    
    def get_all_series(self):
        """Returns all series codes."""
        return list(self.mapping_data.values())
    
    def get_mappings_for_file(self, filename):
        """Get all mapping metadata entries for a specific source file.
        
        Args:
            filename: Name of the source file (can be full path or just basename)
        
        Returns:
            Dictionary of filtered metadata for series matching this source file
        """
        # Extract just the filename from path if full path provided
        import os
        basename = os.path.basename(filename).lower()
        
        # Filter metadata to only entries matching this source file
        filtered_metadata = {}
        for series_name, meta in self.metadata.items():
            source_file = str(meta.get('source_file', '')).lower().strip()
            
            # Match if source file contains the filename (flexible matching)
            if source_file and basename in source_file:
                filtered_metadata[series_name] = meta
        
        self.logger.info(f"📂 Filtered {len(filtered_metadata)} mappings for file: {basename}")
        return filtered_metadata
    
    def get_tabs_for_file(self, filename):
        """Get list of TAB names to extract from the source file.
        
        Args:
            filename: Name of the source file
        
        Returns:
            List of unique tab/sheet names to process
        """
        import os
        basename = os.path.basename(filename).lower()
        
        tabs = set()
        for series_name, meta in self.metadata.items():
            source_file = str(meta.get('source_file', '')).lower().strip()
            if source_file and basename in source_file:
                tab = meta.get('tab', '')
                if tab and tab != '':
                    tabs.add(tab)
        
        return sorted(list(tabs))