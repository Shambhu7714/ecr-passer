import pandas as pd
import json
import os
from typing import Dict, List, Tuple

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
        print(f"[INFO] Loading mapping file: {self.mapping_file}")
        
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
        # Simple mapping (header -> series code)
            self.mapping_data = data
        
        print(f"[OK] JSON mapping loaded with {len(self.mapping_data)} definitions.")
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
        
        df_full = pd.read_excel(self.mapping_file, sheet_name=sheet_name, header=None)
        
        # Find header row
        header_row_idx = 0
        for idx, row in df_full.iterrows():
            row_str = ' '.join([str(x) for x in row.values if pd.notna(x)])
            if 'Update Name' in row_str or 'SERIES CODE' in row_str:
                header_row_idx = idx
                break
        
        # Re-read with correct header
        df = pd.read_excel(self.mapping_file, sheet_name=sheet_name, header=header_row_idx)
        
        # Parse the comprehensive mapping structure
        self._parse_comprehensive_mapping(df)
        
        print(f"[OK] Excel mapping loaded with {len(self.mapping_data)} series definitions.")
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
            print(f"[WARN] Warning: Missing mapped columns in input: {missing[:5]}...")  # Show first 5
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
    
    def _normalize_filename(self, name: str) -> str:
        """Normalize a filename for flexible matching: lowercase + collapse all spaces."""
        if not name: return ""
        import re
        # Remove extension for comparison
        name = os.path.splitext(name)[0]
        return re.sub(r'\s+', '', name.lower().strip())

    def _is_filename_match(self, file1: str, file2: str) -> bool:
        """Robust but strict filename matching."""
        if not file1 or not file2: return False
        
        n1 = self._normalize_filename(file1)
        n2 = self._normalize_filename(file2)
        
        # 1. Exact match after normalization
        if n1 == n2: return True
        
        # 2. Fuzzy match: Check if core parts match
        import re
        parts1 = set(re.findall(r'[a-z0-9]+', n1))
        parts2 = set(re.findall(r'[a-z0-9]+', n2))
        
        if not parts1 or not parts2: return False
        
        # Remove common small words/extensions that might cause false positives
        noise = {'xlsx', 'xls', 'xlsm', 'anex', 'anexo', 'cuadros', 'total', 'series', 'data', 'cuadro', 'anexos', 'enlace'}
        p1_sig = parts1 - noise
        p2_sig = parts2 - noise
        
        if not p1_sig or not p2_sig:
            common = parts1.intersection(parts2)
            ratio = len(common) / max(len(parts1), len(parts2)) if parts1 or parts2 else 0
            return ratio > 0.8
        
        common_sig = p1_sig.intersection(p2_sig)
        
        # 3. Subset check: If all significant parts of one are in the other
        # This handles 'anex-EMMET...' vs 'anex-EMMET..._2026-02-19...'
        if len(p1_sig) > 0 and p1_sig.issubset(p2_sig):
            return True
        if len(p2_sig) > 0 and p2_sig.issubset(p1_sig):
            return True
            
        # 4. High overlap check
        ratio = len(common_sig) / max(len(p1_sig), len(p2_sig))
        return ratio >= 0.6

    def get_mappings_for_file(self, filename):
        """Get all mapping metadata entries for a specific source file.

        Uses space-insensitive matching so 'anex-emmet -totalnacional-nov2025.xlsx'
        (with a stray space in the mapping) matches the real file
        'anex-emmet-totalnacional-nov2025.xlsx'.

        Args:
            filename: Name of the source file (can be full path or just basename)

        Returns:
            Dictionary of filtered metadata for series matching this source file
        """
        import os
        basename_norm = self._normalize_filename(os.path.basename(filename))

        filtered_metadata = {}
        for series_name, meta in self.metadata.items():
            source_file = str(meta.get('source_file', ''))
            if self._is_filename_match(filename, source_file):
                filtered_metadata[series_name] = meta

        self.logger.info(
            f"[INFO] Filtered {len(filtered_metadata)} mappings for file: "
            f"{os.path.basename(filename)}"
        )
        return filtered_metadata
    
    def get_tabs_for_file(self, filename):
        """Get list of TAB names to extract from the source file.

        Uses space-insensitive matching (same as get_mappings_for_file).

        Args:
            filename: Name of the source file

        Returns:
            List of unique tab/sheet names to process
        """
        import os
        basename_norm = self._normalize_filename(os.path.basename(filename))

        tabs = set()
        for series_name, meta in self.metadata.items():
            source_file = str(meta.get('source_file', ''))
            if self._is_filename_match(filename, source_file):
                tab = str(meta.get('tab', '')).strip()
                if tab and tab.lower() not in ('', 'nan', 'none'):
                    tabs.add(tab)

        self.logger.info(
            f"[INFO] Found {len(tabs)} TAB(s) for file '{os.path.basename(filename)}': {sorted(tabs)}"
        )
        return sorted(list(tabs))

    def validate_mapping_vs_source(self, input_file: str) -> Tuple[bool, Dict]:
        """
        Problem 2 Fix: Cross-check mapping TAB names and SOURCE FILE values
        against the actual input Excel file.

        Returns:
            (is_valid, report) where report contains:
              - matched_tabs: list of TAB names found in both mapping and file
              - missing_tabs: TAB names in mapping but NOT in file
              - extra_sheets: sheets in file but NOT referenced in mapping
              - source_file_match: whether the SOURCE FILE column matches input_file
              - mapping_entries_affected: count of mapping rows referencing missing tabs
        """
        import pandas as pd

        report = {
            "input_file": input_file,
            "matched_tabs": [],
            "missing_tabs": [],
            "extra_sheets": [],
            "source_file_match": False,
            "mapping_entries_affected": 0,
            "warnings": [],
            "errors": []
        }

        # --- 1. Load actual sheet names from input file ---
        try:
            xl = pd.ExcelFile(input_file)
            actual_sheets = set(s.strip() for s in xl.sheet_names)
        except Exception as e:
            report["errors"].append(f"Cannot open input file: {e}")
            self.logger.error(f"[ERR] Mapping validation: Cannot open input file '{input_file}': {e}")
            return False, report

        # --- 2. Collect TAB names referenced in mapping ---
        mapping_tabs = set()
        for series_name, meta in self.metadata.items():
            tab = str(meta.get("tab", "")).strip()
            if tab and tab.lower() not in ("", "nan", "none"):
                mapping_tabs.add(tab)

        # --- 3. Cross-check ---
        for tab in sorted(mapping_tabs):
            # Try exact match first
            if tab in actual_sheets:
                report["matched_tabs"].append(tab)
            else:
                # Try case-insensitive / stripped match
                tab_lower = tab.lower()
                fuzzy_match = next(
                    (s for s in actual_sheets if s.strip().lower() == tab_lower), None
                )
                if fuzzy_match:
                    report["matched_tabs"].append(f"{tab}   {fuzzy_match} (fuzzy)")
                else:
                    report["missing_tabs"].append(tab)
                    # Count how many mapping entries are affected
                    affected = sum(
                        1 for m in self.metadata.values()
                        if str(m.get("tab", "")).strip() == tab
                    )
                    report["mapping_entries_affected"] += affected
                    report["warnings"].append(
                        f"TAB '{tab}' referenced in mapping ({affected} entries) "
                        f"but NOT found in '{os.path.basename(input_file)}'"
                    )

        # Sheets in file but not in mapping
        matched_tab_names = set()
        for t in report["matched_tabs"]:
            matched_tab_names.add(t.split("   ")[0].strip())
        report["extra_sheets"] = sorted(
            s for s in actual_sheets if s not in matched_tab_names
        )

        # --- 4. Check SOURCE FILE column matches input filename ---
        basename = os.path.basename(input_file).lower()
        source_files_in_mapping = set()
        for meta in self.metadata.values():
            sf = str(meta.get("source_file", "")).strip().lower()
            if sf and sf not in ("", "nan", "none"):
                source_files_in_mapping.add(sf)

        if any(self._is_filename_match(basename, sf) for sf in source_files_in_mapping):
            report["source_file_match"] = True
        else:
            report["source_file_match"] = False
            report["warnings"].append(
                f"Input file '{basename}' does NOT match any SOURCE FILE entry in mapping. "
                f"Mapping SOURCE FILE values: {sorted(source_files_in_mapping)[:5]}"
            )

        # --- 5. Log structured report ---
        self.logger.info("=" * 60)
        self.logger.info("[INFO] MAPPING vs SOURCE FILE VALIDATION REPORT")
        self.logger.info("=" * 60)
        self.logger.info(f"  Input File     : {input_file}")
        self.logger.info(f"  Actual Sheets  : {sorted(actual_sheets)}")
        self.logger.info(f"  Mapping TABs   : {sorted(mapping_tabs)}")
        self.logger.info(f"  [OK] Matched TABs : {len(report['matched_tabs'])}")
        self.logger.info(f"  [ERR] Missing TABs : {len(report['missing_tabs'])}   {report['missing_tabs']}")
        self.logger.info(f"  [INFO]  Extra Sheets : {report['extra_sheets']}")
        self.logger.info(f"  SOURCE FILE match: {'[OK] Yes' if report['source_file_match'] else '[ERR] No'}")
        self.logger.info(f"  Affected entries: {report['mapping_entries_affected']}")

        for w in report["warnings"]:
            self.logger.warning(f"  [WARN]  {w}")

        is_valid = len(report["missing_tabs"]) == 0 and report["source_file_match"]
        self.logger.info(f"  Overall: {'[OK] VALID' if is_valid else '[WARN]  MISMATCH DETECTED'}")
        self.logger.info("=" * 60)

        return is_valid, report