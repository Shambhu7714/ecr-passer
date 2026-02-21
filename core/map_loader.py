import pandas as pd
import os
import re
from core.logger import get_logger

class MapLoader:
    def __init__(self, mapping_file):
        self.mapping_file = mapping_file
        self.mapping_data = {}
        self.metadata = {}
        self.logger = get_logger()

    def load(self):
        """Loads mapping from Excel."""
        if not os.path.exists(self.mapping_file):
            self.logger.error(f"Mapping file not found: {self.mapping_file}")
            return {}
        
        xl = pd.ExcelFile(self.mapping_file)
        sheet_name = self._find_mapping_sheet(xl.sheet_names)
        
        # Read with header detection
        df_header = pd.read_excel(self.mapping_file, sheet_name=sheet_name, nrows=20, header=None)
        header_row_idx = 0
        for i, row in df_header.iterrows():
            if any(str(v).strip().upper() in ['UPDATE NAME', 'SERIES CODE', 'SERIESCODE', 'SOURCE FILE'] for v in row):
                header_row_idx = i
                break
        
        df = pd.read_excel(self.mapping_file, sheet_name=sheet_name, header=header_row_idx)
        self._parse_comprehensive_mapping(df)
        
        print(f"[OK] Excel mapping loaded with {len(self.mapping_data)} series definitions.")
        return self.mapping_data

    def _find_mapping_sheet(self, sheet_names):
        priority = ['Mapping Rules + Checks', 'Sheet1', 'Definitions', 'Mapping']
        for name in priority:
            if name in sheet_names: return name
        return sheet_names[0] if sheet_names else None

    def _parse_comprehensive_mapping(self, df):
        # Clean columns
        df.columns = [str(c).strip() for c in df.columns]
        
        for _, row in df.iterrows():
            update_name = str(row.get('Update Name', '')).strip()
            series_code = str(row.get('SERIES CODE', '')).strip()
            
            if not update_name and not series_code: continue
            
            # Key is series_code if available, otherwise synthetic
            identifier = series_code if series_code and series_code.lower() != 'nan' else f"{update_name}_{row.get('TAB', '')}"
            
            self.mapping_data[identifier] = series_code if series_code and series_code.lower() != 'nan' else identifier
            
            self.metadata[identifier] = {
                'series_code': series_code if series_code and series_code.lower() != 'nan' else identifier,
                'update_name': update_name,
                'country': row.get('Country', ''),
                'source_file': row.get('SOURCE FILE', ''),
                'tab': row.get('TAB', ''),
                'primary_concept': row.get('PRIMARY CONCEPT', ''),
                'secondary_concept': row.get('SECONDARY CONCEPT', ''),
                'third_concept': row.get('THIRD CONCEPT', ''),
                'factor': row.get('FACTOR', 1),
                'date_format': row.get('Date Format', '')
            }

    def get_mappings_for_file(self, filename):
        """Returns metadata for series defined for this file in mapping."""
        if not self.metadata:
            self.load()
            
        basename = os.path.basename(filename).lower()
        filtered = {}
        
        # Discriminators that should NOT be mixed
        discriminators = ['desestacionalizado', 'desestac', 'mls', 'original']
        found_discriminators = [d for d in discriminators if d in basename]
        
        for identifier, meta in self.metadata.items():
            source = str(meta.get('source_file', '')).lower()
            if not source or source == 'nan': continue
            
            # 1. Direct or fuzzy match
            match = self._is_filename_match(basename, source)
            
            # 2. Strict Discriminator Check
            # If input file has 'Desestacionalizado', don't match source that doesn't (and vice versa)
            if match:
                source_discriminators = [d for d in discriminators if d in source]
                if set(found_discriminators) != set(source_discriminators):
                    match = False
            
            if match:
                filtered[identifier] = meta
                
        self.logger.info(f"[INFO] Filtered {len(filtered)} mappings for file: '{basename}'")
        return filtered

    def get_tabs_for_file(self, filename):
        mappings = self.get_mappings_for_file(filename)
        tabs = set()
        for meta in mappings.values():
            tab = str(meta.get('tab', '')).strip()
            if tab and tab.lower() not in ('', 'nan', 'none'):
                tabs.add(tab)
        return list(tabs)

    def _is_filename_match(self, n1, n2):
        """Fuzzy filename matching (ignores extension/date)"""
        if not n1 or not n2: return False
        n1, n2 = n1.lower(), n2.lower()
        if n1 == n2: return True
        
        # Strip extensions
        n1 = os.path.splitext(n1)[0]
        n2 = os.path.splitext(n2)[0]
        
        # Token overlap - split on non-alphanumeric and also split letters from numbers (e.g. nov2025 -> nov, 2025)
        def get_tokens(s):
            tokens = re.findall(r'[a-z]+|[0-9]+', s)
            noise = {'xlsx', 'xls', 'xlsm', 'anex', 'anexo', 'cuadros', 'total', 'series', 'data', 'cuadro', 'anexos'}
            return {t for t in tokens if t not in noise and len(t) > 1}

        t1 = get_tokens(n1)
        t2 = get_tokens(n2)
        
        if not t1 or not t2: 
             # Fallback if tokens are too short or all noise
             return n1 in n2 or n2 in n1
        
        if t1.issubset(t2) or t2.issubset(t1):
            return True
            
        common = t1.intersection(t2)
        # Higher weight to shared tokens
        # Ignore numeric tokens in ratio if possible, or just lower threshold
        ratio = len(common) / max(len(t1), len(t2))
        return ratio >= 0.4 # More lenient for month/year changes