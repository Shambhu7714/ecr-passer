import pandas as pd
import re
import numpy as np

class AxisResolver:
    def resolve(self, df, layout):
        return "monthly"

class TimeNormalizer:
    def __init__(self):
        self.month_map_es = {
            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6,
            'julio': 7, 'agosto': 8, 'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12,
            'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6,
            'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12,
            'setiembre': 9, 'set': 9
        }
        self.month_map_en = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
            'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12,
            'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
            'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12
        }

    def parse_date(self, s):
        """Robust date parsing for headers or cells."""
        if pd.isna(s): return None
        s_orig = str(s).strip()
        s = s_orig.lower().replace('\n', ' ')
        if not s or s == 'nan' or s == 'none': return None
        
        # Quarter mapping for strings like "Enero - marzo"
        if '-' in s or 'trimestre' in s:
            if any(m in s for m in ['enero', 'marzo']): return "-03-01"
            if any(m in s for m in ['abril', 'junio']): return "-06-01"
            if any(m in s for m in ['julio', 'septiembre']): return "-09-01"
            if any(m in s for m in ['octubre', 'diciembre']): return "-12-01"

        # Pattern: "YYYY-MM-DD"
        if re.match(r'^\d{4}-\d{1,2}-\d{1,2}', s):
            return s[:10]
        
        # Split by non-alphanumeric (except space)
        parts = [p.strip() for p in re.split(r'[^a-z0-9]', s) if p.strip()]
        y, m = None, None
        for p in parts:
            if p.isdigit():
                val = int(p)
                if 1900 < val < 2100: 
                    y = val
                elif 10 <= val <= 99 and y is None: 
                    y = 2000 + val
                elif 1 <= val <= 12 and m is None:
                    m = val
            elif p in self.month_map_es:
                if m is None: m = self.month_map_es[p]
            elif p in self.month_map_en:
                if m is None: m = self.month_map_en[p]
        
        if y and m: return f"{y}-{m:02d}-01"
        if m: return f"-{m:02d}-01" # Return suffix for partial match
        if y: return f"{y}-01-01"
        return None

class DeterministicExtractor:
    def __init__(self):
        self.time_normalizer = TimeNormalizer()

    def extract_concept_based(self, df, mapping_metadata, base_year=2024, source_file=None, all_metadata=None, reasoned_mappings=None):
        # Header-based date detection (Pivot)
        date_headers = sum(1 for col in df.columns if self.time_normalizer.parse_date(col) and not str(self.time_normalizer.parse_date(col)).startswith("-"))
        
        if date_headers >= 2:
            return self._extract_pivot(df, mapping_metadata, base_year, reasoned_mappings)
        else:
            return self._extract_stacked(df, mapping_metadata, base_year, reasoned_mappings)

    def _extract_pivot(self, df, mapping_metadata, base_year, reasoned_mappings=None):
        date_cols = [col for col in df.columns if self.time_normalizer.parse_date(col)]
        output = {}
        
        for _, row in df.iterrows():
            row_val = str(row.iloc[0]).strip().lower() 
            if not row_val or row_val in ('nan', 'none'): continue
            
            for code_key, meta in self._iter_metadata(mapping_metadata):
                scode = meta.get('series_code', code_key)
                
                match = False
                prim = str(meta.get('primary_concept', '')).strip().lower()
                sec = str(meta.get('secondary_concept', '')).strip().lower()
                
                if sec and row_val == sec: match = True
                elif prim and row_val == prim: match = True
                elif prim and sec and (prim in row_val and sec in row_val): match = True
                
                if not match and reasoned_mappings:
                    if row_val in reasoned_mappings and reasoned_mappings[row_val] == scode:
                        match = True
                
                if match:
                    if scode not in output: output[scode] = {"values": {}}
                    for col in date_cols:
                        dkey = self.time_normalizer.parse_date(col)
                        val = row.get(col)
                        if pd.isna(val) or str(val).strip() in ('', '-'): continue
                        
                        clean_val = self._clean_value(val)
                        if clean_val is not None:
                            clean_dkey = f"{base_year}{dkey}" if dkey.startswith("-") else dkey
                            # Nacional data prioritized over Cabeceras
                            if clean_dkey not in output[scode]["values"]:
                                output[scode]["values"][clean_dkey] = clean_val
        return output

    def _extract_stacked(self, df, mapping_metadata, base_year, reasoned_mappings=None):
        YEAR_KWS = ['año', 'year', 'anos', 'ano', 'años']
        MONTH_KWS = ['mes', 'month', 'periodo', 'trimestre', 'meses']
        
        y_col, m_col = None, None
        best_y_score, best_m_score = -1, -1
        
        for col in df.columns:
            cl = str(col).lower()
            vals = df[col].dropna().astype(str).str.strip().str.lower()
            if vals.empty: continue
            
            # Count potential years and partial dates
            y_hits = sum(1 for v in vals if v.replace('.0','').isdigit() and 1990 < int(float(v)) < 2100)
            m_hits = sum(1 for v in vals if self.time_normalizer.parse_date(v) is not None)
            
            if any(kw in cl for kw in YEAR_KWS): y_hits += 50
            if any(kw in cl for kw in MONTH_KWS): m_hits += 50
            
            if y_hits > best_y_score:
                best_y_score = y_hits
                y_col = col
            if m_hits > best_m_score:
                best_m_score = m_hits
                m_col = col

        if not y_col and not m_col: return {}
        
        val_cols = [c for c in df.columns if c not in [y_col, m_col]]
        col_to_scode = self._match_cols_to_series(val_cols, mapping_metadata, reasoned_mappings)
        
        output = {}
        last_y = None
        for _, row in df.iterrows():
            y_raw = row.get(y_col) if y_col else None
            m_raw = row.get(m_col) if m_col else None
            
            if pd.notna(y_raw): 
                try: 
                    val = float(str(y_raw).replace(',',''))
                    if 1990 < val < 2100: last_y = int(val)
                except: pass
            
            if not last_y or pd.isna(m_raw): continue
            
            norm_m = self.time_normalizer.parse_date(m_raw)
            if not norm_m: continue
            
            date_key = f"{last_y}{norm_m}" if norm_m.startswith("-") else norm_m

            for col, scode in col_to_scode.items():
                val = row.get(col)
                if pd.isna(val) or str(val).strip() in ('', '-'): continue
                
                clean_val = self._clean_value(val)
                if clean_val is not None:
                    if scode not in output: output[scode] = {"values": {}}
                    output[scode]["values"][date_key] = clean_val
        return output

    def _iter_metadata(self, mapping_metadata):
        if not isinstance(mapping_metadata, dict): return []
        for k, v in mapping_metadata.items():
            if isinstance(v, dict):
                yield k, v
            else:
                yield k, {"series_code": v}

    def _match_cols_to_series(self, val_cols, mapping_metadata, reasoned_mappings=None):
        matches = {}
        for col in val_cols:
            cl = str(col).lower().strip()
            best_scode = None
            best_score = -1
            
            for code_key, meta in self._iter_metadata(mapping_metadata):
                scode = meta.get('series_code', code_key)
                prim = str(meta.get('primary_concept', '')).lower().strip()
                sec = str(meta.get('secondary_concept', '')).lower().strip()
                
                score = 0
                if prim and sec and prim in cl and sec in cl: score = 10
                elif sec and sec == cl: score = 8
                elif prim and prim == cl: score = 5
                elif prim and prim in cl: score = 2
                
                if reasoned_mappings and cl in reasoned_mappings:
                    if reasoned_mappings[cl] == scode:
                        score = 15
                
                if score > best_score and score > 0:
                    best_score = score
                    best_scode = scode
            if best_scode: matches[col] = best_scode
        return matches

    def _clean_value(self, val):
        try:
            # Handle percentage notation, commas, and whitespace
            s_clean = str(val).replace(',', '').replace('%', '').strip()
            if not s_clean or s_clean == '-': return None
            f = float(s_clean)
            
            # Filter out 0.0 values as they often indicate no data recorded for the period
            if f == 0:
                return None
                
            # Return full precision as requested
            return f
        except: return None

class DeterministicCore:
    def __init__(self):
        self.extractor = DeterministicExtractor()

    def process(self, df, layout, mapping, metadata_dict=None, base_year=2024, source_file=None, all_metadata=None):
        reasoned = None
        if isinstance(all_metadata, dict):
            reasoned = all_metadata.get("reasoned_mappings")
            
        # Ensure we use full metadata for concept matching
        primary_mapping = metadata_dict if metadata_dict and isinstance(next(iter(metadata_dict.values()), None), dict) else mapping
        
        return self.extractor.extract_concept_based(df, primary_mapping, base_year, source_file, all_metadata, reasoned_mappings=reasoned)
