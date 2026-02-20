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
            'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
        }

    def parse_date(self, s):
        """Robust date parsing for headers or cells."""
        s = str(s).strip().lower().replace('\n', ' ')
        if not s or s == 'nan' or s == 'none': return None
        
        # Pattern: "YYYY-MM-DD"
        if re.match(r'^\d{4}-\d{1,2}-\d{1,2}', s):
            return s[:10]
            
        # Pattern: "MM-YYYY" or "YYYY-MM" or "Year - Month"
        # Split by non-alphanumeric (except space)
        parts = [p.strip() for p in re.split(r'[^a-z0-9]', s) if p.strip()]
        y, m = None, None
        for p in parts:
            if p.isdigit():
                val = int(p)
                if 1900 < val < 2100: y = val
                elif 1 <= val <= 12: m = val
            elif p in self.month_map_es:
                 m = self.month_map_es[p]
            elif p in self.month_map_en:
                 m = self.month_map_en[p]
        
        if y and m: return f"{y}-{m:02d}-01"
        if y: return f"{y}-01-01"
        return None

class DeterministicExtractor:
    def __init__(self):
        self.time_normalizer = TimeNormalizer()

    def extract_concept_based(self, df, mapping_metadata, base_year=2024, source_file=None, all_metadata=None):
        if df.empty: return {}
        
        # 1. Detect Layout (Pivot or Stacked)
        # GEIH is Pivot (Time in headers), ELIC is Stacked (Time in rows)
        date_headers = sum(1 for col in df.columns if self.time_normalizer.parse_date(col))
        
        if date_headers >= 3:
            return self._extract_pivot(df, mapping_metadata)
        else:
            return self._extract_stacked(df, mapping_metadata, base_year)

    def _extract_pivot(self, df, mapping_metadata):
        """Time in columns, Indicators in rows (GEIH Style)"""
        # Find indicator column (usually col 0 or 1)
        best_col = None
        best_match_count = -1
        
        concepts = []
        for meta in mapping_metadata.values():
            if meta.get('primary_concept'): concepts.append(str(meta['primary_concept']).lower().strip())
            if meta.get('secondary_concept'): concepts.append(str(meta['secondary_concept']).lower().strip())
        
        for cidx in range(min(4, len(df.columns))):
            matches = sum(1 for v in df.iloc[:, cidx].dropna() if str(v).lower().strip() in concepts)
            if matches > best_match_count:
                best_match_count = matches
                best_col = df.columns[cidx]
        
        if best_col is None: return {}
        
        date_cols = [c for c in df.columns if self.time_normalizer.parse_date(c)]
        output = {}
        
        for _, row in df.iterrows():
            row_val = str(row.get(best_col, '')).lower().strip()
            if not row_val: continue
            
            for code_key, meta in mapping_metadata.items():
                scode = meta.get('series_code', code_key)
                prim = str(meta.get('primary_concept', '')).lower().strip()
                sec = str(meta.get('secondary_concept', '')).lower().strip()
                
                # Match logic: Exact match on secondary is usually best for GEIH
                if row_val == sec or row_val == prim or (prim and sec and row_val == sec):
                    if scode not in output: output[scode] = {"values": {}}
                    for col in date_cols:
                        dkey = self.time_normalizer.parse_date(col)
                        val = row.get(col)
                        if pd.isna(val) or str(val).strip() in ('', '-'): continue
                        output[scode]["values"][dkey] = str(val)
                    break 
        return output

    def _extract_stacked(self, df, mapping_metadata, base_year):
        """Indicators in columns, Time in rows (ELIC Style)"""
        YEAR_KWS = ['año', 'year', 'anos', 'ano', 'años']
        MONTH_KWS = ['mes', 'month', 'periodo', 'periodo', 'meses']
        
        # Pick Year and Month columns
        y_col, m_col = None, None
        best_y_score, best_m_score = -1, -1
        
        for col in df.columns:
            cl = str(col).lower()
            vals = df[col].dropna().astype(str).str.strip().str.lower()
            if vals.empty: continue
            
            y_hits = sum(1 for v in vals if v.replace('.0','').isdigit() and 1990 < int(float(v)) < 2100)
            m_hits = sum(1 for v in vals if v in self.time_normalizer.month_map_es or (v.isdigit() and 1 <= int(float(v)) <= 12))
            
            if any(kw in cl for kw in YEAR_KWS): y_hits += 50
            if any(kw in cl for kw in MONTH_KWS): m_hits += 50
            
            if y_hits > best_y_score:
                best_y_score = y_hits
                y_col = col
            if m_hits > best_m_score:
                best_m_score = m_hits
                m_col = col

        if y_col == m_col or not y_col or not m_col: return {}
        
        val_cols = [c for c in df.columns if c not in [y_col, m_col]]
        col_to_scode = self._match_cols_to_series(val_cols, mapping_metadata)
        
        output = {}
        for _, row in df.iterrows():
            y_raw = row.get(y_col)
            m_raw = row.get(m_col)
            if pd.isna(y_raw) or pd.isna(m_raw): continue
            
            try:
                y_val = int(float(y_raw))
                m_str = str(m_raw).strip().lower()
                m_val = self.time_normalizer.month_map_es.get(m_str, self.time_normalizer.month_map_en.get(m_str))
                if m_val is None: m_val = int(float(m_raw))
                date_key = f"{y_val}-{m_val:02d}-01"
            except: continue
            
            for col, scode in col_to_scode.items():
                val = row.get(col)
                if pd.isna(val) or str(val).strip() in ('', '-'): continue
                if scode not in output: output[scode] = {"values": {}}
                output[scode]["values"][date_key] = str(val)
        return output

    def _match_cols_to_series(self, val_cols, mapping_metadata):
        matches = {}
        for col in val_cols:
            cl = str(col).lower().strip()
            best_scode = None
            best_score = -1
            
            for code_key, meta in mapping_metadata.items():
                scode = meta.get('series_code', code_key)
                prim = str(meta.get('primary_concept', '')).lower().strip()
                sec = str(meta.get('secondary_concept', '')).lower().strip()
                
                score = 0
                if prim and sec:
                    if prim in cl and sec in cl: score = 5
                elif prim:
                    if prim == cl: score = 3
                    elif prim in cl: score = 1
                
                if score > best_score:
                    best_score = score
                    best_scode = scode
            if best_scode: matches[col] = best_scode
        return matches

class DeterministicCore:
    def __init__(self):
        self.extractor = DeterministicExtractor()

    def process(self, df, layout, mapping, metadata_dict=None, base_year=2024, source_file=None, all_metadata=None):
        return self.extractor.extract_concept_based(df, metadata_dict, base_year, source_file, all_metadata)
