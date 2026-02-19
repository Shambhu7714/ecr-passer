import pandas as pd
import re
import json

class AxisResolver:
    def resolve(self, df, layout):
        """Identifies time axis (Month / Quarter / Year)."""
        print(f"\n[INFO] AxisResolver: Analyzing time axis...")
        
        # Check column names for date patterns
        time_cols = [col for col in df.columns if any(month in str(col).lower() 
                     for month in ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
                                   'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre',
                                   'january', 'february', 'march', 'april', 'may', 'june',
                                   'july', 'august', 'september', 'october', 'november', 'december'])]
        
        if time_cols:
            print(f"[OK] Detected monthly time series ({len(time_cols)} time columns)")
            return "monthly"
        else:
            print(f"[WARN] Could not detect time axis type. Defaulting to 'monthly'")
            return "monthly"

class TimeNormalizer:
    def __init__(self):
        # Spanish month mapping
        self.month_map_es = {
            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
            'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
            'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
        }
        # English month mapping
        self.month_map_en = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12
        }
    
    def normalize(self, date_str, base_year=2024):
        """
        Converts date strings to canonical format (YYYY-MM-DD).
        """
        if pd.isna(date_str) or date_str == '':
            return "unknown"
            
        # Avoid converting small integers (like 0, 1, 2) to 1970-01-01
        is_small_num = False
        try:
            if float(date_str) < 1000:
                is_small_num = True
        except:
            pass

        if is_small_num:
            pass # Fall through to month mapping or return as-is
        else:
            try:
                # Try pandas datetime for robust formats
                return pd.to_datetime(date_str).strftime('%Y-%m-%d')
            except:
                pass
        
        # Extract month name
        date_lower = str(date_str).lower()
        
        # Try Spanish months
        for month_name, month_num in self.month_map_es.items():
            if month_name in date_lower:
                # Try to extract year from string as well
                year_match = re.search(r'20\d{2}', date_lower)
                year = year_match.group(0) if year_match else base_year
                return f"{year}-{month_num:02d}-01"
        
        # Try English months
        for month_name, month_num in self.month_map_en.items():
            if month_name in date_lower:
                year_match = re.search(r'20\d{2}', date_lower)
                year = year_match.group(0) if year_match else base_year
                return f"{year}-{month_num:02d}-01"
        
        # If all fails, return as-is
        return str(date_str)

class DeterministicExtractor:
    def __init__(self):
        self.time_normalizer = TimeNormalizer()

    def extract_concept_based(self, df, mapping_metadata, base_year=2024, source_file=None, all_metadata=None):
        """
        Extracts time series based on PRIMARY and SECONDARY concepts.
        Supports both Wide (months as columns) and Stacked (indicators as columns, time as rows).
        """
        self.all_metadata = all_metadata
        if source_file:
            print(f"[INFO] Processing with {len(mapping_metadata) if mapping_metadata else 0} pre-filtered mappings")

        print(f"\n{'='*60}")
        print(f"[INFO] STEP 4: DETERMINISTIC EXTRACTION")
        print(f"{'='*60}")
        
        # Identify columns
        concept_cols = self._identify_concept_columns(df)
        time_cols = self._identify_time_columns(df)
        
        # Detect stacked time columns (Year/Month rows)
        # Look for Year and Month columns specifically (including Spanish accented forms)
        YEAR_KWS  = ['a\u00f1o', 'a o', 'year', 'ano']
        MONTH_KWS = ['mes', 'month', 'periodo', 'per\u00edodo', 'fecha']
        
        stacked_time_cols = []
        year_col  = None
        month_col = None
        for c in df.columns:
            cl = str(c).lower().strip()
            if any(kw == cl for kw in YEAR_KWS):
                stacked_time_cols.append(c)
                year_col = c
            elif any(kw == cl for kw in MONTH_KWS):
                stacked_time_cols.append(c)
                month_col = c

        # ---------------------------------------------------------------
        # STACKED TIME-AXIS MODE
        # When Año + Mes columns exist, each ROW is a time period.
        # We map each numeric value column directly to a series code
        # using the mapping metadata — no concept matching needed.
        # ---------------------------------------------------------------
        if year_col and month_col:
            print(f"[INFO] Stacked time-axis detected: year={year_col!r}, month={month_col!r}")
            return self._extract_stacked_time_axis(
                df, year_col, month_col, mapping_metadata, base_year, all_metadata
            )

        value_cols = [c for c in df.columns if c not in concept_cols and c not in stacked_time_cols]
        
        # If we have time columns (Wide), prioritize them over generic value columns
        target_value_cols = time_cols if time_cols else value_cols
        
        print(f"[INFO] Concept Columns: {concept_cols}")
        print(f"[INFO] Time/Value Columns: {target_value_cols[:5]}...")
        if stacked_time_cols:
            print(f"[INFO] Stacked Time Columns: {stacked_time_cols}")

        # Prepare a lookup of all concepts mentioned in mappings for early filtering
        if not hasattr(self, '_concept_cache'): self._concept_cache = {}
        # Prime the cache for the current mapping if needed
        self._find_series_code_multi_concept(["dummy"], mapping_metadata)
        
        all_mapping_concepts = set()
        for item in self._concept_cache.get(id(mapping_metadata), []):
            all_mapping_concepts.update(item['concepts'])
        if self.all_metadata:
            for series_name, meta in self.all_metadata.items():
                all_mapping_concepts.add(str(meta.get('primary_concept', '')).lower())
                all_mapping_concepts.add(str(meta.get('secondary_concept', '')).lower())
                all_mapping_concepts.add(str(meta.get('third_concept', '')).lower())

        output = {}
        self._warn_count = 0
        
        # Include concept column headers in the base concepts for context
        header_concepts = [str(c).lower().strip() for c in concept_cols]
        
        for idx, row in df.iterrows():
            row_concepts = []
            for col in concept_cols:
                val = row.get(col)
                if pd.notna(val) and str(val).strip() != '':
                    val_str = str(val).strip()
                    # Handle combined ConceptID format: (CODE, DESCRIPTION)
                    if val_str.startswith('(') and val_str.endswith(')') and ',' in val_str:
                        inner = val_str[1:-1]
                        parts = [p.strip().lower() for p in inner.split(',', 1)]
                        row_concepts.extend(parts)
                        # Also add the raw code part (zfill-normalized) for numeric matching
                        raw_code = parts[0].strip()
                        if raw_code.isdigit():
                            row_concepts.append(raw_code.lstrip('0') or '0')
                    else:
                        row_concepts.append(val_str.lower())
            
            # Combine row values as the primary concepts
            row_context = row_concepts
            
            # Determine the date for this row if stacked
            row_date = None
            if stacked_time_cols:
                y = None
                for yc in stacked_time_cols:
                    ycl = str(yc).lower()
                    if any(kw in ycl for kw in ['a o', 'año', 'year', 'ano', 'a\u00f1o']):
                        y = row.get(yc)
                        break
                if y is None or pd.isna(y): y = base_year
                
                m = None
                for mc in stacked_time_cols:
                    mcl = str(mc).lower()
                    if any(kw in mcl for kw in ['mes', 'month', 'periodo']):
                        m = row.get(mc)
                        break
                if m is None or pd.isna(m): m = 1
                
                try:
                    if isinstance(m, (int, float, complex)) or (isinstance(m, str) and m.isdigit()):
                        row_date = f"{int(float(y))}-{int(float(m)):02d}-01"
                    else:
                        row_date = self.time_normalizer.normalize(f"{m} {y}", base_year)
                except:
                    pass

            # Process each value column
            for val_col in target_value_cols:
                # SKIP columns that ARE the time columns we already used
                if val_col in stacked_time_cols:
                    continue
                
                # SKIP columns that look like Year/Month but weren't caught
                vcl = str(val_col).lower()
                if vcl in ['mes', 'año', 'a o', 'year', 'month', 'ano']:
                    continue

                val = row.get(val_col)
                if pd.isna(val) or str(val).strip() == '':
                    continue
                
                try:
                    float_val = float(val)
                except:
                    continue
                
                # Combine row context with current column header + column headers context
                # Row concepts first, then the value indicator, then header noise
                val_col_str = str(val_col).lower().strip()
                current_concepts = row_context + [val_col_str] + header_concepts
                
                # Identify the date
                if row_date:
                    date_key = row_date
                else:
                    date_key = self.time_normalizer.normalize(val_col, base_year)
                
                # Match concepts
                series_code = self._find_series_code_multi_concept(current_concepts, mapping_metadata)
                match_src = "file-specific"
                
                # STRICTER GLOBAL FALLBACK: Only if score is very high (>= 2 matches)
                if not series_code and self.all_metadata:
                    # We pass a flag or just use a helper to verify significance
                    temp_match, temp_score = self._find_series_code_with_score(current_concepts, self.all_metadata)
                    if temp_score >= 2.0: # Need at least 2 concepts to match for global fallback
                        series_code = temp_match
                        match_src = "global fallback"
                
                if series_code:
                    if series_code not in output:
                        output[series_code] = {"values": {}}
                    output[series_code]["values"][date_key] = str(val)
                    
                    if len(output[series_code]["values"]) == 1:
                        print(f"[OK] Row {idx}: Matched '{val_col}' to {series_code} ({match_src})")
                else:
                    plausible = any(c in all_mapping_concepts for c in current_concepts)
                    if plausible and self._warn_count < 10:
                        c_str = ' | '.join([str(c) for c in current_concepts[:5]])
                        print(f"[WARN] Row {idx}: No mapping for concepts: {c_str}...")
                        self._warn_count += 1
        
        print(f"\n[OK] Extraction complete. Total series: {len(output)}")
        return output

    def _extract_stacked_time_axis(self, df, year_col, month_col, mapping_metadata, base_year, all_metadata):
        """
        Handles the STACKED TIME-AXIS layout where:
          - Each ROW represents a single time period (year + month)
          - Each numeric VALUE COLUMN represents a specific indicator/series
          - The mapping entry for this sheet maps each column name to a series code

        Strategy:
          1. Build the date from Año + Mes columns for each row
          2. For each remaining numeric column, find its series code:
             a. Try matching the column name as a concept against mapping
             b. If only 1 mapping entry exists for this sheet, use it directly
             c. Match column header substrings against primary/secondary concepts
          3. Assign values to matched series
        """
        print(f"\n{'='*60}")
        print(f"[INFO] STEP 4: STACKED TIME-AXIS EXTRACTION")
        print(f"{'='*60}")

        # Identify value columns: everything except year/month/empty-header cols
        skip_cols = {year_col, month_col}
        value_cols = []
        for col in df.columns:
            if col in skip_cols:
                continue
            col_lower = str(col).lower().strip()
            # Skip columns whose values are non-numeric or all NaN
            non_na = df[col].dropna()
            if len(non_na) == 0:
                continue
            numeric_count = sum(1 for v in non_na if self._is_numeric_val(v))
            if numeric_count / len(non_na) >= 0.5:  # at least 50% numeric = value column
                value_cols.append(col)

        print(f"[INFO] Value columns detected: {value_cols}")

        # Build a column → series_code lookup
        col_to_series = self._match_value_cols_to_series(value_cols, mapping_metadata, all_metadata)
        print(f"[INFO] Column→Series matches: {col_to_series}")

        output = {}
        for idx, row in df.iterrows():
            # Build date from Año + Mes
            year_val  = row.get(year_col)
            month_val = row.get(month_col)
            if pd.isna(year_val) or pd.isna(month_val):
                continue
            try:
                year_int = int(float(year_val))
            except (ValueError, TypeError):
                continue
            month_str = str(month_val).strip().lower()
            month_num = self.time_normalizer.month_map_es.get(
                month_str,
                self.time_normalizer.month_map_en.get(month_str, None)
            )
            if month_num is None:
                # Try numeric month
                try:
                    month_num = int(float(month_val))
                except (ValueError, TypeError):
                    continue
            date_key = f"{year_int}-{month_num:02d}-01"

            for col in value_cols:
                series_code = col_to_series.get(col)
                if not series_code:
                    continue
                val = row.get(col)
                if pd.isna(val):
                    continue
                try:
                    float(val)  # validate it's numeric
                except (ValueError, TypeError):
                    continue
                if series_code not in output:
                    output[series_code] = {"values": {}}
                output[series_code]["values"][date_key] = str(val)

        print(f"\n[OK] Stacked extraction complete. Total series: {len(output)}")
        return output

    def _is_numeric_val(self, val):
        """Return True if val can be parsed as a float."""
        if isinstance(val, (int, float)):
            return not pd.isna(val)
        try:
            float(str(val).strip().replace(',', '.'))
            return True
        except (ValueError, TypeError):
            return False

    def _match_value_cols_to_series(self, value_cols, mapping_metadata, all_metadata):
        """
        Match each value column name to a series code from the mapping.

        Priority order:
          1. Exact column-name match against primary/secondary concept
          2. Substring match
          3. If mapping has exactly 1 entry and 1 value column → use that entry directly
        """
        col_to_series = {}
        all_meta = dict(mapping_metadata or {})
        if all_metadata:
            all_meta.update(all_metadata)

        entries = list((mapping_metadata or {}).items())
        n_entries = len(entries)

        for col in value_cols:
            col_lower = str(col).lower().strip()
            best_code = None
            best_score = 0

            for series_name, meta in (mapping_metadata or {}).items():
                prim = str(meta.get('primary_concept', '')).lower().strip()
                sec  = str(meta.get('secondary_concept', '')).lower().strip()
                code = meta.get('series_code', series_name)

                score = 0
                if col_lower == prim:
                    score = 3
                elif col_lower == sec:
                    score = 2
                elif prim and (prim in col_lower or col_lower in prim):
                    score = 1
                elif sec and (sec in col_lower or col_lower in sec):
                    score = 0.5

                if score > best_score:
                    best_score = score
                    best_code = code

            if best_code:
                col_to_series[col] = best_code
            elif n_entries == 1 and len(value_cols) == 1:
                # Single mapping + single value column → direct assignment
                col_to_series[col] = entries[0][1].get('series_code', entries[0][0])
                print(f"[INFO] Direct 1:1 mapping assignment: {col!r} → {col_to_series[col]}")
            elif n_entries == 1:
                # Single mapping entry, multiple columns — assign to first/best column
                # Find the column most likely to be the primary index
                primary_prim = str(entries[0][1].get('primary_concept', '')).lower()
                if col_lower == primary_prim or primary_prim in col_lower:
                    col_to_series[col] = entries[0][1].get('series_code', entries[0][0])
                    print(f"[INFO] Single-entry mapping matched: {col!r} → {col_to_series[col]}")

        return col_to_series

    
    def _identify_concept_columns(self, df):
        """Identify columns containing concept information."""
        concept_keywords = [
            'c digo', 'codigo', 'division', 'divisi n', 'concepto', 'descripci n',
            'description', 'conceptid', 'concept_id', 'ponderaci n', 'clases',
            'actividad', 'sector', 'nombre', 'grupo', 'group', 'category'
        ]
        concept_cols = []
        
        for col in df.columns:
            col_lower = str(col).lower().strip()
            # Direct keyword match
            if any(keyword in col_lower for keyword in concept_keywords):
                concept_cols.append(col)
                continue
            # Also capture purely object dtype columns in first 3 columns
            # (fallback for columns without standard names)
        
        # If no keywords found, assume first non-numeric column is a concept
        if not concept_cols:
            for col in df.columns[:4]:  # Check first 4 cols
                if df[col].dtype == object or str(df[col].dtype) == 'string':
                    concept_cols.append(col)
                    break
            if not concept_cols:
                concept_cols = list(df.columns[:1])
        
        return concept_cols
    
    def _identify_time_columns(self, df):
        """Identify columns containing time series data."""
        time_keywords = [' ndice', 'indice', 'enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
                        'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre',
                        'january', 'february', 'march', 'april', 'may', 'june',
                        'july', 'august', 'september', 'october', 'november', 'december',
                        '2020', '2021', '2022', '2023', '2024', '2025']
        
        time_cols = []
        for col in df.columns:
            col_str = str(col).lower()
            if any(keyword in col_str for keyword in time_keywords):
                time_cols.append(col)
        
        return time_cols
    
    def _find_series_code(self, primary_concept, secondary_concept, mapping_metadata):
        """
        Find series code by matching primary concept (C digo) and secondary concept (Divisi n).
        Primary concept is matched as string (exact match), secondary concept uses flexible text matching.
        """
        # Convert primary concept to string for comparison
        if pd.isna(primary_concept) or primary_concept == '':
            return None
        
        # Format primary concept as string (handle int/float by converting to string)
        if isinstance(primary_concept, (int, float)):
            primary_str = str(int(primary_concept)).zfill(8)  # Pad with zeros to 8 digits
        else:
            primary_str = str(primary_concept).strip()
        
        # Debug: Track if we found primary match
        found_primary_match = False
        
        # Search through mapping metadata
        for series_name, metadata in mapping_metadata.items():
            # Get primary concept from metadata (should be C digo)
            metadata_primary = metadata.get('primary_concept', '')
            
            if pd.isna(metadata_primary) or metadata_primary == '':
                continue
            
            # Convert metadata primary to string
            metadata_primary_str = str(metadata_primary).strip()
            
            # Check if primary concepts match (exact string match)
            if metadata_primary_str != primary_str:
                continue
            
            # Primary concept matched!
            found_primary_match = True
            secondary_match = str(metadata.get('secondary_concept', '')).lower().strip() if pd.notna(metadata.get('secondary_concept')) else ''
            
            if secondary_match and secondary_concept:
                # Secondary concept exists in both - do flexible text matching
                secondary_lower = secondary_concept.lower().strip()
                if secondary_match in secondary_lower or secondary_lower in secondary_match:
                    return metadata.get('series_code', series_name)
                else:
                    # Debug output for secondary mismatch
                    print(f"  [SEARCH] Primary matched ('{primary_str}'), but secondary didn't: '{secondary_lower}' vs '{secondary_match}'")
            else:
                # No secondary concept required, primary match is enough
                return metadata.get('series_code', series_name)
        
        if found_primary_match:
            print(f"  [SEARCH] Primary concept '{primary_str}' found but secondary concept '{secondary_concept}' didn't match")
        
        return None
    
    def _find_series_code_multi_concept(self, concepts, mapping_metadata):
        """
        Find best matching series code using multi-concept matching.
        """
        code, score = self._find_series_code_with_score(concepts, mapping_metadata)
        return code

    def _find_series_code_with_score(self, concepts, mapping_metadata):
        """
        Find best matching series and its match score.
        """
        if not concepts or len(concepts) == 0:
            return None, 0
        
        # Performance Optimization: Cache pre-parsed meta concepts
        cache_key = id(mapping_metadata)
        if not hasattr(self, '_concept_cache'): self._concept_cache = {}
        
        if cache_key not in self._concept_cache:
            self._concept_cache[cache_key] = []
            for series_name, metadata in mapping_metadata.items():
                m_concepts = [
                    str(metadata.get('primary_concept', '')).strip().lower(),
                    str(metadata.get('secondary_concept', '')).strip().lower(),
                    str(metadata.get('third_concept', '')).strip().lower(),
                    str(metadata.get('fourth_concept', '')).strip().lower()
                ]
                m_concepts = [c for c in m_concepts if c and c != 'nan']
                if m_concepts:
                    self._concept_cache[cache_key].append({
                        'concepts': m_concepts,
                        'code': metadata.get('series_code', series_name)
                    })
        
        best_match = None
        best_match_count = 0
        
        # Search through cached concepts
        for item in self._concept_cache[cache_key]:
            meta_concepts = item['concepts']
            series_code = item['code']
            
            # Count how many concepts match (in order)
            matches = 0
            for i in range(min(len(concepts), len(meta_concepts))):
                data_val = str(concepts[i]).strip().lower()
                meta_val = meta_concepts[i].lower()
                
                # Match logic: Priority to row values
                if data_val == meta_val:
                    matches += 1
                elif meta_val in data_val or data_val in meta_val:
                    matches += 0.5
            
            if matches > best_match_count:
                best_match_count = matches
                best_match = series_code
        
        # Return best match if we matched at least one full concept
        if best_match_count >= 1:
            return best_match, best_match_count
        return None, 0

class DeterministicCore:
    def __init__(self):
        self.axis_resolver = AxisResolver()
        self.extractor = DeterministicExtractor()

    def process(self, df, layout, mapping, metadata_dict=None, base_year=2024, source_file=None, all_metadata=None):
        """
        Main processing function.
        
        Args:
            df: Input dataframe
            layout: Layout information from Intelligence Layer
            mapping: Series name mapping (Update Name -> Code)
            metadata_dict: Pre-filtered metadata for this file
            base_year: Base year for date normalization
            source_file: Source file path
            all_metadata: Full mapping dictionary (for fallback)
        """
        print(f"\n{'='*60}")
        print(f"[INFO] DETERMINISTIC CORE PROCESSING")
        print(f"{'='*60}")
        
        # 1. Resolve Axis
        axis_type = self.axis_resolver.resolve(df, layout)
        
        # 2. Extract Data (concept-based extraction)
        if metadata_dict or all_metadata:
            result = self.extractor.extract_concept_based(
                df, 
                metadata_dict, 
                base_year, 
                source_file=source_file,
                all_metadata=all_metadata
            )
        else:
            print("[WARN] No metadata provided. Cannot perform concept-based extraction.")
            result = {}
        
        return result