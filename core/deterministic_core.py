import pandas as pd
import re
import json

class AxisResolver:
    def resolve(self, df, layout):
        """Identifies time axis (Month / Quarter / Year)."""
        print(f"\n🔹 AxisResolver: Analyzing time axis...")
        
        # Check column names for date patterns
        time_cols = [col for col in df.columns if any(month in str(col).lower() 
                     for month in ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
                                   'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre',
                                   'january', 'february', 'march', 'april', 'may', 'june',
                                   'july', 'august', 'september', 'october', 'november', 'december'])]
        
        if time_cols:
            print(f"✅ Detected monthly time series ({len(time_cols)} time columns)")
            return "monthly"
        else:
            print(f"⚠️ Could not detect time axis type. Defaulting to 'monthly'")
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
        Handles formats like: "(Índice, Enero)", "Enero", "January", etc.
        """
        try:
            # Try pandas datetime first
            return pd.to_datetime(date_str).strftime('%Y-%m-%d')
        except:
            pass
        
        # Extract month name from patterns like "(Índice, Enero)"
        date_lower = str(date_str).lower()
        
        # Try Spanish months
        for month_name, month_num in self.month_map_es.items():
            if month_name in date_lower:
                return f"{base_year}-{month_num:02d}-01"
        
        # Try English months
        for month_name, month_num in self.month_map_en.items():
            if month_name in date_lower:
                return f"{base_year}-{month_num:02d}-01"
        
        # If all fails, return as-is
        return str(date_str)

class DeterministicExtractor:
    def __init__(self):
        self.time_normalizer = TimeNormalizer()

    def extract_concept_based(self, df, mapping_metadata, base_year=2024, source_file=None):
        """
        Extracts time series based on PRIMARY and SECONDARY concepts.
        
        Process:
        1. Identify concept columns (Código, División, etc.)
        2. Identify time series columns (months/dates)
        3. For each row, match concepts to mapping file
        4. Extract time series values for matched series
        
        Args:
            df: Input dataframe
            mapping_metadata: Metadata dictionary from MapLoader (will be filtered by source_file)
            base_year: Base year for date normalization
            source_file: Source file path (used to filter mappings)
        
        Returns: Dictionary format { "SERIES_CODE": { "values": { "date": value } } }
        """
        # Filter mappings by source file if provided
        if source_file:
            import os
            basename = os.path.basename(source_file).lower()
            filtered_metadata = {}
            for series_name, meta in mapping_metadata.items():
                src = str(meta.get('source_file', '')).lower().strip()
                if src and basename in src:
                    filtered_metadata[series_name] = meta
            
            if filtered_metadata:
                print(f"📂 Filtered to {len(filtered_metadata)} mappings relevant for file: {basename}")
                mapping_metadata = filtered_metadata
            else:
                print(f"⚠️ No mappings found for source file: {basename}. Using all {len(mapping_metadata)} mappings.")
        print(f"\n{'='*60}")
        print(f"🔹 STEP 4: DETERMINISTIC EXTRACTION")
        print(f"{'='*60}")
        
        # Step 1: Identify concept columns and time columns
        concept_cols = self._identify_concept_columns(df)
        time_cols = self._identify_time_columns(df)
        
        print(f"📊 Identified {len(concept_cols)} concept columns: {concept_cols}")
        print(f"📅 Identified {len(time_cols)} time columns (showing first 3): {time_cols[:3]}")
        
        # Step 2: Build output structure
        output = {}
        
        # Step 3: Process each row
        for idx, row in df.iterrows():
            # Extract concepts from this row - use all available concept columns
            concepts = []
            for i, col in enumerate(concept_cols):
                val = row.get(col, '')
                if pd.notna(val) and val != '':
                    concepts.append(str(val))
            
            # Need at least one concept to proceed
            if not concepts:
                continue
            
            # Primary concept is the first non-empty concept
            primary_concept = concepts[0] if len(concepts) > 0 else ''
            
            # Skip invalid primary concepts
            if pd.isna(primary_concept) or primary_concept == '' or str(primary_concept) == 'nan':
                continue
            
            # Find matching series code from mapping using all concepts
            series_code = self._find_series_code_multi_concept(concepts, mapping_metadata)
            
            if not series_code:
                concepts_str = ' | '.join([f"Concept{i+1}={c}" for i, c in enumerate(concepts)])
                print(f"⚠️ Row {idx}: No mapping found for {concepts_str}")
                continue
            
            concepts_str = ' | '.join([f"Concept{i+1}={c}" for i, c in enumerate(concepts)])
            print(f"✅ Row {idx}: Matched {concepts_str} → {series_code}")
            
            # Extract time series values
            values_dict = {}
            for time_col in time_cols:
                value = row.get(time_col)
                
                # Skip non-numeric values
                if pd.isna(value):
                    continue
                
                try:
                    numeric_value = float(value)
                    normalized_date = self.time_normalizer.normalize(time_col, base_year)
                    values_dict[normalized_date] = str(numeric_value)
                except:
                    continue
            
            # Add to output
            if values_dict:
                output[series_code] = {"values": values_dict}
                print(f"   📈 Extracted {len(values_dict)} time series values")
        
        print(f"\n✅ Extraction complete. Total series: {len(output)}")
        return output
    
    def _identify_concept_columns(self, df):
        """Identify columns containing concept information."""
        concept_keywords = ['código', 'division', 'división', 'concepto', 'descripción', 'description']
        concept_cols = []
        
        for col in df.columns:
            col_lower = str(col).lower()
            if any(keyword in col_lower for keyword in concept_keywords):
                concept_cols.append(col)
        
        # If no keywords found, assume first 2 columns are concepts
        if not concept_cols:
            concept_cols = list(df.columns[:2])
        
        return concept_cols
    
    def _identify_time_columns(self, df):
        """Identify columns containing time series data."""
        time_keywords = ['índice', 'indice', 'enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
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
        Find series code by matching primary concept (Código) and secondary concept (División).
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
            # Get primary concept from metadata (should be Código)
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
                    print(f"  🔍 Primary matched ('{primary_str}'), but secondary didn't: '{secondary_lower}' vs '{secondary_match}'")
            else:
                # No secondary concept required, primary match is enough
                return metadata.get('series_code', series_name)
        
        if found_primary_match:
            print(f"  🔍 Primary concept '{primary_str}' found but secondary concept '{secondary_concept}' didn't match")
        
        return None
    
    def _find_series_code_multi_concept(self, concepts, mapping_metadata):
        """
        Find series code by matching against primary, secondary, third, and fourth concepts.
        Tries to match as many concepts as possible, prioritizing exact matches.
        
        Args:
            concepts: List of concept values from the data row (in order)
            mapping_metadata: Dictionary of series metadata
        
        Returns:
            Series code if match found, None otherwise
        """
        if not concepts or len(concepts) == 0:
            return None
        
        best_match = None
        best_match_count = 0
        
        # Search through all mapping entries
        for series_name, metadata in mapping_metadata.items():
            # Get all concept levels from metadata
            meta_concepts = [
                str(metadata.get('primary_concept', '')).strip(),
                str(metadata.get('secondary_concept', '')).strip(),
                str(metadata.get('third_concept', '')).strip(),
                str(metadata.get('fourth_concept', '')).strip()
            ]
            
            # Remove empty concepts
            meta_concepts = [c for c in meta_concepts if c and c.lower() != 'nan']
            
            if not meta_concepts:
                continue
            
            # Count how many concepts match (in order)
            matches = 0
            for i in range(min(len(concepts), len(meta_concepts))):
                data_val = str(concepts[i]).strip().lower()
                meta_val = meta_concepts[i].lower()
                
                # Try exact match first
                if data_val == meta_val:
                    matches += 1
                # Try partial match (either contains the other)
                elif meta_val in data_val or data_val in meta_val:
                    matches += 0.5  # Partial match worth less
            
            # Update best match if this is better
            if matches > best_match_count:
                best_match_count = matches
                best_match = metadata.get('series_code', series_name)
        
        # Return best match if we matched at least the primary concept
        if best_match_count >= 1:
            return best_match
        
        return None

class DeterministicCore:
    def __init__(self):
        self.axis_resolver = AxisResolver()
        self.extractor = DeterministicExtractor()

    def process(self, df, layout, mapping, metadata_dict=None, base_year=2024, source_file=None):
        """
        Main processing function.
        
        Args:
            df: Input dataframe
            layout: Layout information from Intelligence Layer
            mapping: Series name mapping
            metadata_dict: Metadata dictionary from MapLoader
            base_year: Base year for date normalization
            source_file: Source file path (used to filter mappings)
        """
        print(f"\n{'='*60}")
        print(f"🔹 DETERMINISTIC CORE PROCESSING")
        print(f"{'='*60}")
        
        # 1. Resolve Axis
        axis_type = self.axis_resolver.resolve(df, layout)
        
        # 2. Extract Data (concept-based extraction)
        if metadata_dict:
            result = self.extractor.extract_concept_based(df, metadata_dict, base_year, source_file=source_file)
        else:
            print("⚠️ No metadata provided. Cannot perform concept-based extraction.")
            result = {}
        
        return result