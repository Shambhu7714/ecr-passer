import pandas as pd
import openpyxl
import os
import re
from core.country_config import CountryConfigLoader

_country_loader = CountryConfigLoader()

def preprocess_excel(file_path, sheet_name=None, mapping_file=None):
    """
    Reads raw Excel, handles merged cells, and prepares a clean grid.
    Removes header rows, footers, and extracts only the data table.
    Flattens multi-level headers properly.
    Dynamically uses country-specific keywords via CountryConfigLoader.
    Falls back to intelligent data-table detection when keywords fail.

    Args:
        file_path: Path to Excel file
        sheet_name: Specific sheet name to read (if None, reads first sheet)
        mapping_file: Path to mapping file (used for country detection)
    """
    # Load country config dynamically (Problem 1 + 4 Fix)
    country_cfg = _country_loader.detect_and_load(file_path, mapping_file)
    header_keywords = country_cfg.get("header_detection_keywords", [])
    footer_keywords_cfg = country_cfg.get("footer_keywords", [])
    time_keywords_cfg = country_cfg.get("time_keywords", [])
    code_column_names = country_cfg.get("code_column_names", ["C digo", "Code"])
    sheet_info = f" (Sheet: {sheet_name})" if sheet_name else ""
    print(f"\n{'='*60}")
    print(f"[INFO] STEP 1: PREPROCESSING - {file_path}{sheet_info}")
    print(f"{'='*60}")
    
    # Load mapping file for group descriptions
    mapping_df = None
    _mapping_path = mapping_file or 'config/Argentina_Map_Updated.xlsx'
    if _mapping_path and os.path.exists(_mapping_path):
        try:
            mapping_df = pd.read_excel(_mapping_path, sheet_name='Sheet1', engine='openpyxl')
            print(f"[INFO] Loaded mapping file for group descriptions: {_mapping_path}")
        except Exception as _e:
            print(f"[WARN] Could not load mapping file '{_mapping_path}': {_e}. Will use input descriptions.")
    
    # If sheet_name is None, pandas reads first sheet (0)
    effective_sheet = sheet_name if sheet_name is not None else 0
    
    # Load raw file with no header to inspect all rows
    df = pd.read_excel(file_path, sheet_name=effective_sheet, header=None)
    print(f"  Loaded raw file with shape: {df.shape}")
    
    # -----------------------------------------------------------------------
    # STEP A: Detect year (only look in text-like cells, not row indices)
    # -----------------------------------------------------------------------
    year = None
    for idx in range(min(15, len(df))):
        for col_pos, val in enumerate(df.iloc[idx].values):
            if pd.isna(val):
                continue
            val_str = str(val).strip()
            # Only treat as year if it's purely a 4-digit year starting with 20
            if re.fullmatch(r'20\d{2}', val_str):
                year = val_str
                print(f"[INFO] Found year: {year} at row {idx}")
                break
            # Also catch "Enero de 2026", "2026" embedded in a text cell
            if col_pos <= 2:  # Only look in first 3 columns to avoid false hits
                year_match = re.search(r'\b(20[12]\d)\b', val_str)
                if year_match:
                    year = year_match.group(1)
                    print(f"[INFO] Found year: {year} embedded in text at row {idx}")
                    break
        if year:
            break

    # -----------------------------------------------------------------------
    # STEP B: Find the actual data table header row
    # Strategy 1: Match country-specific keywords
    # Strategy 2: Intelligent scan — find the row where the block below it
    #             has the most numeric data (the true header row)
    # -----------------------------------------------------------------------
    header_row_idx = _find_header_by_keywords(df, header_keywords)
    
    if header_row_idx is None:
        print("[WARN] Keyword-based header detection failed. Switching to intelligent scan...")
        header_row_idx = _find_data_table_start(df)
        if header_row_idx is not None:
            row_preview = ' | '.join([str(v) for v in df.iloc[header_row_idx].values if pd.notna(v)][:6])
            print(f"[OK] Auto-detected header at row {header_row_idx}: {row_preview[:80]}")
        else:
            header_row_idx = 0
            print("[WARN] Could not auto-detect header. Using first row.")

    # -----------------------------------------------------------------------
    # STEP C: Check for a second header row (months / time periods)
    # -----------------------------------------------------------------------
    second_header_idx = header_row_idx + 1
    has_second_header = False
    
    # Only treat as a second header row if it contains ACTUAL calendar month/quarter names.
    # Words like 'Mensual', 'Anual', 'Trimestral' are periodic TYPE labels, not date sub-headers.
    REAL_TIME_SUBHEADER_KWS = [
        'enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
        'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre',
        'january', 'february', 'march', 'april', 'june',
        'july', 'august', 'september', 'october', 'november', 'december',
        'q1', 'q2', 'q3', 'q4', 'trimestre i', 'trimestre ii', 'trim.',
    ]
    if second_header_idx < len(df):
        second_row = df.iloc[second_header_idx]
        for val in second_row.values:
            val_lower = str(val).lower() if pd.notna(val) else ''
            if any(kw in val_lower for kw in REAL_TIME_SUBHEADER_KWS):
                has_second_header = True
                print(f"[OK] Found second header row (time periods) at index: {second_header_idx}")
                break

    # -----------------------------------------------------------------------
    # STEP D: Read with proper headers
    # -----------------------------------------------------------------------
    if has_second_header:
        df_clean = pd.read_excel(file_path, sheet_name=effective_sheet,
                                  header=[header_row_idx, second_header_idx])
        new_columns = []
        for col in df_clean.columns:
            if isinstance(col, tuple):
                parts = [str(c).strip() for c in col
                         if 'Unnamed' not in str(c) and str(c) != 'nan' and str(c).strip() != '']
                if len(parts) == 2:
                    new_col = f"{parts[0]}, {parts[1]}, {year}" if year else ', '.join(parts)
                elif len(parts) == 1:
                    new_col = parts[0]
                else:
                    new_col = ', '.join(parts) if parts else 'Column'
            else:
                new_col = str(col)
            new_columns.append(new_col)
        df_clean.columns = new_columns
        print(f"[OK] Flattened headers: {list(df_clean.columns)[:5]}... (showing first 5)")
    else:
        df_clean = pd.read_excel(file_path, sheet_name=effective_sheet, header=header_row_idx)

    # -----------------------------------------------------------------------
    # STEP E: Normalize code column
    # -----------------------------------------------------------------------
    code_col_found = None
    for col_name in code_column_names:
        if col_name in df_clean.columns:
            code_col_found = col_name
            break
    if code_col_found:
        pad_len = country_cfg.get("code_pad_length", 8)
        df_clean[code_col_found] = df_clean[code_col_found].astype(str).str.zfill(pad_len)
        print(f"[OK] Normalized '{code_col_found}' column to {pad_len}-digit strings")

    # -----------------------------------------------------------------------
    # STEP F: Handle merged cells, drop fully empty rows/cols, remove footers
    # -----------------------------------------------------------------------
    df_clean = df_clean.ffill(axis=0).ffill(axis=1)
    df_clean = df_clean.dropna(how='all').dropna(how='all', axis=1)

    # Remove footer rows (only if the dataframe has enough rows to justify it)
    if len(df_clean) > 3:
        for idx, row in df_clean.iterrows():
            search_area = " ".join(str(val) for val in row.iloc[:3].values if pd.notna(val)).lower()
            if any(keyword.lower() in search_area for keyword in footer_keywords_cfg):
                df_clean = df_clean.iloc[:idx]
                print(f"[CUT] Removed footer rows starting at index: {idx}")
                break

    # Remove rows with NaN in the code column
    if code_col_found and code_col_found in df_clean.columns:
        original_len = len(df_clean)
        df_clean = df_clean[df_clean[code_col_found].notna()]
        if len(df_clean) < original_len:
            print(f"[CUT] Removed {original_len - len(df_clean)} rows with NaN codes")

    print(f"[OK] Preprocessing complete. Clean shape: {df_clean.shape}")
    print(f"[INFO] Columns: {list(df_clean.columns)[:5]}... (showing first 5)")

    # Aggregate subcategories to main groups if code + description columns exist
    desc_col_names = country_cfg.get("description_column_names", ["Grupo", "Group"])
    desc_col_found = next((c for c in desc_col_names if c in df_clean.columns), None)
    if code_col_found and desc_col_found:
        df_clean = _aggregate_to_main_groups(
            df_clean, mapping_df,
            code_col=code_col_found,
            desc_col=desc_col_found,
            prefix_len=country_cfg.get("group_code_prefix_length", 2),
            pad_len=country_cfg.get("code_pad_length", 8)
        )

    return df_clean


def _find_header_by_keywords(df, header_keywords):
    """
    Attempt to find a header row by matching known country-specific keywords.
    Returns the row index or None if not found.
    """
    for idx, row in df.iterrows():
        row_str = ' '.join([str(x) for x in row.values if pd.notna(x)])
        if any(kw in row_str for kw in header_keywords):
            print(f"[OK] Found header row at index: {idx} (matched keyword in: {row_str[:60]})")
            return idx
    return None


# Strong column-label keywords that are clear signs of a header row
_STRONG_HEADER_LABELS = [
    'año', 'mes', 'indice', 'índice', 'variacion', 'variación',
    'periodo', 'período', 'fecha', 'date', 'year', 'month',
    'ciudades', 'ciudad', 'region', 'región', 'area', 'área',
    'codigo', 'código', 'grupo', 'descripcion', 'descripción',
    'total', 'nacional', 'category', 'indicator',
]


def _find_data_table_start(df, min_rows_below=3, min_numeric_ratio=0.3):
    """
    Intelligently detect the header row of a data table by scanning the sheet.

    Strategy:
      For each candidate row (top 30 rows), compute a score =
        (number of data rows below with numeric content)
        + bonus if the row itself has strong column-label keywords.

      The candidate with the highest score is the header.

    This works for ANY layout:
      - IPC tables with Código / Grupo / months as columns
      - City × category cross-tables (Ciudades as rows, categories as columns)
      - Stacked time-series with Año/Mes/Índice/Variación columns
      - Any other tabular structure

    Args:
        df: Raw dataframe (no header set)
        min_rows_below: Minimum data rows required below a candidate header
        min_numeric_ratio: Minimum fraction of cells that must be numeric in data rows

    Returns:
        Best header row index, or None if not found
    """
    n_rows = len(df)
    n_cols = max(df.shape[1], 1)

    best_idx = None
    best_score = -1

    # Only scan first 30 rows as potential headers
    search_limit = min(30, n_rows - min_rows_below)

    for candidate_idx in range(search_limit):
        # The candidate row itself should have at least 2 non-null values
        candidate_row = df.iloc[candidate_idx]
        non_null_vals = [v for v in candidate_row.values if pd.notna(v) and str(v).strip() != '']
        if len(non_null_vals) < 2:
            continue

        # Must have at least one text-like (non-numeric) value — real headers have labels
        text_count = sum(1 for v in non_null_vals if not _is_numeric(v))
        if text_count < 1:
            continue

        # BONUS: strong column-label keywords boost the score
        row_str_lower = ' '.join(str(v).lower() for v in non_null_vals if not _is_numeric(v))
        label_bonus = sum(1 for kw in _STRONG_HEADER_LABELS if kw in row_str_lower)

        # Scan rows below the candidate — count how many have numeric data
        numeric_row_count = 0
        data_rows_checked = 0
        for data_idx in range(candidate_idx + 1, min(candidate_idx + 1 + 50, n_rows)):
            data_row = df.iloc[data_idx]
            numeric_vals = [v for v in data_row.values if _is_numeric(v)]
            if len(numeric_vals) / n_cols >= min_numeric_ratio:
                numeric_row_count += 1
            data_rows_checked += 1

        if data_rows_checked < min_rows_below:
            continue

        # Final score: numeric rows below + 5× label quality bonus
        score = numeric_row_count + label_bonus * 5
        if score > best_score:
            best_score = score
            best_idx = candidate_idx

    if best_idx is not None and best_score >= min_rows_below:
        return best_idx
    return None


def _is_numeric(val):
    """Return True if val is a number or a string that represents a number."""
    if isinstance(val, (int, float)):
        return not pd.isna(val)
    try:
        float(str(val).strip().replace(',', '.'))
        return True
    except (ValueError, TypeError):
        return False

def _aggregate_to_main_groups(df, mapping_df=None, code_col='C digo', desc_col='Grupo',
                               prefix_len=2, pad_len=8):
    """
    Aggregates subcategories to main groups based on first N digits of code column.
    Averages numeric values (Ponderaci n and time series data).
    Uses mapping file to get correct group descriptions.
    Returns aggregated dataframe with combined (CODE, DESCRIPTION) format
    and saves metadata for downstream processing.

    Args:
        df: Input dataframe
        mapping_df: Mapping dataframe for description lookup
        code_col: Name of the code column (country-specific, default 'C digo')
        desc_col: Name of the description column (country-specific, default 'Grupo')
        prefix_len: Number of digits to use as group prefix (default 2)
        pad_len: Total length to pad codes to (default 8)
    """
    print(f"\n[INFO] Aggregating {len(df)} subcategories to main groups...")
    print(f"   Code column: '{code_col}', Desc column: '{desc_col}', Prefix: {prefix_len} digits")

    # Build mapping lookup for descriptions
    description_map = {}
    if mapping_df is not None:
        try:
            cpi_mask = mapping_df['Update Name'].astype(str).str.contains('CPI by Group Total', case=False, na=False)
            cpi_df = mapping_df[cpi_mask]
            for _, row in cpi_df.iterrows():
                code = str(row.get('PRIMARY CONCEPT', '')).strip()
                desc = str(row.get('SECONDARY CONCEPT', '')).strip()
                if code and code != 'nan' and desc and desc != 'nan':
                    description_map[code] = desc
            print(f"[INFO] Loaded {len(description_map)} group descriptions from mapping")
        except Exception as e:
            print(f"[WARN] Could not load descriptions from mapping: {e}")

    # Store original data as metadata
    metadata = {
        'original_shape': df.shape,
        'original_columns': list(df.columns),
        'subcategory_mapping': {},
        'group_descriptions': {},
        'code_col': code_col,
        'desc_col': desc_col,
    }

    # Extract main group code (first N digits)
    df = df.copy()
    df['MainGroup'] = df[code_col].astype(str).str[:prefix_len]

    # Build subcategory mapping
    zero_prefix = '0' * prefix_len
    for main_group in df['MainGroup'].unique():
        if pd.notna(main_group) and main_group != zero_prefix:
            subcats = df[df['MainGroup'] == main_group][code_col].tolist()
            metadata['subcategory_mapping'][main_group] = subcats

    # Identify numeric columns
    numeric_cols = []
    for col in df.columns:
        if col not in [code_col, desc_col, 'MainGroup']:
            try:
                test_series = pd.to_numeric(df[col], errors='coerce')
                if test_series.notna().sum() > 0:
                    numeric_cols.append(col)
            except Exception:
                pass

    metadata['numeric_columns'] = numeric_cols
    metadata['concept_columns'] = [code_col, desc_col]

    # Group by main group and aggregate
    agg_dict = {code_col: 'first', desc_col: 'first'}
    for col in numeric_cols:
        agg_dict[col] = 'mean'

    df_agg = df.groupby('MainGroup', as_index=False).agg(agg_dict)

    # Filter out invalid groups
    df_agg = df_agg[df_agg['MainGroup'].notna()]
    df_agg = df_agg[df_agg['MainGroup'] != zero_prefix]

    # Update code to main group format (e.g., XX000000)
    suffix_zeros = '0' * (pad_len - prefix_len)
    df_agg[code_col] = df_agg['MainGroup'] + suffix_zeros

    # Use mapping descriptions if available
    if description_map:
        df_agg[desc_col] = df_agg[code_col].map(description_map).fillna(df_agg[desc_col])

    # Store group descriptions
    for _, row in df_agg.iterrows():
        metadata['group_descriptions'][row[code_col]] = row[desc_col]

    # Combine code and description into single ConceptID column
    df_agg.insert(0, 'ConceptID', df_agg.apply(
        lambda row: f"({row[code_col]}, {row[desc_col]})", axis=1
    ))

    # Drop the individual code, desc, and MainGroup columns
    df_agg = df_agg.drop(columns=[code_col, desc_col, 'MainGroup'])

    # Rename columns to match expected format: (ColumnName)
    new_columns = {}
    column_mapping = {}
    for col in df_agg.columns:
        if col == 'ConceptID':
            continue
        if 'Ponderaci n' in col or 'Weight' in col.title() or 'Ponderacao' in col:
            new_col = "(Ponderaci n)"
        else:
            new_col = f"({col})"
        column_mapping[col] = new_col
        new_columns[col] = new_col

    df_agg = df_agg.rename(columns=new_columns)

    metadata['column_mapping'] = column_mapping
    metadata['final_columns'] = list(df_agg.columns)
    metadata['aggregated_shape'] = df_agg.shape

    # Store metadata in the dataframe as an attribute
    df_agg.attrs['preprocessing_metadata'] = metadata

    print(f"[OK] Aggregated to {len(df_agg)} main groups")
    print(f"[INFO] New columns: {list(df_agg.columns)[:5]}... (showing first 5)")
    print(f"[SAVE] Metadata saved: {len(metadata['subcategory_mapping'])} main groups tracked")

    return df_agg


def save_intermediate_grid(df, output_path):
    """Saves the preprocessed tabular data to an Excel file and metadata to JSON."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_excel(output_path, index=False)
    
    # Save metadata if it exists
    if hasattr(df, 'attrs') and 'preprocessing_metadata' in df.attrs:
        import json
        metadata_path = output_path.replace('.xlsx', '_metadata.json')
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(df.attrs['preprocessing_metadata'], f, indent=2, ensure_ascii=False)
        print(f"[SAVE] Intermediate grid saved to: {output_path}")
        print(f"[SAVE] Metadata saved to: {metadata_path}")
    else:
        print(f"[SAVE] Intermediate grid saved to: {output_path}")
