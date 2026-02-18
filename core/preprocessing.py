import pandas as pd
import openpyxl
import os
import re

def preprocess_excel(file_path, sheet_name=None):
    """
    Reads raw Excel, handles merged cells, and prepares a clean grid.
    Removes header rows, footers, and extracts only the data table.
    Flattens multi-level headers properly.
    
    Args:
        file_path: Path to Excel file
        sheet_name: Specific sheet name to read (if None, reads first sheet)
    """
    sheet_info = f" (Sheet: {sheet_name})" if sheet_name else ""
    print(f"\n{'='*60}")
    print(f"🔍 STEP 1: PREPROCESSING - {file_path}{sheet_info}")
    print(f"{'='*60}")
    
    # Load mapping file for group descriptions
    mapping_df = None
    mapping_file = 'config/Argentina_Map_Updated.xlsx'
    if os.path.exists(mapping_file):
        try:
            mapping_df = pd.read_excel(mapping_file, sheet_name='Mapping Rules + Checks', engine='openpyxl')
            print(f"📂 Loaded mapping file for group descriptions")
        except:
            print(f"⚠️ Could not load mapping file, will use input descriptions")
    
    # If sheet_name is None, pandas reads all sheets as a dict. 
    # We want either a specific sheet or the first sheet (0).
    effective_sheet = sheet_name if sheet_name is not None else 0
    
    # Load with openpyxl to handle merged cells better if needed
    df = pd.read_excel(file_path, sheet_name=effective_sheet, header=None)
    print(f"📥 Loaded raw file with shape: {df.shape}")
    
    # Find the year (usually appears a few rows before the header)
    year = None
    for idx in range(min(10, len(df))):
        for val in df.iloc[idx].values:
            if pd.notna(val) and str(val).isdigit() and len(str(val)) == 4 and str(val).startswith('20'):
                year = str(val)
                print(f"📅 Found year: {year} at row {idx}")
                break
        if year:
            break
    
    # Find the actual data table start (look for row with "Código" or similar)
    header_row_idx = None
    for idx, row in df.iterrows():
        row_str = ' '.join([str(x) for x in row.values if pd.notna(x)])
        if 'Código' in row_str or 'División' in row_str or 'Ponderación' in row_str:
            header_row_idx = idx
            print(f"✅ Found header row at index: {idx}")
            break
    
    if header_row_idx is None:
        print("⚠️ Could not find header row. Using first row as header.")
        header_row_idx = 0
    
    # Check if there's a second header row (months, quarters, etc.)
    second_header_idx = header_row_idx + 1
    has_second_header = False
    
    if second_header_idx < len(df):
        second_row = df.iloc[second_header_idx]
        # Check if this row contains month names or time labels
        time_keywords = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 
                        'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre',
                        'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec',
                        'q1', 'q2', 'q3', 'q4', 'trimestre']
        
        for val in second_row.values:
            if pd.notna(val) and any(kw in str(val).lower() for kw in time_keywords):
                has_second_header = True
                print(f"✅ Found second header row (time periods) at index: {second_header_idx}")
                break
    
    # Read the data with proper multi-level headers
    if has_second_header:
        # Read both header rows
        df_clean = pd.read_excel(file_path, sheet_name=sheet_name, header=[header_row_idx, second_header_idx])
        
        # Flatten the multi-index columns properly
        new_columns = []
        for col in df_clean.columns:
            if isinstance(col, tuple):
                # Remove 'Unnamed' entries and NaN
                parts = [str(c).strip() for c in col if 'Unnamed' not in str(c) and str(c) != 'nan' and str(c).strip() != '']
                
                # If we have both parts (e.g., "Índice" and "Enero"), add year
                if len(parts) == 2:
                    if year:
                        new_col = f"{parts[0]}, {parts[1]}, {year}"
                    else:
                        new_col = ', '.join(parts)
                elif len(parts) == 1:
                    # Just one part (e.g., "Código")
                    new_col = parts[0]
                else:
                    # Fallback
                    new_col = ', '.join(parts) if parts else 'Column'
            else:
                new_col = str(col)
            
            new_columns.append(new_col)
        
        df_clean.columns = new_columns
        print(f"✅ Flattened headers: {list(df_clean.columns)[:5]}... (showing first 5)")
    else:
        # Single header row
        df_clean = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row_idx)
    
    # Keep Código column as string with leading zeros (if it exists and is numeric)
    if 'Código' in df_clean.columns:
        # Convert to string and pad with leading zeros to 8 digits
        df_clean['Código'] = df_clean['Código'].astype(str).str.zfill(8)
        print(f"✅ Normalized Código column to 8-digit strings")
    
    # Handle merged cells (propagate values)
    df_clean = df_clean.ffill(axis=0).ffill(axis=1)
    
    # Remove completely empty rows/cols
    df_clean = df_clean.dropna(how='all').dropna(how='all', axis=1)
    
    # Remove footer rows (rows that start with "Fuente:", "Nota:", etc.)
    footer_keywords = ['Fuente:', 'Nota:', 'FUENTE:', 'NOTA:', '*', 'Trimestre']
    for idx, row in df_clean.iterrows():
        first_cell = str(row.iloc[0]) if len(row) > 0 else ''
        if any(keyword in first_cell for keyword in footer_keywords):
            df_clean = df_clean.iloc[:idx]
            print(f"✂️ Removed footer rows starting at index: {idx}")
            break
    
    # Remove any remaining rows with NaN in the code column
    if 'Código' in df_clean.columns:
        original_len = len(df_clean)
        df_clean = df_clean[df_clean['Código'].notna()]
        if len(df_clean) < original_len:
            print(f"✂️ Removed {original_len - len(df_clean)} rows with NaN códigos")
    
    print(f"✅ Preprocessing complete. Clean shape: {df_clean.shape}")
    print(f"📋 Columns: {list(df_clean.columns)[:5]}... (showing first 5)")
    
    # Aggregate subcategories to main groups if Código column exists
    if 'Código' in df_clean.columns and 'Grupo' in df_clean.columns:
        df_clean = _aggregate_to_main_groups(df_clean, mapping_df)
    
    return df_clean

def _aggregate_to_main_groups(df, mapping_df=None):
    """
    Aggregates subcategories to main groups based on first 2 digits of código.
    Averages numeric values (Ponderación and time series data).
    Uses mapping file to get correct group descriptions.
    Returns aggregated dataframe with combined (CODE, DESCRIPTION) format
    and saves metadata for downstream processing.
    """
    print(f"\n🔄 Aggregating {len(df)} subcategories to main groups...")
    
    # Build mapping lookup for descriptions
    description_map = {}
    if mapping_df is not None:
        # Filter for CPI by Group Total
        cpi_mask = mapping_df['Update Name'].astype(str).str.contains('CPI by Group Total', case=False, na=False)
        cpi_df = mapping_df[cpi_mask]
        
        for _, row in cpi_df.iterrows():
            code = str(row.get('PRIMARY CONCEPT', '')).strip()
            desc = str(row.get('SECONDARY CONCEPT', '')).strip()
            if code and code != 'nan' and desc and desc != 'nan':
                description_map[code] = desc
        print(f"📚 Loaded {len(description_map)} group descriptions from mapping")
    
    # Store original data as metadata
    metadata = {
        'original_shape': df.shape,
        'original_columns': list(df.columns),
        'subcategory_mapping': {},  # Will map main group -> list of subcategories
        'group_descriptions': {}     # Will map main group code -> description
    }
    
    # Extract main group code (first 2 digits)
    df['MainGroup'] = df['Código'].str[:2]
    
    # Build subcategory mapping
    for main_group in df['MainGroup'].unique():
        if pd.notna(main_group) and main_group != '00':  # Exclude invalid "00" group
            subcats = df[df['MainGroup'] == main_group]['Código'].tolist()
            metadata['subcategory_mapping'][main_group] = subcats
    
    # Identify numeric columns (time series and Ponderación)
    numeric_cols = []
    for col in df.columns:
        if col not in ['Código', 'Grupo', 'MainGroup']:
            # Check if column has numeric data
            try:
                test_series = pd.to_numeric(df[col], errors='coerce')
                if test_series.notna().sum() > 0:  # Has at least some numeric values
                    numeric_cols.append(col)
            except:
                pass
    
    metadata['numeric_columns'] = numeric_cols
    metadata['concept_columns'] = ['Código', 'Grupo']
    
    # Group by main group and aggregate
    # For Grupo, take the first value (they should be similar within a group)
    # For numeric columns, take the mean
    agg_dict = {'Código': 'first', 'Grupo': 'first'}
    for col in numeric_cols:
        agg_dict[col] = 'mean'
    
    df_agg = df.groupby('MainGroup', as_index=False).agg(agg_dict)
    
    # Filter out any groups with NaN MainGroup or "00" (invalid codes)
    df_agg = df_agg[df_agg['MainGroup'].notna()]
    df_agg = df_agg[df_agg['MainGroup'] != '00']
    
    # Update código to main group format (XX000000) and get proper descriptions
    df_agg['Código'] = df_agg['MainGroup'] + '000000'
    
    # Use mapping descriptions if available, otherwise use aggregated description
    if description_map:
        df_agg['Grupo'] = df_agg['Código'].map(description_map).fillna(df_agg['Grupo'])
    
    # Store group descriptions
    for _, row in df_agg.iterrows():
        metadata['group_descriptions'][row['Código']] = row['Grupo']
    
    # Combine Código and Grupo into single column in first position
    df_agg.insert(0, 'ConceptID', df_agg.apply(
        lambda row: f"({row['Código']}, {row['Grupo']})", axis=1
    ))
    
    # Drop the individual Código, Grupo, and MainGroup columns
    df_agg = df_agg.drop(columns=['Código', 'Grupo', 'MainGroup'])
    
    # Rename columns to match expected format: (ColumnName) with year preserved
    new_columns = {}
    column_mapping = {}  # Track original -> new column names
    
    for col in df_agg.columns:
        if col == 'ConceptID':
            continue
        # Keep year in column names (e.g., "Índice, Enero, 2025" -> "(Índice, Enero, 2025)")
        if 'Ponderación' in col:
            new_col = "(Ponderación)"
            column_mapping[col] = new_col
        else:
            # Wrap entire column name in parentheses
            new_col = f"({col})"
            column_mapping[col] = new_col
        new_columns[col] = new_col
    
    df_agg = df_agg.rename(columns=new_columns)
    
    metadata['column_mapping'] = column_mapping
    metadata['final_columns'] = list(df_agg.columns)
    metadata['aggregated_shape'] = df_agg.shape
    
    # Store metadata in the dataframe as an attribute
    df_agg.attrs['preprocessing_metadata'] = metadata
    
    print(f"✅ Aggregated to {len(df_agg)} main groups")
    print(f"📋 New columns: {list(df_agg.columns)[:5]}... (showing first 5)")
    print(f"💾 Metadata saved: {len(metadata['subcategory_mapping'])} main groups tracked")
    
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
        print(f"💾 Intermediate grid saved to: {output_path}")
        print(f"💾 Metadata saved to: {metadata_path}")
    else:
        print(f"💾 Intermediate grid saved to: {output_path}")
