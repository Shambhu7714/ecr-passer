from core.preprocessing import preprocess_excel, save_intermediate_grid
from core.map_loader import MapLoader
from core.intelligence import IntelligenceLayer
from core.deterministic_core import DeterministicCore
from core.output_writer import OutputWriter
from core.validation import Validator
from core.logger import get_logger
from core.multi_track_router import MultiTrackRouter
from agents.semantic_validator import SemanticValidator
from agents.hierarchy_extractor import HierarchyExtractor
from agents.quality_auditor import QualityAuditor
from agents.mapping_agent import MappingAgent
import os
import pandas as pd

class Supervisor:
    def __init__(self, mapping_file, pattern_file=None, enable_hybrid=True):
        self.logger = get_logger()
        self.map_loader = MapLoader(mapping_file)
        self.intelligence = IntelligenceLayer(pattern_file)
        self.core = DeterministicCore()
        self.validator = Validator(map_loader=self.map_loader)
        self.writer = OutputWriter()
        self.enable_hybrid = enable_hybrid
        if self.enable_hybrid:
            self.router = MultiTrackRouter(enable_cache=True, enable_feedback=True)
            self.mapping_agent = MappingAgent()
        else:
            self.router = None
            self.mapping_agent = None

    def run_pipeline(self, input_file, base_year=2024, sheet_name=None):
        if self.enable_hybrid and self.router:
            return self.run_hybrid_pipeline(input_file, base_year, sheet_name)
        df_raw = preprocess_excel(input_file, sheet_name=sheet_name)
        mapping = self.map_loader.load()
        return self.core.process(df_raw, {"time_axis_index": 1}, mapping, self.map_loader.metadata, base_year)

    def _get_relevant_mapping(self, input_file, sheet_name=None):
        """
        Gets mappings strictly related to the input file and, if provided, the specific sheet.
        This prevents cross-file and cross-tab false positives.
        """
        all_file_meta = self.map_loader.get_mappings_for_file(input_file)
        
        if not all_file_meta:
            self.logger.warning(f"No mapping found for file: {input_file}")
            return {}, {}

        # If we have a specific sheet, filter mapping entries to that TAB
        if sheet_name:
            sheet_norm = str(sheet_name).strip().lower()
            filtered_meta = {}
            for k, v in all_file_meta.items():
                tab_val = str(v.get('tab', '')).strip().lower()
                # Accept exact match or subset match for tab names
                if tab_val == sheet_norm or sheet_norm in tab_val or tab_val in sheet_norm:
                    filtered_meta[k] = v
            
            if filtered_meta:
                self.logger.info(f"Filtered to {len(filtered_meta)} mapping entries for sheet '{sheet_name}'")
                mapping_data = {k: v.get('series_code', k) for k, v in filtered_meta.items()}
                return mapping_data, filtered_meta
            else:
                self.logger.warning(f"No mapping entries found specifically for tab '{sheet_name}' in {input_file}")
        
        # Fallback to all file entries if no sheet-specific filtering possible
        mapping_data = {k: v.get('series_code', k) for k, v in all_file_meta.items()}
        return mapping_data, all_file_meta

    def run_hybrid_pipeline(self, input_file, base_year=2024, sheet_name=None):
        df_raw = preprocess_excel(input_file, sheet_name=sheet_name)
        mapping, metadata_dict = self._get_relevant_mapping(input_file, sheet_name)
        metadata = {"base_year": base_year, "mapping_metadata": list(metadata_dict.values()), "mapping": mapping}
        
        processors = {
            "fast": lambda df, meta: self.core.process(df, {"orientation": "row-wise", "time_axis_index": 1}, mapping, metadata_dict, base_year),
            "hybrid": lambda df, meta: self._hybrid_track_processor(df, meta, mapping, metadata_dict, base_year, input_file),
            "full": lambda df, meta: self._hybrid_track_processor(df, meta, mapping, metadata_dict, base_year, input_file)
        }
        result = self.router.route_and_process(file_path=input_file, df=df_raw, sheet_name=sheet_name, metadata=metadata, processors=processors)
        return self.writer.write(result.data, os.path.splitext(os.path.basename(input_file))[0])

    def process_sheet(self, input_file, base_year=2024, sheet_name=None):
        df_raw = preprocess_excel(input_file, sheet_name=sheet_name)
        mapping, metadata_dict = self._get_relevant_mapping(input_file, sheet_name)
        
        if not mapping:
             return {}

        metadata = {"base_year": base_year, "mapping_metadata": list(metadata_dict.values()), "mapping": mapping}
        processors = {
            "fast": lambda df, meta: self.core.process(df, {"orientation": "row-wise", "time_axis_index": 1}, mapping, metadata_dict, base_year),
            "hybrid": lambda df, meta: self._hybrid_track_processor(df, meta, mapping, metadata_dict, base_year, input_file),
            "full": lambda df, meta: self._hybrid_track_processor(df, meta, mapping, metadata_dict, base_year, input_file)
        }
        result = self.router.route_and_process(file_path=input_file, df=df_raw, sheet_name=sheet_name, metadata=metadata, processors=processors)
        return result.data if result.success else {}

    def _hybrid_track_processor(self, df, metadata, mapping, metadata_dict, base_year, input_file):
        sample_data = df.head(5).to_dict(orient='records')
        layout = self.intelligence.layout_detection(list(df.columns), sample_data)
        
        if self.mapping_agent:
            self.logger.info("Using MappingAgent for semantic reasoning...")
            col_labels = [str(c) for c in df.columns if "Unnamed" not in str(c)]
            row_labels = df.iloc[:, 0].dropna().unique().tolist()[:20]
            title_labels = df.iloc[:5, :].values.flatten().tolist()
            title_labels = [str(t) for t in title_labels if pd.notna(t) and len(str(t)) > 5]
            all_labels = list(set(col_labels + row_labels + title_labels))[:50]
            
            from core.country_config import CountryConfigLoader
            country_code = CountryConfigLoader().detect_and_load(input_file).get("country_code")
            
            metadata["reasoned_mappings"] = self.mapping_agent.reason_mappings(
                all_labels, 
                metadata_dict, # Use the already filtered metadata!
                context={"filename": input_file},
                country_filter=country_code
            )
        return self.core.process(df, layout, mapping, metadata_dict, base_year, input_file, metadata)
