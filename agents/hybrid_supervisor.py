import os
import pandas as pd
from core.preprocessing import preprocess_excel, save_intermediate_grid
from core.map_loader import MapLoader
from core.deterministic_core import DeterministicCore
from core.output_writer import OutputWriter
from core.validation import Validator
from core.logger import get_logger
from core.multi_track_router import MultiTrackRouter
from agents.semantic_validator import SemanticValidator
from agents.hierarchy_extractor import HierarchyExtractor
from agents.quality_auditor import QualityAuditor

class HybridSupervisor:
    def __init__(self, mapping_file, pattern_file=None, enable_hybrid=True):
        self.logger = get_logger()
        self.mapping_file = mapping_file
        self.map_loader = MapLoader(mapping_file)
        self.core = DeterministicCore()
        self.writer = OutputWriter()
        self.validator = Validator(map_loader=self.map_loader)
        self.enable_hybrid = enable_hybrid
        self.router = MultiTrackRouter(enable_cache=True) if enable_hybrid else None
        self.semantic_validator = SemanticValidator()
        self.hierarchy_extractor = HierarchyExtractor()
        self.quality_auditor = QualityAuditor()

    def run_pipeline(self, input_file, base_year=2024):
        self.logger.info(f"[START] Processing {input_file}")
        self.map_loader.load()
        
        # Get actual sheets from file
        import pandas as pd
        xl = pd.ExcelFile(input_file)
        actual_sheets = xl.sheet_names
        
        # Get matching tabs from mapping
        tabs_from_mapping = self.map_loader.get_tabs_for_file(input_file)
        
        if not tabs_from_mapping:
            tabs_to_process = [None]
            sheet_to_tab = {None: None}
        else:
            tabs_to_process = []
            sheet_to_tab = {}
            for t in tabs_from_mapping:
                t_clean = str(t).strip().lower()
                # Find best match in actual sheets
                match = None
                for s in actual_sheets:
                    if s.strip().lower() == t_clean:
                        match = s
                        break
                if match:
                    tabs_to_process.append(match)
                    sheet_to_tab[match] = t
                else:
                    self.logger.warning(f"TAB '{t}' defined in mapping not found in {input_file}")
        
        if not tabs_to_process:
            tabs_to_process = [None]

        all_results = {}
        for sheet in tabs_to_process:
            self.logger.info(f"Processing sheet: {sheet}")
            df = preprocess_excel(input_file, sheet_name=sheet, mapping_file=self.mapping_file)
            
            # Filter metadata for this tab
            tab_metadata = {}
            file_meta = self.map_loader.get_mappings_for_file(input_file)
            target_tab = sheet_to_tab.get(sheet)
            target_tab_norm = str(target_tab).strip().lower() if target_tab else None
            
            for k, v in file_meta.items():
                m_tab = str(v.get('tab', '')).strip().lower()
                if target_tab_norm and m_tab == target_tab_norm:
                    tab_metadata[k] = v
                elif not target_tab_norm:
                    tab_metadata[k] = v
            
            # If still empty, fall back to all file mappings
            if not tab_metadata:
                tab_metadata = file_meta
                    
            if not tab_metadata:
                self.logger.warning(f"No metadata found for file '{input_file}'")
                continue

            layout = {"time_axis_index": 1} # Placeholder
            
            # Deterministic processing
            res = self.core.process(df, layout, {}, tab_metadata, base_year, input_file, self.map_loader.metadata)
            all_results.update(res)

        output_path = self.writer.write(all_results, os.path.splitext(os.path.basename(input_file))[0])
        self.logger.info(f"[DONE] Output: {output_path}")
        return output_path
