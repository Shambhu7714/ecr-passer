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
from agents.supervisor import Supervisor

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
        self.quality_auditor = QualityAuditor()
        self.supervisor = Supervisor(mapping_file, pattern_file, enable_hybrid)

    def run_pipeline(self, input_file, base_year=2024):
        self.logger.info(f"[START] Processing {input_file}")
        self.map_loader.load()
        xl = pd.ExcelFile(input_file)
        actual_sheets = xl.sheet_names
        tabs_from_mapping = self.map_loader.get_tabs_for_file(input_file)
        if not tabs_from_mapping:
            tabs_to_process = [None]
            sheet_to_tab = {None: None}
        else:
            tabs_to_process = []
            sheet_to_tab = {}
            for t in tabs_from_mapping:
                t_clean = str(t).strip().lower()
                match = next((s for s in actual_sheets if s.strip().lower() == t_clean), None)
                if match:
                    tabs_to_process.append(match)
                    sheet_to_tab[match] = t
        if not tabs_to_process: tabs_to_process = [None]
        all_results = {}
        for sheet in tabs_to_process:
            self.logger.info(f"Processing sheet: {sheet}")
            if self.enable_hybrid:
                res_data = self.supervisor.process_sheet(input_file, base_year, sheet_name=sheet)
                all_results.update(res_data)
            else:
                df = preprocess_excel(input_file, sheet_name=sheet, mapping_file=self.mapping_file)
                res = self.core.process(df, {"time_axis_index": 1}, {}, self.map_loader.get_mappings_for_file(input_file), base_year, input_file, self.map_loader.metadata)
                all_results.update(res)
        return self.writer.write(all_results, os.path.splitext(os.path.basename(input_file))[0])
