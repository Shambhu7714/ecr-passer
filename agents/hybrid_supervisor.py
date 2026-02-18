"""
Enhanced Hybrid Supervisor - Orchestrates multi-track agentic parsing
Integrates all specialized agents and routing logic
"""

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
import os


class HybridSupervisor:
    """
    Enhanced supervisor with multi-track hybrid agentic architecture.
    
    Features:
    - Intelligent complexity-based routing (Fast/Hybrid/Full tracks)
    - Pattern caching for fast processing
    - Semantic validation
    - Hierarchy extraction
    - Quality auditing
    - Continuous learning feedback loop
    """
    
    def __init__(self, mapping_file, pattern_file=None, enable_hybrid=True):
        self.logger = get_logger()
        self.map_loader = MapLoader(mapping_file)
        self.intelligence = IntelligenceLayer(pattern_file)
        self.core = DeterministicCore()
        self.validator = Validator(map_loader=self.map_loader)
        self.writer = OutputWriter()
        
        # Hybrid Agentic Architecture Components
        self.enable_hybrid = enable_hybrid
        
        if self.enable_hybrid:
            self.logger.info("🚀 Initializing Hybrid Agentic Architecture")
            self.router = MultiTrackRouter(enable_cache=True, enable_feedback=True)
            self.semantic_validator = SemanticValidator()
            self.hierarchy_extractor = HierarchyExtractor()
            self.quality_auditor = QualityAuditor()
        else:
            self.router = None
            self.semantic_validator = None
            self.hierarchy_extractor = None
            self.quality_auditor = None
        
        # vector_mapper may be injected/initialized later
        self.vector_mapper = None
    
    def run_pipeline(self, input_file, base_year=2024):
        """
        Main entry point - routes to hybrid or legacy pipeline
        
        Args:
            input_file: Path to input Excel file
            base_year: Base year for date parsing (default 2024)
        """
        if self.enable_hybrid and self.router:
            return self.run_hybrid_pipeline(input_file, base_year)
        else:
            return self._run_legacy_pipeline(input_file, base_year)
    
    def run_hybrid_pipeline(self, input_file, base_year=2024):
        """
        Multi-track hybrid agentic pipeline with multi-sheet support
        
        Process Flow:
        1. Get list of sheets to extract (from TAB column in mapping)
        2. For each sheet:
           a. Preprocess Excel sheet
           b. Analyze complexity and route to appropriate track
           c. Process using track-specific pipeline
        3. Combine all sheet results
        4. Validate semantically
        5. Extract hierarchy
        6. Audit quality
        7. Learn from outcome
        """
        self.logger.info("=" * 70)
        self.logger.info("🚀 HYBRID AGENTIC PIPELINE")
        self.logger.info("=" * 70)

        base_name = os.path.splitext(os.path.basename(input_file))[0]

        # Step 1: Load mapping first to get TAB names
        self.logger.info("Step 1: Loading mapping configuration...")
        mapping = self.map_loader.load()
        self.logger.info(f"  Loaded {len(mapping)} series mappings")
        
        # Step 2: Get relevant sheets to extract
        self.logger.info("Step 2: Identifying sheets to extract...")
        tabs_from_mapping = self.map_loader.get_tabs_for_file(input_file)
        
        if not tabs_from_mapping or len(tabs_from_mapping) == 0:
            self.logger.warning("  ⚠️ No specific TAB names found in mapping. Processing first sheet only.")
            tabs = [None]  # Will read first/default sheet
        else:
            # Get actual sheet names from file for fuzzy matching
            import pandas as pd
            xl = pd.ExcelFile(input_file)
            actual_sheets = xl.sheet_names
            
            # Match mapping TABs to actual sheets (handle trailing spaces, case differences)
            tabs = []
            for tab_name in tabs_from_mapping:
                # Try exact match first
                if tab_name in actual_sheets:
                    tabs.append(tab_name)
                else:
                    # Try fuzzy match (strip spaces, case-insensitive)
                    tab_clean = tab_name.strip().lower()
                    matched = False
                    for actual_sheet in actual_sheets:
                        if actual_sheet.strip().lower() == tab_clean:
                            tabs.append(actual_sheet)
                            self.logger.info(f"  🔄 Matched '{tab_name}' → '{actual_sheet}'")
                            matched = True
                            break
                    
                    if not matched:
                        self.logger.warning(f"  ⚠️ Sheet '{tab_name}' not found in file. Skipping.")
            
            if not tabs:
                self.logger.warning("  ⚠️ No matching sheets found. Processing first sheet.")
                tabs = [None]
            
            self.logger.info(f"  📑 Will extract {len(tabs)} sheet(s): {tabs}")
        
        # Step 3: Process each sheet
        all_results = {}
        total_processing_time = 0
        total_cost = 0
        tracks_used = []
        
        for tab in tabs:
            sheet_label = tab if tab else "default"
            self.logger.info(f"\n{'='*70}")
            self.logger.info(f"📄 Processing Sheet: {sheet_label}")
            self.logger.info(f"{'='*70}")
            
            # Preprocess this specific sheet
            df_raw = preprocess_excel(input_file, sheet_name=tab)
            save_intermediate_grid(df_raw, f"logs/{base_name}_{sheet_label}_preprocessed.xlsx")
            self.logger.info(f"  Shape: {df_raw.shape}")
            
            # Get mappings relevant to this specific tab
            tab_metadata = {}
            for series_name, meta in self.map_loader.metadata.items():
                # Filter by both source file AND tab
                if tab:
                    if meta.get('tab', '') == tab:
                        tab_metadata[series_name] = meta
                else:
                    # If no tab specified, use all mappings for this file
                    tab_metadata = self.map_loader.get_mappings_for_file(input_file)
                    break
            
            self.logger.info(f"  📊 {len(tab_metadata)} mappings relevant for this sheet")
            
            # Prepare metadata
            metadata = {
                "base_year": base_year,
                "mapping_metadata": list(tab_metadata.values()),
                "mapping": mapping,
                "expected_series_count": len(tab_metadata),
                "source_file": input_file,
                "sheet_name": tab
            }
            
            # Multi-Track Routing & Processing
            processors = {
                "fast": lambda df, meta: self._fast_track_processor(df, meta, mapping, base_year, input_file),
                "hybrid": lambda df, meta: self._hybrid_track_processor(df, meta, mapping, base_year, input_file),
                "full": lambda df, meta: self._full_agentic_processor(df, meta, mapping, base_year, input_file)
            }
            
            result = self.router.route_and_process(
                file_path=input_file,
                df=df_raw,
                sheet_name=tab,
                metadata=metadata,
                processors=processors
            )
            
            if result.success:
                all_results.update(result.data)
                total_processing_time += result.processing_time
                total_cost += result.cost
                tracks_used.append(result.track_used)
                self.logger.info(f"✓ Sheet '{sheet_label}': {len(result.data)} series extracted")
            else:
                self.logger.error(f"❌ Sheet '{sheet_label}' failed on {result.track_used} track")
        
        # Combine results
        result_data = all_results
        self.logger.info(f"\n✓ Total extraction: {len(result_data)} series from {len(tabs)} sheet(s)")
        
        # Create combined result object (use last result as template)
        result.data = result_data
        result.processing_time = total_processing_time
        result.cost = total_cost
        
        # Step 4: Semantic Validation (hybrid/full tracks only)
        if result.track_used in ["hybrid", "full"] and self.semantic_validator:
            self.logger.info("Step 4: Semantic validation...")
            is_valid, validation_report = self.semantic_validator.validate(
                result_data, metadata
            )
            
            result.metadata["validation_report"] = validation_report
            
            if is_valid:
                self.logger.info(f"  ✓ Validation passed "
                               f"(confidence: {validation_report.get('confidence_score', 0):.2%})")
            else:
                self.logger.warning(f"  ⚠ Validation flagged {len(validation_report.get('issues_found', []))} issues")
        
        # Step 5: Hierarchy Extraction
        if self.hierarchy_extractor and self.map_loader.metadata:
            self.logger.info("Step 5: Extracting concept hierarchy...")
            concepts = list(self.map_loader.metadata.values())
            hierarchy = self.hierarchy_extractor.extract_hierarchy(concepts, metadata)
            result.metadata["hierarchy"] = hierarchy
            
            self.logger.info(f"  Type: {hierarchy.get('hierarchy_type')}")
            self.logger.info(f"  Levels: {len(hierarchy.get('levels', []))}")
            self.logger.info(f"  Root nodes: {len(hierarchy.get('root_nodes', []))}")
        
        # Step 6: Quality Audit
        if self.quality_auditor:
            self.logger.info("Step 6: Quality audit...")
            passed, audit_report = self.quality_auditor.audit(
                result_data,
                source_metadata=metadata,
                validation_results=result.metadata.get("validation_report")
            )
            
            result.quality_score = audit_report["overall_score"]
            result.metadata["audit_report"] = audit_report
            
            status = "PASSED ✓" if passed else "FAILED ✗"
            self.logger.info(f"  {status} (Score: {audit_report['overall_score']:.2%})")
            
            if audit_report.get("critical_issues"):
                self.logger.warning(f"  Critical issues: {len(audit_report['critical_issues'])}")
            
            if not passed:
                self.logger.info("\n" + self.quality_auditor.generate_report(audit_report))
        
        # Step 7: Traditional Validation (legacy compatibility)
        if not self.validator.validate(result_data, result.metadata.get("layout", {})):
            self.logger.error("❌ Traditional validation failed")
            return None

        # Step 8: Write Output
        self.logger.info("Step 7: Writing output...")
        output_path = self.writer.write(result_data, base_name)
        self.logger.info(f"  ✓ Output written to: {output_path}")

        # Step 9: Auto-save Pattern
        if result.success:
            self._auto_save_pattern(
                input_file, df_raw,
                result.metadata.get("layout", {}),
                mapping, result_data
            )
        
        # Step 10: Summary Statistics
        self.logger.info("=" * 70)
        self.logger.info("📊 PIPELINE SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info(f"  Track: {result.track_used.upper()}")
        self.logger.info(f"  Processing Time: {result.processing_time:.2f}s")
        self.logger.info(f"  Quality Score: {result.quality_score:.2%}")
        self.logger.info(f"  AI Cost: ${result.cost:.6f}")
        self.logger.info(f"  Series Extracted: {len(result_data)}")
        
        total_points = sum(len(s.get("values", {})) for s in result_data.values())
        self.logger.info(f"  Data Points: {total_points}")
        self.logger.info("=" * 70)
        
        # Log routing stats
        router_stats = self.router.get_statistics()
        self.logger.info("\n📈 ROUTING STATISTICS")
        self.logger.info(f"  Fast Track: {router_stats.get('fast_track_pct', 0):.1f}%")
        self.logger.info(f"  Hybrid Track: {router_stats.get('hybrid_track_pct', 0):.1f}%")
        self.logger.info(f"  Full Agentic: {router_stats.get('full_agentic_pct', 0):.1f}%")
        
        return output_path
    
    def _fast_track_processor(self, df, metadata, mapping, base_year, input_file=None):
        """
        Fast Track: Pure deterministic extraction
        - No AI inference
        - Pattern matching only
        - ~1-2 seconds
        """
        layout = {
            "orientation": "row-wise",
            "time_axis_index": 1,
            "concept_columns": list(range(1))
        }
        return self.core.process(df, layout, mapping, self.map_loader.metadata, base_year, source_file=input_file)
    
    def _hybrid_track_processor(self, df, metadata, mapping, base_year, input_file):
        """
        Hybrid Track: AI layout + Vector filtering + Deterministic
        - AI for layout detection
        - Vector semantic matching
        - Deterministic extraction
        - ~5-10 seconds
        """
        # AI layout detection
        sample_data = df.head(5).to_dict(orient='records')
        layout = self.intelligence.layout_detection(list(df.columns), sample_data)
        
        # Vector-based pattern matching (if available)
        if self.vector_mapper:
            pattern_info = self.intelligence.pattern_matcher(
                layout, list(df.columns), sample_data,
                vector_mapper=self.vector_mapper,
                source_file=input_file
            )
            if isinstance(pattern_info, dict):
                layout.update(pattern_info)
        
        # Deterministic extraction
        return self.core.process(df, layout, mapping, self.map_loader.metadata, base_year, source_file=input_file)
    
    def _full_agentic_processor(self, df, metadata, mapping, base_year, input_file):
        """
        Full Agentic Track: Complete AI pipeline
        - Full intelligence layer
        - Vector semantic matching
        - AI-guided extraction
        - ~20-30 seconds
        """
        sample_data = df.head(5).to_dict(orient='records')

        # AI layout detection
        layout = self.intelligence.layout_detection(list(df.columns), sample_data)

        # AI pattern matching
        pattern_info = self.intelligence.pattern_matcher(
            layout, list(df.columns), sample_data,
            vector_mapper=self.vector_mapper,
            source_file=input_file
        )
        if isinstance(pattern_info, dict):
            layout.update(pattern_info)
        
        # Filter mapping
        mapping_to_use, metadata_to_use = self._filter_mapping_metadata(df, layout, mapping)
        
        # Deterministic extraction
        return self.core.process(df, layout, mapping_to_use, metadata_to_use, base_year, source_file=input_file)
    
    def _filter_mapping_metadata(self, df_raw, layout, mapping):
        """Filter mapping to relevant entries based on sheet concepts"""
        try:
            # Determine candidate concept values from sheet
            candidate_vals = set()
            time_idx = layout.get('time_axis_index', None) if isinstance(layout, dict) else None
            
            if isinstance(time_idx, int) and time_idx > 0:
                cols = list(range(0, min(time_idx, df_raw.shape[1])))
            else:
                cols = list(range(0, min(3, df_raw.shape[1])))

            for c in cols:
                try:
                    vals = df_raw.iloc[:, c].dropna().astype(str).str.strip().unique().tolist()
                    candidate_vals.update([v for v in vals if v and v.lower() != 'nan'])
                except Exception:
                    continue

            # Include all mapping concepts
            mapping_concepts = set()
            for meta in self.map_loader.metadata.values():
                for k in ('primary_concept', 'secondary_concept', 'third_concept', 'fourth_concept'):
                    v = str(meta.get(k, '')).strip()
                    if v and v.lower() != 'nan':
                        mapping_concepts.add(v)
            candidate_vals.update(mapping_concepts)

            # Filter mappings
            filtered_mapping = {}
            filtered_metadata = {}
            
            for key, series_code in mapping.items():
                meta = self.map_loader.metadata.get(key, {})
                primary = str(meta.get('primary_concept', '')).strip()
                secondary = str(meta.get('secondary_concept', '')).strip()
                third = str(meta.get('third_concept', '')).strip()

                # Keep if any concept matches
                if (primary and primary in candidate_vals) or \
                   (secondary and secondary in candidate_vals) or \
                   (third and third in candidate_vals):
                    filtered_mapping[key] = series_code
                    filtered_metadata[key] = meta

            # Deduplicate by series_code
            if filtered_mapping:
                deduped = {}
                dedup_meta = {}
                by_series = {}
                
                for k, v in filtered_mapping.items():
                    meta = filtered_metadata.get(k, {})
                    scode = str(v)
                    prim = str(meta.get('primary_concept', '')).strip()
                    pnorm = prim.strip().lower()
                    
                    if scode not in by_series:
                        by_series[scode] = (k, meta, pnorm)
                    else:
                        prev_k, prev_meta, prev_pnorm = by_series[scode]
                        prev_score = 1 if prev_pnorm in candidate_vals else 0
                        new_score = 1 if pnorm in candidate_vals else 0
                        
                        if new_score > prev_score:
                            by_series[scode] = (k, meta, pnorm)

                for scode, (k, meta, _) in by_series.items():
                    deduped[k] = mapping[k]
                    dedup_meta[k] = meta

                return deduped, dedup_meta
            
            return mapping, self.map_loader.metadata
            
        except Exception as e:
            self.logger.warning(f"Mapping filter error: {e}")
            return mapping, self.map_loader.metadata
    
    def _run_legacy_pipeline(self, input_file, base_year=2024):
        """Legacy pipeline for backwards compatibility"""
        self.logger.info("Running legacy pipeline...")

        base_name = os.path.splitext(os.path.basename(input_file))[0]

        # Preprocess
        df_raw = preprocess_excel(input_file)
        save_intermediate_grid(df_raw, f"logs/{base_name}_preprocessed.xlsx")

        # Load mapping
        mapping = self.map_loader.load()

        # Intelligence layer
        sample_data = df_raw.head(5).to_dict(orient='records')
        layout = self.intelligence.layout_detection(list(df_raw.columns), sample_data)

        pattern_info = self.intelligence.pattern_matcher(
            layout, list(df_raw.columns), sample_data,
            vector_mapper=self.vector_mapper,
            source_file=input_file
        )
        if isinstance(pattern_info, dict):
            layout.update(pattern_info)

        # Filter mapping
        mapping_to_use, metadata_to_use = self._filter_mapping_metadata(
            df_raw, layout, mapping
        )

        # Deterministic processing
        result_data = self.core.process(df_raw, layout, mapping_to_use, metadata_to_use, base_year)

        # Validate
        if not self.validator.validate(result_data, layout):
            self.logger.error("Validation failed")
            return None

        # Write output
        output_path = self.writer.write(result_data, base_name)

        # Auto-save pattern
        self._auto_save_pattern(input_file, df_raw, layout, mapping, result_data)

        return output_path
    
    def _auto_save_pattern(self, input_file, df_raw, layout, mapping, result_data):
        """Auto-save successful patterns to pattern library"""
        try:
            base_name = os.path.splitext(os.path.basename(input_file))[0]
            headers = list(df_raw.columns)
            
            # Detect concept structure
            time_axis_index = layout.get('time_axis_index', 3)
            num_concepts = 0
            index_col_desc = None
            
            for i, col in enumerate(headers[:time_axis_index]):
                col_lower = str(col).lower()
                if 'ponderación' in col_lower or 'weight' in col_lower or 'índice' in col_lower:
                    index_col_desc = f"Column {i} - {col} (metadata), NORMAL DATA"
                else:
                    num_concepts += 1
            
            # Build concept descriptions
            concepts = []
            concept_labels = ["Primary", "Secondary", "Third", "Fourth"]
            
            for i in range(num_concepts):
                if i < len(headers):
                    concepts.append(f"{concept_labels[i]} Concept (Column {i} - {headers[i]}): NORMAL DATA")
            
            concepts_desc = ". ".join(concepts) + "."
            
            # Time series description
            time_cols = headers[time_axis_index:]
            time_series_desc = f"Columns {time_axis_index}-{len(headers)-1} ({len(time_cols)} columns), TIME SERIES DATA"
            
            # Get update name from metadata
            update_name = None
            if result_data:
                first_series_code = next(iter(result_data.keys()))
                
                for meta_key, meta in self.map_loader.metadata.items():
                    if meta.get('series_code') == first_series_code:
                        update_name = meta.get('update_name')
                        break
            
            if not update_name:
                if self.map_loader.metadata:
                    first_key = next(iter(self.map_loader.metadata.keys()))
                    if '_' in first_key:
                        update_name = first_key.rsplit('_', 1)[0]
            
            if not update_name:
                update_name = base_name
            
            # Save pattern
            orientation = layout.get('orientation', 'row-wise')
            self.intelligence.pattern_lib.add_pattern(
                pattern_name=base_name,
                update_name=update_name,
                num_concepts=num_concepts,
                concepts_desc=concepts_desc,
                index_col=index_col_desc,
                time_series_cols=time_series_desc,
                orientation=orientation
            )
            
            self.logger.info(f"✓ Pattern saved: {base_name}")
            
        except Exception as e:
            self.logger.warning(f"⚠ Could not auto-save pattern: {str(e)}")
