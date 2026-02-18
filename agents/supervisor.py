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

class ParserSupervisor:
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
        
        # vector_mapper may be injected/initialized later; ensure attribute exists
        self.vector_mapper = None

    def run_pipeline(self, input_file, base_year = 2024):
        """
        Orchestrates the parsing pipeline with optional hybrid routing

        Args:
            input_file (str): Path to the input Excel file
        """
        self.logger.info("Starting the parsing pipeline...")
        
        # Use hybrid pipeline if enabled
        if self.enable_hybrid and self.router:
            return self.run_hybrid_pipeline(input_file, base_year)
        
        # Otherwise use legacy pipeline
        return self._run_legacy_pipeline(input_file, base_year)
    
    def run_hybrid_pipeline(self, input_file, base_year=2024):
        """
        NEW: Multi-track hybrid agentic pipeline with intelligent routing
        
        Implements:
        - Complexity analysis and track routing
        - Cache-based fast track (70%)
        - Vector + AI hybrid track (25%)
        - Full agentic track (5%)
        - Semantic validation
        - Hierarchy extraction
        - Quality auditing
        - Feedback loop learning
        """
        self.logger.info("🚀 Running Hybrid Agentic Pipeline")

        base_name = os.path.splitext(os.path.basename(input_file))[0]

        # Step 1: Preprocess the input Excel file
        df_raw = preprocess_excel(input_file)
        save_intermediate_grid(df_raw, f"logs/{base_name}_preprocessed.xlsx")

        # Step 2: Load mapping configuration
        mapping = self.map_loader.load()
        
        # Prepare metadata for routing
        metadata = {
            "base_year": base_year,
            "mapping_metadata": list(self.map_loader.metadata.values()),
            "mapping": mapping
        }
        
        # Step 3: Multi-Track Routing & Processing
        processors = {
            "fast": lambda df, meta: self._fast_track_processor(df, meta, mapping, base_year),
            "hybrid": lambda df, meta: self._hybrid_track_processor(df, meta, mapping, base_year, input_file),
            "full": lambda df, meta: self._full_agentic_processor(df, meta, mapping, base_year, input_file)
        }
        
        result = self.router.route_and_process(
            file_path=input_file,
            df=df_raw,
            sheet_name=None,
            metadata=metadata,
            processors=processors
        )
        
        if not result.success:
            self.logger.error(f"Processing failed on {result.track_used} track")
            return None
        
        result_data = result.data
        
        # Step 4: Semantic Validation (if full or hybrid track)
        if result.track_used in ["hybrid", "full"] and self.semantic_validator:
            self.logger.info("Running semantic validation...")
            is_valid, validation_report = self.semantic_validator.validate(
                result_data, metadata
            )
            
            if not is_valid:
                self.logger.warning(f"Semantic validation flagged issues: "
                                   f"Confidence={validation_report.get('confidence_score', 0):.2f}")
            
            result.metadata["validation_report"] = validation_report
        
        # Step 5: Hierarchy Extraction (if applicable)
        if self.hierarchy_extractor and self.map_loader.metadata:
            self.logger.info("Extracting concept hierarchy...")
            concepts = list(self.map_loader.metadata.values())
            hierarchy = self.hierarchy_extractor.extract_hierarchy(concepts, metadata)
            result.metadata["hierarchy"] = hierarchy
            
            self.logger.info(f"Hierarchy: {hierarchy.get('hierarchy_type')}, "
                           f"{len(hierarchy.get('levels', []))} levels")
        
        # Step 6: Quality Audit
        if self.quality_auditor:
            self.logger.info("Running quality audit...")
            passed, audit_report = self.quality_auditor.audit(
                result_data,
                source_metadata=metadata,
                validation_results=result.metadata.get("validation_report")
            )
            
            result.quality_score = audit_report["overall_score"]
            result.metadata["audit_report"] = audit_report
            
            self.logger.info(f"Quality Audit: {'PASSED ✓' if passed else 'FAILED ✗'} "
                           f"(Score: {audit_report['overall_score']:.2%})")
            
            # Print audit summary
            if not passed:
                self.logger.warning("Quality audit report:")
                self.logger.warning(self.quality_auditor.generate_report(audit_report))
        
        # Step 7: Traditional Validation
        if not self.validator.validate(result_data, result.metadata.get("layout", {})):
            self.logger.error("Traditional validation failed. Check logs for details.")
            return None

        # Step 8: Write output to Excel
        output_path = self.writer.write(result_data, base_name)

        # Step 9: Auto-save pattern if successful
        if result.success:
            self._auto_save_pattern(input_file, df_raw, 
                                   result.metadata.get("layout", {}), 
                                   mapping, result_data)
        
        # Step 10: Log statistics
        self.logger.info("=" * 70)
        self.logger.info("Pipeline Statistics:")
        self.logger.info(f"  Track Used: {result.track_used}")
        self.logger.info(f"  Processing Time: {result.processing_time:.2f}s")
        self.logger.info(f"  Quality Score: {result.quality_score:.2%}")
        self.logger.info(f"  Cost: ${result.cost:.6f}")
        self.logger.info(f"  Series Extracted: {len(result_data)}")
        self.logger.info("=" * 70)
        
        return output_path
    
    def _fast_track_processor(self, df, metadata, mapping, base_year):
        """Fast track: Pure deterministic extraction"""
        layout = {"orientation": "row-wise", "time_axis_index": 1}
        return self.core.process(df, layout, mapping, self.map_loader.metadata, base_year)
    
    def _hybrid_track_processor(self, df, metadata, mapping, base_year, input_file):
        """Hybrid track: AI layout + Vector filtering + Deterministic"""
        # AI layout detection
        sample_data = df.head(5).to_dict(orient='records')
        layout = self.intelligence.layout_detection(list(df.columns), sample_data)
        
        # Vector-based filtering (if vector_mapper available)
        if self.vector_mapper:
            pattern_info = self.intelligence.pattern_matcher(
                layout, list(df.columns), sample_data,
                vector_mapper=self.vector_mapper,
                source_file=input_file
            )
            if isinstance(pattern_info, dict):
                layout.update(pattern_info)
        
        # Deterministic extraction
        return self.core.process(df, layout, mapping, self.map_loader.metadata, base_year)
    
    def _full_agentic_processor(self, df, metadata, mapping, base_year, input_file):
        """Full agentic: Complete AI pipeline (legacy behavior)"""
        return self._run_legacy_pipeline_core(df, mapping, metadata, base_year, input_file)
    
    def _run_legacy_pipeline_core(self, df_raw, mapping, metadata, base_year, input_file):
        """Core logic of legacy pipeline (for use in full agentic track)"""
        sample_data = df_raw.head(5).to_dict(orient='records')

        # First detect the layout structure
        layout = self.intelligence.layout_detection(list(df_raw.columns), sample_data)

        # Then apply pattern matching to enhance the layout
        pattern_info = self.intelligence.pattern_matcher(
            layout,
            list(df_raw.columns),
            sample_data,
            vector_mapper=self.vector_mapper,
            source_file=input_file
        )
        if isinstance(pattern_info, dict):
            layout.update(pattern_info)
        
        # Filter mapping
        mapping_to_use, metadata_to_use = self._filter_mapping_metadata(
            df_raw, layout, mapping
        )
        
        # Run deterministic core
        return self.core.process(df_raw, layout, mapping_to_use, metadata_to_use, base_year)
    
    def _run_legacy_pipeline(self, input_file, base_year=2024):
        """
        Legacy pipeline (pre-hybrid) for backwards compatibility
        """
        self.logger.info("Running legacy pipeline...")

        base_name = os.path.splitext(os.path.basename(input_file))[0]

        # Step 1: Preprocess the input Excel file
        df_raw = preprocess_excel(input_file)
        save_intermediate_grid(df_raw, f"logs/{base_name}_preprocessed.xlsx")

        # Step 2: Load mapping configuration
        mapping = self.map_loader.load()

        # Step 3: Apply intelligence layer
        sample_data = df_raw.head(5).to_dict(orient='records')
        layout = self.intelligence.layout_detection(list(df_raw.columns), sample_data)

        pattern_info = self.intelligence.pattern_matcher(
            layout,
            list(df_raw.columns),
            sample_data,
            vector_mapper=self.vector_mapper,
            source_file=input_file
        )
        if isinstance(pattern_info, dict):
            layout.update(pattern_info)

        # Filter mapping to entries relevant to this sheet using mapping concepts
        try:
            # Determine candidate concept values from the sheet (concept columns)
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

            # Also include all concept tokens from mapping metadata (PRIMARY/SECONDARY/THIRD/FOURTH)
            mapping_concepts = set()
            for meta in self.map_loader.metadata.values():
                for k in ('primary_concept', 'secondary_concept', 'third_concept', 'fourth_concept'):
                    v = str(meta.get(k, '')).strip()
                    if v and v.lower() != 'nan':
                        mapping_concepts.add(v)
            candidate_vals.update(mapping_concepts)

            filtered_mapping = {}
            filtered_metadata = {}
            for key, series_code in mapping.items():
                meta = self.map_loader.metadata.get(key, {})
                primary = str(meta.get('primary_concept', '')).strip()
                secondary = str(meta.get('secondary_concept', '')).strip()
                third = str(meta.get('third_concept', '')).strip()

                # If any of the mapping concepts appear in the sheet candidate values, keep
                if (primary and primary in candidate_vals) or (secondary and secondary in candidate_vals) or (third and third in candidate_vals):
                    filtered_mapping[key] = series_code
                    filtered_metadata[key] = meta

            # Deduplicate by series_code, preferring exact primary matches
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

                mapping_to_use = deduped
                metadata_to_use = dedup_meta
            else:
                mapping_to_use = mapping
                metadata_to_use = self.map_loader.metadata
        except Exception:
            mapping_to_use = mapping
            metadata_to_use = self.map_loader.metadata

        # Step 4: Run deterministic core parsing logic
        result_data = self.core.process(df_raw, layout, mapping, self.map_loader.metadata, base_year)

        # Step 5: Validate the results
        if not self.validator.validate(result_data, layout):
            self.logger.error("Validation failed. Check logs for details.")
            return None

        # Step 6: Write output to Excel
        output_path = self.writer.write(result_data, base_name)

        # Step 7: Auto-save pattern if processing was successful and pattern is new
        self._auto_save_pattern(input_file, df_raw, layout, mapping, result_data)

        return output_path

    def _auto_save_pattern(self, input_file, df_raw, layout, mapping, result_data):
        """Automatically save pattern after successful processing."""
        try:
            # Extract pattern information
            base_name = os.path.splitext(os.path.basename(input_file))[0]
            headers = list(df_raw.columns)
            
            # Detect number of concepts (columns before time series)
            time_axis_index = layout.get('time_axis_index', 3)
            num_concepts = 0
            index_col_desc = None
            
            # Count concept columns (non-time-series columns)
            for i, col in enumerate(headers[:time_axis_index]):
                col_lower = str(col).lower()
                if 'ponderación' in col_lower or 'weight' in col_lower or 'índice' in col_lower:
                    index_col_desc = f"Column {i} - {col} (metadata), NORMAL DATA"
                else:
                    num_concepts += 1
            
            # Build concepts description
            concepts = []
            concept_labels = ["Primary", "Secondary", "Third", "Fourth"]
            for i in range(num_concepts):
                if i < len(headers):
                    concepts.append(f"{concept_labels[i]} Concept (Column {i} - {headers[i]}): NORMAL DATA")
            concepts_desc = ". ".join(concepts) + "."
            
            # Build time series description
            time_cols = headers[time_axis_index:]
            time_series_desc = f"Columns {time_axis_index}-{len(headers)-1} ({len(time_cols)} columns), TIME SERIES DATA"
            
            # Get update name from mapping metadata (extract from first matched series)
            update_name = None
            if result_data:
                # result_data structure: { "COLCPI0053.M": { "values": {...} }, ... }
                first_series_code = next(iter(result_data.keys()))
                
                # Find the metadata entry for this series
                for meta_key, meta in self.map_loader.metadata.items():
                    if meta.get('series_code') == first_series_code:
                        # Extract Update Name from metadata
                        update_name = meta.get('update_name')
                        break
            
            # If still not found, try to get from metadata keys (they contain Update Name)
            if not update_name:
                if self.map_loader.metadata:
                    first_key = next(iter(self.map_loader.metadata.keys()))
                    # Keys are in format "Update Name_PRIMARY CONCEPT"
                    if '_' in first_key:
                        update_name = first_key.rsplit('_', 1)[0]
            
            # Fallback to base_name if still not found
            if not update_name:
                update_name = base_name
            
            # Add pattern to library
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
            
        except Exception as e:
            print(f"⚠️ Could not auto-save pattern: {str(e)}")
            # Don't fail the entire pipeline if pattern saving fails