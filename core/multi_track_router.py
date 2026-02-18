"""
Multi-Track Router - Routes files to Fast/Hybrid/Full Agentic tracks
Orchestrates track-specific processing pipelines
"""

from typing import Dict, Tuple, Any
from enum import Enum
from core.logger import get_logger
from core.complexity_analyzer import ComplexityAnalyzer, ComplexityTrack
from core.cache_manager import PatternCache
from core.feedback_loop import FeedbackLoop
import time

logger = get_logger()


class ProcessingResult:
    """Result from track processing"""
    def __init__(self, success: bool, data: Dict = None, 
                 track_used: str = None, metadata: Dict = None):
        self.success = success
        self.data = data or {}
        self.track_used = track_used
        self.metadata = metadata or {}
        self.processing_time = 0.0
        self.cost = 0.0
        self.quality_score = 0.0


class MultiTrackRouter:
    """
    Routes files to appropriate processing tracks based on complexity.
    
    Three Processing Tracks:
    - Fast Track (70%): Deterministic + Cache (known patterns)
    - Hybrid Track (25%): Vector Mapper + Deterministic + Light AI
    - Full Agentic Track (5%): Complete AI pipeline with all agents
    """
    
    def __init__(self, enable_cache: bool = True, enable_feedback: bool = True):
        self.complexity_analyzer = ComplexityAnalyzer()
        
        # Initialize supporting systems
        self.cache_manager = PatternCache() if enable_cache else None
        self.feedback_loop = FeedbackLoop() if enable_feedback else None
        
        # Pass cache manager to complexity analyzer
        if self.cache_manager:
            self.complexity_analyzer.cache_manager = self.cache_manager
        
        # Track statistics
        self.stats = {
            "fast_track_count": 0,
            "hybrid_track_count": 0,
            "full_agentic_count": 0,
            "total_files": 0,
            "cache_hits": 0
        }
        
        logger.info("Multi-Track Router initialized")
        logger.info(f"  Cache: {'Enabled' if enable_cache else 'Disabled'}")
        logger.info(f"  Feedback Loop: {'Enabled' if enable_feedback else 'Disabled'}")
    
    def route_and_process(self, file_path: str, df, sheet_name: str = None, 
                         metadata: Dict = None, processors: Dict = None) -> ProcessingResult:
        """
        Analyze complexity, route to appropriate track, and process file.
        
        Args:
            file_path: Source file path
            df: Preprocessed DataFrame
            sheet_name: Sheet name (if applicable)
            metadata: File metadata
            processors: Dict of processors for each track
                {
                    "fast": fast_track_processor,
                    "hybrid": hybrid_track_processor,
                    "full": full_agentic_processor
                }
        
        Returns:
            ProcessingResult
        """
        start_time = time.time()
        
        self.stats["total_files"] += 1
        
        logger.info(f"Routing file: {file_path}")
        
        # Step 1: Analyze complexity
        track, complexity_score, analysis_details = self.complexity_analyzer.analyze(
            file_path, df, sheet_name, metadata
        )
        
        logger.info(f"Complexity analysis: Track={track.value}, Score={complexity_score:.1f}")
        
        # Step 2: Check feedback loop for recommendations
        if self.feedback_loop:
            recommendation = self.feedback_loop.get_recommendation(
                file_path, complexity_score
            )
            
            if recommendation["confidence"] >= 0.8:
                # High confidence recommendation - override complexity analysis
                recommended_track_str = recommendation["recommended_track"]
                track = ComplexityTrack(recommended_track_str)
                logger.info(f"Feedback override: {recommended_track_str} "
                           f"(confidence: {recommendation['confidence']:.2f}, "
                           f"reason: {recommendation['reason']})")
        
        # Step 3: Route to appropriate track
        if track == ComplexityTrack.FAST:
            self.stats["fast_track_count"] += 1
            result = self._process_fast_track(
                file_path, df, sheet_name, metadata, processors
            )
        elif track == ComplexityTrack.HYBRID:
            self.stats["hybrid_track_count"] += 1
            result = self._process_hybrid_track(
                file_path, df, sheet_name, metadata, processors
            )
        else:  # FULL_AGENTIC
            self.stats["full_agentic_count"] += 1
            result = self._process_full_agentic_track(
                file_path, df, sheet_name, metadata, processors
            )
        
        # Set result metadata
        result.track_used = track.value
        result.processing_time = time.time() - start_time
        result.metadata["complexity_score"] = complexity_score
        result.metadata["analysis_details"] = analysis_details
        
        # Step 4: Record outcome for learning
        if self.feedback_loop:
            outcome = {
                "file_path": file_path,
                "sheet_name": sheet_name,
                "track_used": track.value,
                "success": result.success,
                "quality_score": result.quality_score,
                "processing_time": result.processing_time,
                "cost": result.cost,
                "complexity_score": complexity_score,
                "errors": result.metadata.get("errors", []),
                "warnings": result.metadata.get("warnings", []),
                "metadata": metadata
            }
            self.feedback_loop.record_outcome(outcome)
        
        # Step 5: Update cache if successful
        if result.success and self.cache_manager:
            pattern_data = {
                "track": track.value,
                "layout": result.metadata.get("layout"),
                "mappings": result.metadata.get("mappings")
            }
            self.cache_manager.save_pattern(
                file_path, sheet_name, pattern_data, metadata, success=True
            )
        
        logger.info(f"Processing complete: Track={track.value}, "
                   f"Success={result.success}, Time={result.processing_time:.2f}s")
        
        return result
    
    def _process_fast_track(self, file_path: str, df, sheet_name: str,
                           metadata: Dict, processors: Dict) -> ProcessingResult:
        """
        Fast Track: Deterministic + Cache
        
        - No AI inference
        - Pattern matching only
        - Cached configurations
        - ~1-2 seconds processing
        """
        logger.info("→ Fast Track processing (Deterministic + Cache)")
        
        try:
            # Use fast track processor if provided
            if processors and "fast" in processors:
                processor = processors["fast"]
                result_data = processor(df, metadata)
                
                return ProcessingResult(
                    success=True,
                    data=result_data,
                    track_used="fast",
                    metadata={"method": "deterministic"}
                )
            else:
                # Default fast track: use deterministic core
                from core.deterministic_core import DeterministicCore
                
                # Load mapping metadata
                mapping_metadata = metadata.get("mapping_metadata", [])
                
                # Initialize deterministic core
                deterministic = DeterministicCore()
                
                # Extract using deterministic rules
                result_data = deterministic.extract_concept_based(
                    df, mapping_metadata
                )
                
                # Calculate quality score (simple heuristic)
                quality_score = 0.9 if result_data else 0.0
                
                result = ProcessingResult(
                    success=bool(result_data),
                    data=result_data,
                    track_used="fast"
                )
                result.quality_score = quality_score
                result.cost = 0.0  # No AI cost
                
                return result
                
        except Exception as e:
            logger.error(f"Fast track processing failed: {e}")
            
            # Fallback to hybrid track
            logger.info("↑ Falling back to Hybrid Track")
            return self._process_hybrid_track(
                file_path, df, sheet_name, metadata, processors
            )
    
    def _process_hybrid_track(self, file_path: str, df, sheet_name: str,
                             metadata: Dict, processors: Dict) -> ProcessingResult:
        """
        Hybrid Track: Vector Mapper + Deterministic + Light AI
        
        - Vector-based field matching
        - AI for layout detection only
        - Deterministic extraction
        - ~5-10 seconds processing
        """
        logger.info("→ Hybrid Track processing (Vector + Deterministic + Light AI)")
        
        try:
            # Use hybrid track processor if provided
            if processors and "hybrid" in processors:
                processor = processors["hybrid"]
                result_data = processor(df, metadata)
                
                return ProcessingResult(
                    success=True,
                    data=result_data,
                    track_used="hybrid",
                    metadata={"method": "hybrid"}
                )
            else:
                # Default hybrid track: Intelligence Layer + Vector Mapper + Deterministic
                from core.intelligence import IntelligenceLayer
                from core.vector_mapper import VectorMapper
                from core.deterministic_core import DeterministicCore
                
                # Step 1: AI layout detection
                intelligence = IntelligenceLayer()
                layout_result = intelligence.layout_detection(df, sheet_name)
                
                # Step 2: Vector-based field filtering
                vector_mapper = VectorMapper()
                mapping_metadata = metadata.get("mapping_metadata", [])
                
                # Filter mappings using vector similarity
                if mapping_metadata:
                    # Extract concepts from DataFrame
                    sample_concepts = self._extract_concepts(df, layout_result)
                    
                    # Filter relevant mappings
                    filtered_mappings = vector_mapper.filter_relevant_mappings(
                        sample_concepts, mapping_metadata, top_k=50
                    )
                else:
                    filtered_mappings = mapping_metadata
                
                # Step 3: Deterministic extraction
                deterministic = DeterministicCore()
                result_data = deterministic.extract_concept_based(
                    df, filtered_mappings
                )
                
                # Calculate quality score
                quality_score = 0.85 if result_data else 0.0
                
                result = ProcessingResult(
                    success=bool(result_data),
                    data=result_data,
                    track_used="hybrid"
                )
                result.quality_score = quality_score
                result.cost = 0.0001  # Minimal AI cost (layout detection only)
                result.metadata["layout"] = layout_result
                
                return result
                
        except Exception as e:
            logger.error(f"Hybrid track processing failed: {e}")
            
            # Fallback to full agentic track
            logger.info("↑ Falling back to Full Agentic Track")
            return self._process_full_agentic_track(
                file_path, df, sheet_name, metadata, processors
            )
    
    def _process_full_agentic_track(self, file_path: str, df, sheet_name: str,
                                   metadata: Dict, processors: Dict) -> ProcessingResult:
        """
        Full Agentic Track: Complete AI pipeline
        
        - AI layout detection
        - Vector semantic matching
        - AI-powered extraction
        - Semantic validation
        - Hierarchy extraction
        - Quality auditing
        - ~20-30 seconds processing
        """
        logger.info("→ Full Agentic Track processing (Complete AI Pipeline)")
        
        try:
            # Use full agentic processor if provided
            if processors and "full" in processors:
                processor = processors["full"]
                result_data = processor(df, metadata)
                
                return ProcessingResult(
                    success=True,
                    data=result_data,
                    track_used="full",
                    metadata={"method": "full_agentic"}
                )
            else:
                # Default full agentic: Use supervisor
                from agents.supervisor import Supervisor
                
                supervisor = Supervisor()
                
                # Run full pipeline
                result_data = supervisor.run_pipeline(
                    file_path=file_path,
                    df=df,
                    sheet_name=sheet_name,
                    metadata=metadata
                )
                
                # Calculate quality score from result
                quality_score = result_data.get("quality_score", 0.8)
                
                result = ProcessingResult(
                    success=bool(result_data.get("series")),
                    data=result_data.get("series", {}),
                    track_used="full"
                )
                result.quality_score = quality_score
                result.cost = 0.001  # Full AI cost
                result.metadata = result_data.get("metadata", {})
                
                return result
                
        except Exception as e:
            logger.error(f"Full agentic track processing failed: {e}")
            
            return ProcessingResult(
                success=False,
                data={},
                track_used="full",
                metadata={"error": str(e)}
            )
    
    def _extract_concepts(self, df, layout_result: Dict) -> list:
        """Extract sample concepts from DataFrame for vector matching"""
        concepts = []
        
        try:
            # Extract from column names
            concepts.extend([str(col) for col in df.columns])
            
            # Extract from first few rows (potential headers)
            for i in range(min(5, len(df))):
                row_values = df.iloc[i].values
                concepts.extend([str(v) for v in row_values if v and str(v).strip()])
            
            # Limit to unique concepts
            concepts = list(set(concepts))[:20]
            
        except Exception as e:
            logger.error(f"Concept extraction error: {e}")
        
        return concepts
    
    def get_statistics(self) -> Dict:
        """Get routing statistics"""
        total = self.stats["total_files"]
        
        if total == 0:
            return self.stats
        
        return {
            **self.stats,
            "fast_track_pct": self.stats["fast_track_count"] / total * 100,
            "hybrid_track_pct": self.stats["hybrid_track_count"] / total * 100,
            "full_agentic_pct": self.stats["full_agentic_count"] / total * 100,
            "cache_hit_rate": self.stats["cache_hits"] / total * 100 if self.cache_manager else 0
        }
    
    def get_cache_statistics(self) -> Dict:
        """Get cache statistics"""
        if self.cache_manager:
            return self.cache_manager.get_statistics()
        return {}
    
    def get_feedback_insights(self) -> Dict:
        """Get feedback loop insights"""
        if self.feedback_loop:
            return self.feedback_loop.get_insights_summary()
        return {}
