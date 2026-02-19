"""
Complexity Analyzer - Determines optimal routing track for files
Routes files to Fast (70%), Hybrid (25%), or Full Agentic (5%) tracks
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple, Any
from core.logger import get_logger
from enum import Enum

logger = get_logger()


class ComplexityTrack(Enum):
    """File complexity tracks for routing"""
    FAST = "fast"           # 70% - Known patterns, deterministic
    HYBRID = "hybrid"       # 25% - Moderate complexity, vector + rules
    FULL_AGENTIC = "full"   # 5% - High complexity, full AI reasoning


class ComplexityAnalyzer:
    """
    Analyzes file complexity to determine optimal processing track.
    
    Scoring Factors:
    - File size and shape
    - Data cleanliness (nulls, mixed types)
    - Pattern recognition (known vs unknown)
    - Header complexity (multi-level, merged cells)
    - Date format consistency
    - Concept clarity
    """
    
    def __init__(self, cache_manager=None):
        self.cache_manager = cache_manager
        
        # Complexity thresholds
        self.FAST_THRESHOLD = 30      # Score <= 30: Fast track
        self.HYBRID_THRESHOLD = 60    # Score 31-60: Hybrid track
        # Score > 60: Full agentic track
    
    def analyze(self, file_path: str, df: pd.DataFrame, 
                sheet_name: str = None, metadata: Dict = None) -> Tuple[ComplexityTrack, float, Dict]:
        """
        Analyze file complexity and determine routing track.
        
        Returns:
            (track, complexity_score, analysis_details)
        """
        logger.info(f"Analyzing complexity for: {file_path}")
        
        # Check cache first
        if self.cache_manager:
            cached_pattern = self.cache_manager.get_pattern(file_path, sheet_name)
            if cached_pattern:
                logger.info(f"[OK] Known pattern found in cache - routing to FAST track")
                return ComplexityTrack.FAST, 0, {
                    "reason": "known_pattern",
                    "cached": True,
                    "pattern_id": cached_pattern.get("pattern_id")
                }
        
        # Calculate complexity score
        score = 0
        details = {}
        
        # Factor 1: File size (5-15 points)
        size_score = self._score_file_size(df)
        score += size_score
        details["size_score"] = size_score
        
        # Factor 2: Data cleanliness (5-20 points)
        cleanliness_score = self._score_data_cleanliness(df)
        score += cleanliness_score
        details["cleanliness_score"] = cleanliness_score
        
        # Factor 3: Header complexity (5-15 points)
        header_score = self._score_header_complexity(df)
        score += header_score
        details["header_score"] = header_score
        
        # Factor 4: Layout clarity (5-15 points)
        layout_score = self._score_layout_clarity(df)
        score += layout_score
        details["layout_score"] = layout_score
        
        # Factor 5: Date format consistency (0-10 points)
        date_score = self._score_date_complexity(df)
        score += date_score
        details["date_score"] = date_score
        
        # Factor 6: Concept clarity (5-15 points)
        concept_score = self._score_concept_clarity(df, metadata)
        score += concept_score
        details["concept_score"] = concept_score
        
        # Factor 7: Structure predictability (5-10 points)
        structure_score = self._score_structure_predictability(df)
        score += structure_score
        details["structure_score"] = structure_score
        
        # Determine track based on score
        if score <= self.FAST_THRESHOLD:
            track = ComplexityTrack.FAST
            details["reason"] = "low_complexity"
        elif score <= self.HYBRID_THRESHOLD:
            track = ComplexityTrack.HYBRID
            details["reason"] = "moderate_complexity"
        else:
            track = ComplexityTrack.FULL_AGENTIC
            details["reason"] = "high_complexity"
        
        details["total_score"] = score
        logger.info(f"Complexity analysis: Track={track.value}, Score={score}")
        
        return track, score, details
    
    def _score_file_size(self, df: pd.DataFrame) -> int:
        """Score based on file dimensions (5-15 points)"""
        rows, cols = df.shape
        total_cells = rows * cols
        
        if total_cells < 500:          # Small files
            return 5
        elif total_cells < 2000:       # Medium files
            return 8
        elif total_cells < 5000:       # Large files
            return 12
        else:                          # Very large files
            return 15
    
    def _score_data_cleanliness(self, df: pd.DataFrame) -> int:
        """Score based on null values and mixed types (5-20 points)"""
        score = 5  # Base score
        
        # Check null percentage
        null_pct = df.isnull().sum().sum() / (df.shape[0] * df.shape[1])
        if null_pct > 0.3:      # >30% nulls
            score += 8
        elif null_pct > 0.15:   # 15-30% nulls
            score += 5
        elif null_pct > 0.05:   # 5-15% nulls
            score += 2
        
        # Check mixed types in columns
        mixed_type_cols = 0
        for col in df.columns:
            try:
                types = df[col].apply(type).unique()
                if len(types) > 2:  # More than 2 types
                    mixed_type_cols += 1
            except:
                pass
        
        if mixed_type_cols > df.shape[1] * 0.3:  # >30% mixed
            score += 7
        elif mixed_type_cols > 0:
            score += 3
        
        return min(score, 20)
    
    def _score_header_complexity(self, df: pd.DataFrame) -> int:
        """Score based on header structure (5-15 points)"""
        score = 5  # Base score
        
        # Check for multi-level headers (heuristic)
        first_rows = df.head(5)
        
        # Check for merged cell patterns (many empty cells in header area)
        empty_in_headers = first_rows.isnull().sum().sum()
        empty_pct = empty_in_headers / (first_rows.shape[0] * first_rows.shape[1])
        
        if empty_pct > 0.4:      # Likely multi-level headers
            score += 10
        elif empty_pct > 0.2:
            score += 5
        elif empty_pct > 0.1:
            score += 2
        
        return min(score, 15)
    
    def _score_layout_clarity(self, df: pd.DataFrame) -> int:
        """Score based on data layout clarity (5-15 points)"""
        score = 5  # Base score
        
        # Check for clear numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        numeric_ratio = len(numeric_cols) / len(df.columns) if len(df.columns) > 0 else 0
        
        if numeric_ratio < 0.2:     # Very few numeric columns
            score += 10
        elif numeric_ratio < 0.4:   # Some numeric columns
            score += 5
        elif numeric_ratio < 0.6:
            score += 2
        
        return min(score, 15)
    
    def _score_date_complexity(self, df: pd.DataFrame) -> int:
        """Score based on date format complexity (0-10 points)"""
        score = 0
        
        # Look for date-like patterns in first column
        try:
            first_col = df.iloc[:, 0].astype(str)
            
            # Check for ISO dates (easy)
            iso_pattern = r'\d{4}-\d{2}-\d{2}'
            iso_matches = first_col.str.contains(iso_pattern, na=False).sum()
            
            if iso_matches > len(first_col) * 0.8:  # Mostly ISO dates
                score = 0  # No complexity
            elif iso_matches > len(first_col) * 0.3:
                score = 3  # Some ISO dates
            else:
                # Check for Spanish/Portuguese month names (complex)
                complex_patterns = ['enero', 'febrero', 'marzo', 'trimestre', 'trim']
                for pattern in complex_patterns:
                    if first_col.str.lower().str.contains(pattern, na=False).any():
                        score = 10
                        break
                
                if score == 0:
                    score = 5  # Unknown date format
        except:
            score = 8  # Error parsing dates
        
        return score
    
    def _score_concept_clarity(self, df: pd.DataFrame, metadata: Dict = None) -> int:
        """Score based on concept clarity (5-15 points)"""
        score = 5  # Base score
        
        # Check if concepts are clear (short, alphanumeric)
        try:
            # Look at potential concept columns (first few rows)
            concept_candidates = df.head(5).values.flatten()
            concept_strs = [str(c) for c in concept_candidates if pd.notna(c)]
            
            # Calculate average length
            if concept_strs:
                avg_len = sum(len(s) for s in concept_strs) / len(concept_strs)
                
                if avg_len > 50:        # Very long concepts
                    score += 10
                elif avg_len > 30:      # Long concepts
                    score += 6
                elif avg_len > 20:
                    score += 3
        except:
            score += 5
        
        return min(score, 15)
    
    def _score_structure_predictability(self, df: pd.DataFrame) -> int:
        """Score based on structure predictability (5-10 points)"""
        score = 5  # Base score
        
        # Check for consistent row/column patterns
        try:
            # Check if rows have consistent non-null counts
            row_counts = df.notna().sum(axis=1)
            std_dev = row_counts.std()
            mean_count = row_counts.mean()
            
            if mean_count > 0:
                coefficient_variation = std_dev / mean_count
                
                if coefficient_variation > 0.5:    # High variation
                    score += 5
                elif coefficient_variation > 0.3:  # Moderate variation
                    score += 3
        except:
            score += 5
        
        return min(score, 10)
    
    def get_track_distribution_stats(self, analyses: list) -> Dict[str, Any]:
        """Calculate distribution statistics across multiple analyses"""
        if not analyses:
            return {}
        
        tracks = [a[0] for a in analyses]
        scores = [a[1] for a in analyses]
        
        fast_count = sum(1 for t in tracks if t == ComplexityTrack.FAST)
        hybrid_count = sum(1 for t in tracks if t == ComplexityTrack.HYBRID)
        full_count = sum(1 for t in tracks if t == ComplexityTrack.FULL_AGENTIC)
        
        total = len(tracks)
        
        return {
            "total_files": total,
            "fast_track": {"count": fast_count, "percentage": fast_count / total * 100},
            "hybrid_track": {"count": hybrid_count, "percentage": hybrid_count / total * 100},
            "full_agentic_track": {"count": full_count, "percentage": full_count / total * 100},
            "avg_complexity_score": sum(scores) / len(scores),
            "max_complexity_score": max(scores),
            "min_complexity_score": min(scores)
        }
