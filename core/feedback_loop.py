"""
Feedback Loop - Learns from successes and failures to improve future parsing
Stores insights and adapts parsing strategies
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path
import json
from core.logger import get_logger

logger = get_logger()


class FeedbackLoop:
    """
    Implements continuous learning from parsing outcomes.
    
    Features:
    - Success/failure tracking
    - Pattern learning
    - Error analysis
    - Strategy adaptation
    - Performance metrics
    """
    
    def __init__(self, feedback_dir: str = "./feedback"):
        self.feedback_dir = Path(feedback_dir)
        self.feedback_dir.mkdir(parents=True, exist_ok=True)
        
        self.outcomes_file = self.feedback_dir / "outcomes.jsonl"
        self.insights_file = self.feedback_dir / "insights.json"
        self.metrics_file = self.feedback_dir / "metrics.json"
        
        self.insights = self._load_insights()
        self.metrics = self._load_metrics()
    
    def _load_insights(self) -> Dict:
        """Load accumulated insights"""
        if self.insights_file.exists():
            try:
                with open(self.insights_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load insights: {e}")
                return self._default_insights()
        return self._default_insights()
    
    def _default_insights(self) -> Dict:
        """Default insights structure"""
        return {
            "successful_patterns": {},
            "problematic_patterns": {},
            "error_patterns": {},
            "track_preferences": {},
            "optimization_rules": []
        }
    
    def _load_metrics(self) -> Dict:
        """Load performance metrics"""
        if self.metrics_file.exists():
            try:
                with open(self.metrics_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load metrics: {e}")
                return self._default_metrics()
        return self._default_metrics()
    
    def _default_metrics(self) -> Dict:
        """Default metrics structure"""
        return {
            "total_files_processed": 0,
            "successful_parses": 0,
            "failed_parses": 0,
            "fast_track_count": 0,
            "hybrid_track_count": 0,
            "full_agentic_track_count": 0,
            "avg_processing_time": 0.0,
            "avg_quality_score": 0.0,
            "total_cost": 0.0
        }
    
    def _save_insights(self):
        """Persist insights to disk"""
        try:
            with open(self.insights_file, 'w') as f:
                json.dump(self.insights, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save insights: {e}")
    
    def _save_metrics(self):
        """Persist metrics to disk"""
        try:
            with open(self.metrics_file, 'w') as f:
                json.dump(self.metrics, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save metrics: {e}")
    
    def record_outcome(self, outcome: Dict):
        """
        Record parsing outcome for learning.
        
        Args:
            outcome: {
                "file_path": str,
                "sheet_name": str,
                "track_used": str (fast/hybrid/full),
                "success": bool,
                "quality_score": float,
                "processing_time": float,
                "cost": float,
                "complexity_score": float,
                "errors": List[str],
                "warnings": List[str],
                "metadata": Dict
            }
        """
        # Append to outcomes log
        try:
            with open(self.outcomes_file, 'a') as f:
                outcome["timestamp"] = datetime.now().isoformat()
                f.write(json.dumps(outcome) + '\n')
        except Exception as e:
            logger.error(f"Failed to record outcome: {e}")
        
        # Update metrics
        self.metrics["total_files_processed"] += 1
        
        if outcome.get("success"):
            self.metrics["successful_parses"] += 1
        else:
            self.metrics["failed_parses"] += 1
        
        # Update track counts
        track = outcome.get("track_used", "unknown")
        if track == "fast":
            self.metrics["fast_track_count"] += 1
        elif track == "hybrid":
            self.metrics["hybrid_track_count"] += 1
        elif track == "full":
            self.metrics["full_agentic_track_count"] += 1
        
        # Update running averages
        total = self.metrics["total_files_processed"]
        
        if outcome.get("processing_time"):
            old_avg = self.metrics["avg_processing_time"]
            new_avg = (old_avg * (total - 1) + outcome["processing_time"]) / total
            self.metrics["avg_processing_time"] = new_avg
        
        if outcome.get("quality_score"):
            old_avg = self.metrics["avg_quality_score"]
            new_avg = (old_avg * (total - 1) + outcome["quality_score"]) / total
            self.metrics["avg_quality_score"] = new_avg
        
        if outcome.get("cost"):
            self.metrics["total_cost"] += outcome["cost"]
        
        self._save_metrics()
        
        # Learn from outcome
        self._learn_from_outcome(outcome)
        
        logger.info(f"Outcome recorded: Success={outcome.get('success')}, "
                   f"Track={track}, Quality={outcome.get('quality_score', 0):.2f}")
    
    def _learn_from_outcome(self, outcome: Dict):
        """Extract insights from outcome"""
        file_pattern = self._normalize_filename(outcome.get("file_path", ""))
        track = outcome.get("track_used", "unknown")
        success = outcome.get("success", False)
        quality_score = outcome.get("quality_score", 0.0)
        complexity_score = outcome.get("complexity_score", 0.0)
        
        # Learn successful patterns
        if success and quality_score >= 0.9:
            pattern_key = f"{file_pattern}|{track}"
            
            if pattern_key not in self.insights["successful_patterns"]:
                self.insights["successful_patterns"][pattern_key] = {
                    "file_pattern": file_pattern,
                    "preferred_track": track,
                    "success_count": 0,
                    "avg_quality": 0.0,
                    "avg_complexity": 0.0
                }
            
            pattern = self.insights["successful_patterns"][pattern_key]
            count = pattern["success_count"]
            pattern["success_count"] += 1
            pattern["avg_quality"] = (pattern["avg_quality"] * count + quality_score) / (count + 1)
            pattern["avg_complexity"] = (pattern["avg_complexity"] * count + complexity_score) / (count + 1)
        
        # Learn problematic patterns
        elif not success or quality_score < 0.7:
            pattern_key = f"{file_pattern}|{track}"
            
            if pattern_key not in self.insights["problematic_patterns"]:
                self.insights["problematic_patterns"][pattern_key] = {
                    "file_pattern": file_pattern,
                    "failed_track": track,
                    "failure_count": 0,
                    "errors": []
                }
            
            pattern = self.insights["problematic_patterns"][pattern_key]
            pattern["failure_count"] += 1
            
            # Store error messages
            errors = outcome.get("errors", [])
            pattern["errors"].extend(errors[:3])  # Keep sample
            pattern["errors"] = pattern["errors"][-10:]  # Keep last 10
        
        # Learn error patterns
        if outcome.get("errors"):
            for error in outcome["errors"]:
                error_key = self._categorize_error(error)
                
                if error_key not in self.insights["error_patterns"]:
                    self.insights["error_patterns"][error_key] = {
                        "category": error_key,
                        "count": 0,
                        "examples": []
                    }
                
                error_pattern = self.insights["error_patterns"][error_key]
                error_pattern["count"] += 1
                error_pattern["examples"].append({
                    "file": file_pattern,
                    "track": track,
                    "error": error
                })
                error_pattern["examples"] = error_pattern["examples"][-5:]  # Keep last 5
        
        # Learn track preferences
        if success and quality_score >= 0.85:
            if file_pattern not in self.insights["track_preferences"]:
                self.insights["track_preferences"][file_pattern] = {
                    "fast": 0,
                    "hybrid": 0,
                    "full": 0,
                    "recommended": None
                }
            
            self.insights["track_preferences"][file_pattern][track] += 1
            
            # Update recommendation
            prefs = self.insights["track_preferences"][file_pattern]
            recommended = max(["fast", "hybrid", "full"], key=lambda t: prefs[t])
            prefs["recommended"] = recommended
        
        self._save_insights()
    
    def _normalize_filename(self, file_path: str) -> str:
        """Normalize filename for pattern matching"""
        import re
        import os
        
        name = os.path.basename(file_path)
        name = os.path.splitext(name)[0]
        
        # Remove dates
        name = re.sub(r'\d{4}[-_]?\d{2}[-_]?\d{2}', 'DATE', name)
        name = re.sub(r'\d{8}', 'DATE', name)
        name = re.sub(r'\d{6,8}', 'TIMESTAMP', name)
        
        return name.lower().strip('-_')
    
    def _categorize_error(self, error: str) -> str:
        """Categorize error message"""
        error_lower = error.lower()
        
        if 'mapping' in error_lower or 'series code' in error_lower:
            return "mapping_error"
        elif 'date' in error_lower or 'temporal' in error_lower:
            return "date_error"
        elif 'value' in error_lower or 'numeric' in error_lower:
            return "value_error"
        elif 'layout' in error_lower or 'structure' in error_lower:
            return "layout_error"
        elif 'validation' in error_lower or 'semantic' in error_lower:
            return "validation_error"
        else:
            return "unknown_error"
    
    def get_recommendation(self, file_path: str, complexity_score: float = None) -> Dict:
        """
        Get processing recommendation based on learned insights.
        
        Returns:
            {
                "recommended_track": str,
                "confidence": float,
                "reason": str,
                "fallback_track": str
            }
        """
        file_pattern = self._normalize_filename(file_path)
        
        recommendation = {
            "recommended_track": "hybrid",  # Default
            "confidence": 0.5,
            "reason": "default_strategy",
            "fallback_track": "full"
        }
        
        # Check track preferences
        if file_pattern in self.insights["track_preferences"]:
            prefs = self.insights["track_preferences"][file_pattern]
            recommended_track = prefs.get("recommended")
            
            if recommended_track:
                total_successes = sum(prefs[t] for t in ["fast", "hybrid", "full"])
                track_successes = prefs[recommended_track]
                confidence = track_successes / total_successes if total_successes > 0 else 0.5
                
                recommendation["recommended_track"] = recommended_track
                recommendation["confidence"] = confidence
                recommendation["reason"] = f"learned_preference_{track_successes}_successes"
                
                # Set fallback
                if recommended_track == "fast":
                    recommendation["fallback_track"] = "hybrid"
                elif recommended_track == "hybrid":
                    recommendation["fallback_track"] = "full"
                
                logger.info(f"Learned recommendation for {file_pattern}: "
                           f"{recommended_track} (confidence: {confidence:.2f})")
                
                return recommendation
        
        # Check successful patterns
        for pattern_key, pattern in self.insights["successful_patterns"].items():
            if pattern["file_pattern"] == file_pattern:
                if pattern["success_count"] >= 3:  # At least 3 successes
                    recommendation["recommended_track"] = pattern["preferred_track"]
                    recommendation["confidence"] = min(pattern["avg_quality"], 0.95)
                    recommendation["reason"] = f"proven_pattern_{pattern['success_count']}_successes"
                    
                    logger.info(f"Proven pattern for {file_pattern}: "
                               f"{pattern['preferred_track']} "
                               f"(quality: {pattern['avg_quality']:.2f})")
                    
                    return recommendation
        
        # Check problematic patterns
        for pattern_key, pattern in self.insights["problematic_patterns"].items():
            if pattern["file_pattern"] == file_pattern:
                failed_track = pattern["failed_track"]
                
                # Recommend higher track if previous track failed
                if failed_track == "fast" and pattern["failure_count"] >= 2:
                    recommendation["recommended_track"] = "hybrid"
                    recommendation["reason"] = "fast_track_failures"
                    recommendation["confidence"] = 0.7
                elif failed_track == "hybrid" and pattern["failure_count"] >= 2:
                    recommendation["recommended_track"] = "full"
                    recommendation["reason"] = "hybrid_track_failures"
                    recommendation["confidence"] = 0.8
                
                logger.info(f"Avoiding problematic track for {file_pattern}: "
                           f"Recommending {recommendation['recommended_track']}")
                
                return recommendation
        
        # Use complexity score if available
        if complexity_score is not None:
            if complexity_score <= 30:
                recommendation["recommended_track"] = "fast"
                recommendation["confidence"] = 0.7
                recommendation["reason"] = "low_complexity"
            elif complexity_score <= 60:
                recommendation["recommended_track"] = "hybrid"
                recommendation["confidence"] = 0.75
                recommendation["reason"] = "moderate_complexity"
            else:
                recommendation["recommended_track"] = "full"
                recommendation["confidence"] = 0.8
                recommendation["reason"] = "high_complexity"
        
        return recommendation
    
    def get_insights_summary(self) -> Dict:
        """Get summary of accumulated insights"""
        return {
            "successful_patterns": len(self.insights["successful_patterns"]),
            "problematic_patterns": len(self.insights["problematic_patterns"]),
            "error_patterns": len(self.insights["error_patterns"]),
            "track_preferences": len(self.insights["track_preferences"]),
            "optimization_rules": len(self.insights["optimization_rules"]),
            "metrics": self.metrics
        }
    
    def get_recent_outcomes(self, limit: int = 10) -> List[Dict]:
        """Get recent parsing outcomes"""
        outcomes = []
        
        try:
            if self.outcomes_file.exists():
                with open(self.outcomes_file, 'r') as f:
                    lines = f.readlines()
                    
                # Get last N lines
                for line in lines[-limit:]:
                    try:
                        outcomes.append(json.loads(line))
                    except:
                        pass
        except Exception as e:
            logger.error(f"Failed to read outcomes: {e}")
        
        return outcomes
    
    def generate_insights_report(self) -> str:
        """Generate human-readable insights report"""
        lines = []
        
        lines.append("=" * 70)
        lines.append("FEEDBACK LOOP INSIGHTS REPORT")
        lines.append("=" * 70)
        lines.append(f"Generated: {datetime.now().isoformat()}")
        lines.append("")
        
        # Metrics summary
        lines.append("Performance Metrics:")
        lines.append("-" * 70)
        lines.append(f"  Total Files Processed: {self.metrics['total_files_processed']}")
        lines.append(f"  Success Rate: {self.metrics['successful_parses'] / max(self.metrics['total_files_processed'], 1):.1%}")
        lines.append(f"  Avg Processing Time: {self.metrics['avg_processing_time']:.2f}s")
        lines.append(f"  Avg Quality Score: {self.metrics['avg_quality_score']:.2%}")
        lines.append(f"  Total Cost: ${self.metrics['total_cost']:.4f}")
        lines.append("")
        
        # Track distribution
        lines.append("Track Distribution:")
        lines.append("-" * 70)
        total = self.metrics['total_files_processed']
        if total > 0:
            lines.append(f"  Fast Track: {self.metrics['fast_track_count']} ({self.metrics['fast_track_count']/total:.1%})")
            lines.append(f"  Hybrid Track: {self.metrics['hybrid_track_count']} ({self.metrics['hybrid_track_count']/total:.1%})")
            lines.append(f"  Full Agentic: {self.metrics['full_agentic_track_count']} ({self.metrics['full_agentic_track_count']/total:.1%})")
        lines.append("")
        
        # Successful patterns
        lines.append(f"Successful Patterns: {len(self.insights['successful_patterns'])}")
        lines.append("-" * 70)
        sorted_successes = sorted(
            self.insights["successful_patterns"].items(),
            key=lambda x: x[1]["success_count"],
            reverse=True
        )
        for pattern_key, pattern in sorted_successes[:5]:
            lines.append(f"  [OK] {pattern['file_pattern']}   {pattern['preferred_track']} track")
            lines.append(f"    Successes: {pattern['success_count']}, Avg Quality: {pattern['avg_quality']:.2%}")
        lines.append("")
        
        # Problematic patterns
        if self.insights["problematic_patterns"]:
            lines.append(f"Problematic Patterns: {len(self.insights['problematic_patterns'])}")
            lines.append("-" * 70)
            sorted_problems = sorted(
                self.insights["problematic_patterns"].items(),
                key=lambda x: x[1]["failure_count"],
                reverse=True
            )
            for pattern_key, pattern in sorted_problems[:5]:
                lines.append(f"  [ERR] {pattern['file_pattern']}   {pattern['failed_track']} track")
                lines.append(f"    Failures: {pattern['failure_count']}")
            lines.append("")
        
        # Error patterns
        if self.insights["error_patterns"]:
            lines.append(f"Common Error Patterns: {len(self.insights['error_patterns'])}")
            lines.append("-" * 70)
            sorted_errors = sorted(
                self.insights["error_patterns"].items(),
                key=lambda x: x[1]["count"],
                reverse=True
            )
            for error_key, error_pattern in sorted_errors[:5]:
                lines.append(f"    {error_pattern['category']}: {error_pattern['count']} occurrences")
            lines.append("")
        
        lines.append("=" * 70)
        
        return "\n".join(lines)
