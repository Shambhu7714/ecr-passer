""""
Cache Manager - Stores and retrieves known patterns for fast-track processing
Learns from successful parses to accelerate future processing
"""

import json
import hashlib
import os
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any
from pathlib import Path
from core.logger import get_logger

logger = get_logger()


class PatternCache:
    """
    Intelligent pattern cache for fast-track routing.
    
    Features:
    - Pattern fingerprinting
    - Success rate tracking
    - Auto-expiration of stale patterns
    - Pattern confidence scoring
    """
    
    def __init__(self, cache_dir: str = "./cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.patterns_file = self.cache_dir / "patterns.json"
        self.stats_file = self.cache_dir / "cache_stats.json"
        
        self.patterns = self._load_patterns()
        self.stats = self._load_stats()
        
        # Cache settings
        self.MAX_CACHE_AGE_DAYS = 90      # Patterns expire after 90 days
        self.MIN_CONFIDENCE = 0.7         # Minimum confidence to use pattern
        self.MIN_SUCCESS_COUNT = 2        # Minimum successful uses to trust pattern
    
    def _load_patterns(self) -> Dict:
        """Load patterns from disk"""
        if self.patterns_file.exists():
            try:
                with open(self.patterns_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load patterns: {e}")
                return {}
        return {}
    
    def _load_stats(self) -> Dict:
        """Load cache statistics from disk"""
        if self.stats_file.exists():
            try:
                with open(self.stats_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load stats: {e}")
                return self._default_stats()
        return self._default_stats()
    
    def _default_stats(self) -> Dict:
        """Default statistics structure"""
        return {
            "total_patterns": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "patterns_created": 0,
            "patterns_expired": 0,
            "fast_track_count": 0,
            "avg_confidence": 0.0
        }
    
    def _save_patterns(self):
        """Persist patterns to disk"""
        try:
            with open(self.patterns_file, 'w') as f:
                json.dump(self.patterns, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save patterns: {e}")
    
    def _save_stats(self):
        """Persist statistics to disk"""
        try:
            with open(self.stats_file, 'w') as f:
                json.dump(self.stats, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save stats: {e}")
    
    def generate_fingerprint(self, file_path: str, sheet_name: str = None, 
                           metadata: Dict = None) -> str:
        """
        Generate unique fingerprint for file pattern.
        
        Fingerprint based on:
        - Filename pattern (normalized)
        - Sheet name
        - File structure hints
        """
        # Normalize filename (remove dates, timestamps)
        filename = os.path.basename(file_path)
        normalized = self._normalize_filename(filename)
        
        # Create fingerprint components
        components = [normalized]
        
        if sheet_name:
            components.append(sheet_name)
        
        if metadata:
            # Include structural hints
            if 'shape' in metadata:
                components.append(f"shape_{metadata['shape']}")
            if 'has_multilevel_headers' in metadata:
                components.append(f"mlh_{metadata['has_multilevel_headers']}")
        
        # Generate hash
        fingerprint_str = "|".join(components)
        return hashlib.md5(fingerprint_str.encode()).hexdigest()
    
    def _normalize_filename(self, filename: str) -> str:
        """Normalize filename by removing dates and timestamps"""
        import re
        
        # Remove extension
        name = os.path.splitext(filename)[0]
        
        # Remove dates (YYYY-MM-DD, YYYYMMDD, etc.)
        name = re.sub(r'\d{4}[-_]?\d{2}[-_]?\d{2}', 'DATE', name)
        name = re.sub(r'\d{8}', 'DATE', name)
        
        # Remove timestamps
        name = re.sub(r'\d{2}:\d{2}:\d{2}', 'TIME', name)
        name = re.sub(r'\d{6,8}', 'TIMESTAMP', name)
        
        # Normalize separators
        name = re.sub(r'[-_]+', '-', name)
        
        return name.lower().strip('-')
    
    def get_pattern(self, file_path: str, sheet_name: str = None, 
                   metadata: Dict = None) -> Optional[Dict]:
        """
        Retrieve cached pattern if available and valid.
        
        Returns None if no valid pattern found.
        """
        fingerprint = self.generate_fingerprint(file_path, sheet_name, metadata)
        
        if fingerprint not in self.patterns:
            self.stats["cache_misses"] += 1
            self._save_stats()
            return None
        
        pattern = self.patterns[fingerprint]
        
        # Check if pattern is expired
        created_date = datetime.fromisoformat(pattern["created_at"])
        age_days = (datetime.now() - created_date).days
        
        if age_days > self.MAX_CACHE_AGE_DAYS:
            logger.info(f"Pattern expired (age: {age_days} days) - removing")
            del self.patterns[fingerprint]
            self.stats["patterns_expired"] += 1
            self._save_patterns()
            self._save_stats()
            return None
        
        # Check confidence
        confidence = pattern.get("confidence", 0.0)
        if confidence < self.MIN_CONFIDENCE:
            logger.info(f"Pattern confidence too low ({confidence:.2f}) - ignoring")
            return None
        
        # Check success count
        success_count = pattern.get("success_count", 0)
        if success_count < self.MIN_SUCCESS_COUNT:
            logger.info(f"Pattern not proven yet ({success_count} successes) - ignoring")
            return None
        
        # Valid pattern found
        self.stats["cache_hits"] += 1
        self.stats["fast_track_count"] += 1
        self._save_stats()
        
        logger.info(f"✓ Valid pattern found (fingerprint: {fingerprint[:8]}, "
                   f"confidence: {confidence:.2f}, uses: {success_count})")
        
        return pattern
    
    def save_pattern(self, file_path: str, sheet_name: str = None,
                    pattern_data: Dict = None, metadata: Dict = None,
                    success: bool = True):
        """
        Save or update a pattern in the cache.
        
        Args:
            file_path: Source file path
            sheet_name: Sheet name (if applicable)
            pattern_data: Pattern configuration (layout, mappings, etc.)
            metadata: File metadata
            success: Whether this parse was successful
        """
        fingerprint = self.generate_fingerprint(file_path, sheet_name, metadata)
        
        now = datetime.now()
        
        if fingerprint in self.patterns:
            # Update existing pattern
            pattern = self.patterns[fingerprint]
            
            if success:
                pattern["success_count"] += 1
                pattern["last_success"] = now.isoformat()
            else:
                pattern["failure_count"] += 1
                pattern["last_failure"] = now.isoformat()
            
            # Update confidence
            total = pattern["success_count"] + pattern["failure_count"]
            pattern["confidence"] = pattern["success_count"] / total if total > 0 else 0.0
            
            pattern["last_used"] = now.isoformat()
            pattern["use_count"] += 1
            
            logger.info(f"Updated pattern {fingerprint[:8]}: "
                       f"{pattern['success_count']} successes, "
                       f"{pattern['failure_count']} failures, "
                       f"confidence: {pattern['confidence']:.2f}")
        else:
            # Create new pattern
            pattern = {
                "pattern_id": fingerprint,
                "created_at": now.isoformat(),
                "last_used": now.isoformat(),
                "file_pattern": self._normalize_filename(os.path.basename(file_path)),
                "sheet_name": sheet_name,
                "success_count": 1 if success else 0,
                "failure_count": 0 if success else 1,
                "use_count": 1,
                "confidence": 1.0 if success else 0.0,
                "last_success": now.isoformat() if success else None,
                "last_failure": now.isoformat() if not success else None,
                "pattern_data": pattern_data or {},
                "metadata": metadata or {}
            }
            
            self.patterns[fingerprint] = pattern
            self.stats["patterns_created"] += 1
            self.stats["total_patterns"] = len(self.patterns)
            
            logger.info(f"Created new pattern {fingerprint[:8]} for {pattern['file_pattern']}")
        
        self._save_patterns()
        self._save_stats()
    
    def get_statistics(self) -> Dict:
        """Get cache performance statistics"""
        total_requests = self.stats["cache_hits"] + self.stats["cache_misses"]
        hit_rate = (self.stats["cache_hits"] / total_requests * 100) if total_requests > 0 else 0
        
        # Calculate average confidence
        if self.patterns:
            avg_confidence = sum(p.get("confidence", 0) for p in self.patterns.values()) / len(self.patterns)
        else:
            avg_confidence = 0.0
        
        return {
            "total_patterns": len(self.patterns),
            "active_patterns": sum(1 for p in self.patterns.values() 
                                  if p.get("confidence", 0) >= self.MIN_CONFIDENCE),
            "cache_hits": self.stats["cache_hits"],
            "cache_misses": self.stats["cache_misses"],
            "hit_rate_pct": hit_rate,
            "patterns_created": self.stats["patterns_created"],
            "patterns_expired": self.stats["patterns_expired"],
            "fast_track_count": self.stats["fast_track_count"],
            "avg_confidence": avg_confidence
        }
    
    def list_patterns(self, min_confidence: float = 0.0) -> List[Dict]:
        """List all patterns above confidence threshold"""
        patterns = []
        
        for fingerprint, pattern in self.patterns.items():
            confidence = pattern.get("confidence", 0.0)
            
            if confidence >= min_confidence:
                patterns.append({
                    "fingerprint": fingerprint[:12],
                    "file_pattern": pattern.get("file_pattern"),
                    "sheet_name": pattern.get("sheet_name"),
                    "confidence": confidence,
                    "success_count": pattern.get("success_count", 0),
                    "failure_count": pattern.get("failure_count", 0),
                    "created_at": pattern.get("created_at"),
                    "last_used": pattern.get("last_used")
                })
        
        # Sort by confidence descending
        patterns.sort(key=lambda x: x["confidence"], reverse=True)
        
        return patterns
    
    def clear_expired_patterns(self) -> int:
        """Remove all expired patterns and return count"""
        now = datetime.now()
        expired = []
        
        for fingerprint, pattern in self.patterns.items():
            created_date = datetime.fromisoformat(pattern["created_at"])
            age_days = (now - created_date).days
            
            if age_days > self.MAX_CACHE_AGE_DAYS:
                expired.append(fingerprint)
        
        for fingerprint in expired:
            del self.patterns[fingerprint]
        
        if expired:
            self.stats["patterns_expired"] += len(expired)
            self.stats["total_patterns"] = len(self.patterns)
            self._save_patterns()
            self._save_stats()
            
            logger.info(f"Cleared {len(expired)} expired patterns")
        
        return len(expired)
    
    def clear_all(self):
        """Clear all cached patterns"""
        self.patterns = {}
        self.stats = self._default_stats()
        self._save_patterns()
        self._save_stats()
        logger.info("Cache cleared")