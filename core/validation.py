import pandas as pd
import json
from typing import Tuple, List
from core.evaluation_engine import EvaluationEngine

class Validator:
    def __init__(self, map_loader=None, confidence_threshold=0.8, enable_enhanced_validation=True):
        self.confidence_threshold = confidence_threshold
        self.map_loader = map_loader
        self.enable_enhanced_validation = enable_enhanced_validation
        
        # Initialize evaluation engine if enhanced validation is enabled
        if enable_enhanced_validation:
            self.evaluation_engine = EvaluationEngine(
                format_weight=0.20,
                dates_weight=0.20,
                ranges_weight=0.15,
                mappings_weight=0.25,
                completeness_weight=0.20
            )
        else:
            self.evaluation_engine = None

    def validate(self, data, layout) -> Tuple[bool, float, List[str]]:
        """
        Validates the extracted data structure.
        
        Args:
            data: Dictionary format { "SERIES_CODE": { "values": { "date": value } } }
            layout: Layout information
        
        Returns:
            If enhanced validation enabled: (is_valid, confidence, issues) tuple
            If basic validation only: boolean
        """
        print(f"\n{'='*60}")
        print(f"  STEP 5: VALIDATION & GUARDRAILS")
        print(f"{'='*60}")
        
        # Run enhanced validation if enabled
        if self.enable_enhanced_validation and self.evaluation_engine:
            is_valid, confidence, issues = self.evaluation_engine.evaluate(
                data, layout, self.map_loader.metadata if self.map_loader else None
            )
            
            # Also run basic checks
            basic_valid = self._basic_validation(data, layout)
            
            # Return enhanced result
            return (basic_valid and is_valid, confidence, issues)
        
        # Run basic validation only
        is_valid = self._basic_validation(data, layout)
        return is_valid  # Return boolean for backward compatibility
    
    def _basic_validation(self, data, layout) -> bool:
        """Basic validation checks (original logic)."""
        
        # 1. Check confidence from layout detection
        confidence = layout.get('confidence', 1.0)
        print(f"[DATA] Layout detection confidence: {confidence:.2%}")
        if confidence < self.confidence_threshold:
            print(f"[WARN] Low confidence detected: {confidence:.2%}")
        
        # 2. Validate data structure
        if not isinstance(data, dict):
            print("[ERR] Validation Error: Data is not a dictionary.")
            return False
        
        if len(data) == 0:
            print("[ERR] Validation Error: No series data extracted.")
            return False
        
        print(f"[OK] Found {len(data)} series")
        
        # 3. Validate each series
        for series_code, series_data in data.items():
            if 'values' not in series_data:
                print(f"[WARN] Warning: Series '{series_code}' missing 'values' key")
                continue
            
            values = series_data['values']
            if len(values) == 0:
                print(f"[WARN] Warning: Series '{series_code}' has no values")
                continue
            
            # Check for series-specific validation rules
            if self.map_loader:
                self._validate_series_rules(series_code, values)
        
        print(f"[OK] Validation passed for {len(data)} series.")
        return True
    
    def _validate_series_rules(self, series_code, values):
        """Apply series-specific validation rules."""
        # Find the series in metadata by source_id
        series_name = None
        for name, metadata in self.map_loader.metadata.items():
            if metadata.get('source_id') == series_code:
                series_name = name
                break
        
        if not series_name:
            return
        
        rules = self.map_loader.get_validation_rules(series_name)
        if not rules:
            return
        
        # Convert values to list for analysis
        value_list = [float(v) for v in values.values() if v]
        
        # Check for no-change scenarios
        if rules.get('no_change_action') == 'Stop':
            if len(set(value_list)) == 1:
                print(f"[WARN] Alert: Series '{series_code}' has no variation in values.")
        
        # Check for gaps in dates
        if rules.get('gaps_action') == 'Alert':
            dates = sorted(values.keys())
            if len(dates) > 1:
                date_objs = pd.to_datetime(dates)
                gaps = date_objs.to_series().diff().dt.days
                max_gap = gaps.max()
                if max_gap > 90:
                    print(f"[WARN] Alert: Series '{series_code}' has date gaps > 90 days (max: {max_gap} days).")
        
        # Check threshold values
        threshold = rules.get('threshold')
        if threshold and not pd.isna(threshold):
            try:
                threshold_val = float(threshold)
                if any(v > threshold_val for v in value_list):
                    print(f"[WARN] Alert: Series '{series_code}' exceeds threshold {threshold_val}.")
            except:
                pass