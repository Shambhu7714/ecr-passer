"""
Evaluation Engine - Comprehensive quality validation with confidence scoring.
Part of the Hybrid Agentic Parser implementation.
"""

from typing import Dict, List, Tuple, Any
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil import parser as date_parser
import re


class EvaluationEngine:
    """
    Multi-condition evaluation engine with confidence scoring.
    Runs 5 types of validation and provides weighted confidence scores.
    """
    
    def __init__(self, 
                 format_weight: float = 0.20,
                 dates_weight: float = 0.20,
                 ranges_weight: float = 0.15,
                 mappings_weight: float = 0.25,
                 completeness_weight: float = 0.20):
        """
        Initialize evaluation engine with validation weights.
        
        Args:
            format_weight: Weight for format validation (0-1)
            dates_weight: Weight for date continuity validation (0-1)
            ranges_weight: Weight for value range validation (0-1)
            mappings_weight: Weight for mapping completeness (0-1)
            completeness_weight: Weight for data completeness (0-1)
        """
        self.weights = {
            'format': format_weight,
            'dates': dates_weight,
            'ranges': ranges_weight,
            'mappings': mappings_weight,
            'completeness': completeness_weight
        }
        
        # Normalize weights to sum to 1
        total = sum(self.weights.values())
        self.weights = {k: v/total for k, v in self.weights.items()}
    
    def evaluate(self, result_data: Dict, layout: Dict, 
                 metadata: Dict = None) -> Tuple[bool, float, List[str]]:
        """
        Comprehensive evaluation of extraction results.
        
        Args:
            result_data: Extracted data dictionary
            layout: Layout information
            metadata: Optional metadata for additional validation
        
        Returns:
            (is_valid, confidence, issues) tuple
            - is_valid: Boolean indicating if data passes minimum threshold
            - confidence: Float 0-1 indicating overall confidence
            - issues: List of issue descriptions
        """
        scores = {}
        all_issues = []
        
        # Run all validation checks
        print("\n" + "="*80)
        print("📊 COMPREHENSIVE EVALUATION")
        print("="*80)
        
        # 1. Format Validation
        print("\n1️⃣ Format Validation...")
        format_score, format_issues = self._validate_format(result_data, layout)
        scores['format'] = format_score
        all_issues.extend(format_issues)
        print(f"   Score: {format_score:.1%}")
        
        # 2. Date Continuity Validation
        print("\n2️⃣ Date Continuity Validation...")
        dates_score, dates_issues = self._validate_dates(result_data)
        scores['dates'] = dates_score
        all_issues.extend(dates_issues)
        print(f"   Score: {dates_score:.1%}")
        
        # 3. Value Range Validation
        print("\n3️⃣ Value Range Validation...")
        ranges_score, ranges_issues = self._validate_ranges(result_data)
        scores['ranges'] = ranges_score
        all_issues.extend(ranges_issues)
        print(f"   Score: {ranges_score:.1%}")
        
        # 4. Mapping Completeness Validation
        print("\n4️⃣ Mapping Completeness Validation...")
        mappings_score, mappings_issues = self._validate_mappings(result_data, layout)
        scores['mappings'] = mappings_score
        all_issues.extend(mappings_issues)
        print(f"   Score: {mappings_score:.1%}")
        
        # 5. Data Completeness Validation
        print("\n5️⃣ Data Completeness Validation...")
        completeness_score, completeness_issues = self._validate_completeness(result_data)
        scores['completeness'] = completeness_score
        all_issues.extend(completeness_issues)
        print(f"   Score: {completeness_score:.1%}")
        
        # Calculate weighted confidence
        confidence = sum(scores[key] * self.weights[key] for key in scores)
        
        # Determine if valid (threshold: 90%)
        is_valid = confidence >= 0.90 and format_score >= 0.80
        
        print("\n" + "="*80)
        print(f"🎯 OVERALL CONFIDENCE: {confidence:.1%}")
        print(f"   Format:       {scores['format']:.1%} (weight: {self.weights['format']:.0%})")
        print(f"   Dates:        {scores['dates']:.1%} (weight: {self.weights['dates']:.0%})")
        print(f"   Ranges:       {scores['ranges']:.1%} (weight: {self.weights['ranges']:.0%})")
        print(f"   Mappings:     {scores['mappings']:.1%} (weight: {self.weights['mappings']:.0%})")
        print(f"   Completeness: {scores['completeness']:.1%} (weight: {self.weights['completeness']:.0%})")
        print(f"\n{'✅ PASSED' if is_valid else '⚠️ NEEDS REVIEW'}")
        print("="*80 + "\n")
        
        return is_valid, confidence, all_issues
    
    def _validate_format(self, result_data: Dict, layout: Dict) -> Tuple[float, List[str]]:
        """Validate data format and structure."""
        issues = []
        checks_passed = 0
        total_checks = 0
        
        if not result_data:
            return 0.0, ["No data found in results"]
        
        for series_code, series_data in result_data.items():
            total_checks += 1
            
            # Check if series has values
            if 'values' not in series_data:
                issues.append(f"Series {series_code} missing 'values' key")
                continue
            
            values = series_data['values']
            
            # Check if values is a dict
            if not isinstance(values, dict):
                issues.append(f"Series {series_code} values is not a dictionary")
                continue
            
            # Check if values has data
            if len(values) == 0:
                issues.append(f"Series {series_code} has empty values")
                continue
            
            checks_passed += 1
        
        score = checks_passed / total_checks if total_checks > 0 else 0.0
        return score, issues
    
    def _validate_dates(self, result_data: Dict) -> Tuple[float, List[str]]:
        """Validate date continuity and consistency."""
        issues = []
        checks_passed = 0
        total_checks = 0
        
        for series_code, series_data in result_data.items():
            if 'values' not in series_data or not series_data['values']:
                continue
            
            dates = list(series_data['values'].keys())
            total_checks += 1
            
            if len(dates) < 2:
                checks_passed += 1  # Single date is technically continuous
                continue
            
            # Try to parse dates
            try:
                # Parse dates flexibly (support YYYY-MM, 'Jan 2025', 'Índice, Enero, 2025', etc.)
                parsed_dates = []
                for date_str in dates:
                    if not isinstance(date_str, str):
                        continue
                    date_str_clean = date_str.strip()
                    # Try ISO-like YYYY-MM or YYYY-MM-DD
                    if re.match(r"^\d{4}-\d{1,2}(?:-\d{1,2})?$", date_str_clean):
                        parts = date_str_clean.split('-')
                        year = int(parts[0])
                        month = int(parts[1])
                        parsed_dates.append((year, month))
                        continue

                    # Try dateutil parser for fuzzy formats
                    try:
                        dt = date_parser.parse(date_str_clean, fuzzy=True, default=datetime(1900, 1, 1))
                        year = dt.year
                        month = dt.month
                        parsed_dates.append((year, month))
                        continue
                    except Exception:
                        # Last-resort: look for a four-digit year and a month name/number
                        m = re.search(r"(\d{4})", date_str_clean)
                        month_num = None
                        if m:
                            year = int(m.group(1))
                            # try to find month name in the string
                            month_match = re.search(r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)", date_str_clean, re.IGNORECASE)
                            if month_match:
                                month_name = month_match.group(1).lower()
                                months_map = {
                                    'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12,
                                    'enero':1,'febrero':2,'marzo':3,'abril':4,'mayo':5,'junio':6,'julio':7,'agosto':8,'septiembre':9,'octubre':10,'noviembre':11,'diciembre':12
                                }
                                month_num = months_map.get(month_name[:3], None) if len(month_name) > 0 else None
                            # if we have a year and possibly month, append; else skip
                            parsed_dates.append((year, month_num or 1))
                            continue
                        # otherwise skip non-parseable date strings
                        continue
                
                if len(parsed_dates) < 2:
                    checks_passed += 1
                    continue
                
                # Sort dates
                parsed_dates.sort()
                
                # Check for continuity
                is_continuous = True
                for i in range(len(parsed_dates) - 1):
                    year1, month1 = parsed_dates[i]
                    year2, month2 = parsed_dates[i + 1]
                    
                    # Calculate month difference
                    month_diff = (year2 - year1) * 12 + (month2 - month1)
                    
                    if month_diff > 2:  # Allow 1-2 month gaps
                        is_continuous = False
                        issues.append(f"Series {series_code}: Date gap detected ({year1}-{month1} to {year2}-{month2})")
                        break
                
                if is_continuous:
                    checks_passed += 1
                    
            except Exception as e:
                issues.append(f"Series {series_code}: Date parsing error - {str(e)}")
        
        score = checks_passed / total_checks if total_checks > 0 else 1.0
        return score, issues
    
    def _validate_ranges(self, result_data: Dict) -> Tuple[float, List[str]]:
        """Validate value ranges and detect outliers."""
        issues = []
        checks_passed = 0
        total_checks = 0
        
        for series_code, series_data in result_data.items():
            if 'values' not in series_data or not series_data['values']:
                continue
            
            values = list(series_data['values'].values())
            total_checks += 1
            
            # Convert to numeric
            numeric_values = []
            for v in values:
                try:
                    if v is not None and v != '':
                        numeric_values.append(float(v))
                except (ValueError, TypeError):
                    continue
            
            if len(numeric_values) < 3:
                checks_passed += 1  # Too few values to check
                continue
            
            # Calculate statistics
            arr = np.array(numeric_values)
            mean = np.mean(arr)
            std = np.std(arr)
            
            # Check for outliers (values > 3 standard deviations)
            outliers = []
            for i, val in enumerate(numeric_values):
                if std > 0:
                    z_score = abs((val - mean) / std)
                    if z_score > 3:
                        outliers.append((i, val, z_score))
            
            if len(outliers) > len(numeric_values) * 0.1:  # More than 10% outliers
                issues.append(f"Series {series_code}: {len(outliers)} outliers detected (>10% of data)")
            else:
                checks_passed += 1
        
        score = checks_passed / total_checks if total_checks > 0 else 1.0
        return score, issues
    
    def _validate_mappings(self, result_data: Dict, layout: Dict) -> Tuple[float, List[str]]:
        """Validate mapping completeness."""
        issues = []
        
        # Check if vector mappings were used
        if 'vector_mappings' in layout:
            mappings = layout['vector_mappings']
            total_fields = len(mappings)
            
            if total_fields == 0:
                return 1.0, []  # No mappings needed
            
            # Check mapping confidence
            low_confidence = []
            for field, mapping_info in mappings.items():
                if mapping_info.get('confidence', 1.0) < 0.7:
                    low_confidence.append(field)
            
            if low_confidence:
                issues.append(f"{len(low_confidence)} fields with low mapping confidence: {low_confidence[:3]}")
            
            score = 1.0 - (len(low_confidence) / total_fields)
            return max(score, 0.5), issues  # Minimum 50% if any mappings exist
        
        # No vector mappings - assume traditional mapping used
        return 1.0, []
    
    def _validate_completeness(self, result_data: Dict) -> Tuple[float, List[str]]:
        """Validate data completeness (non-null values)."""
        issues = []
        total_values = 0
        non_null_values = 0
        
        for series_code, series_data in result_data.items():
            if 'values' not in series_data:
                continue
            
            for date, value in series_data['values'].items():
                total_values += 1
                if value is not None and value != '':
                    non_null_values += 1
        
        if total_values == 0:
            return 0.0, ["No values found in any series"]
        
        completeness = non_null_values / total_values
        
        if completeness < 0.8:
            issues.append(f"Low data completeness: {completeness:.1%} ({non_null_values}/{total_values} values)")
        
        return completeness, issues
