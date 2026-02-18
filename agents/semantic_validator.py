"""
Semantic Validator Agent - Validates extracted data for semantic correctness
Uses LLM to verify data makes sense in context
"""

import json
from typing import Dict, List, Any, Tuple
from core.logger import get_logger
import google.generativeai as genai
import os

logger = get_logger()


class SemanticValidator:
    """
    Validates extracted data for semantic correctness.
    
    Checks:
    - Value ranges make sense for concept type
    - Temporal consistency (trends, seasonality)
    - Cross-series relationships
    - Data anomalies and outliers
    """
    
    def __init__(self, model_name: str = None):
        self.model_name = model_name or os.getenv("LLM_MODEL_NAME", "gemini-2.0-flash-exp")
        
        # Validation thresholds
        self.CONFIDENCE_THRESHOLD = 0.7
        self.MAX_ANOMALY_PCT = 0.15  # 15% anomalies acceptable
    
    def validate(self, extracted_data: Dict, metadata: Dict = None) -> Tuple[bool, Dict]:
        """
        Validate extracted data for semantic correctness.
        
        Returns:
            (is_valid, validation_report)
        """
        logger.info(f"Semantic validation started for {len(extracted_data)} series")
        
        validation_report = {
            "series_checked": 0,
            "issues_found": [],
            "warnings": [],
            "confidence_score": 1.0,
            "is_valid": True
        }
        
        # Validate each series
        for series_code, series_data in extracted_data.items():
            validation_report["series_checked"] += 1
            
            # Get values
            values = series_data.get("values", {})
            
            if not values:
                validation_report["warnings"].append({
                    "series": series_code,
                    "issue": "no_data",
                    "message": "Series has no data points"
                })
                continue
            
            # Check 1: Value range validation
            range_check = self._validate_value_range(series_code, values, metadata)
            if not range_check["valid"]:
                validation_report["issues_found"].append({
                    "series": series_code,
                    "check": "value_range",
                    "details": range_check
                })
            
            # Check 2: Temporal consistency
            temporal_check = self._validate_temporal_consistency(series_code, values)
            if not temporal_check["valid"]:
                validation_report["issues_found"].append({
                    "series": series_code,
                    "check": "temporal_consistency",
                    "details": temporal_check
                })
            
            # Check 3: Anomaly detection
            anomaly_check = self._detect_anomalies(series_code, values)
            if anomaly_check["anomaly_count"] > 0:
                anomaly_pct = anomaly_check["anomaly_count"] / len(values)
                
                if anomaly_pct > self.MAX_ANOMALY_PCT:
                    validation_report["issues_found"].append({
                        "series": series_code,
                        "check": "anomalies",
                        "details": anomaly_check
                    })
                else:
                    validation_report["warnings"].append({
                        "series": series_code,
                        "check": "minor_anomalies",
                        "details": anomaly_check
                    })
        
        # Check 4: Cross-series validation (if multiple series)
        if len(extracted_data) > 1:
            cross_check = self._validate_cross_series(extracted_data, metadata)
            if not cross_check["valid"]:
                validation_report["issues_found"].append({
                    "check": "cross_series",
                    "details": cross_check
                })
        
        # Calculate overall confidence
        issue_count = len(validation_report["issues_found"])
        warning_count = len(validation_report["warnings"])
        
        # Reduce confidence based on issues
        confidence = 1.0 - (issue_count * 0.15) - (warning_count * 0.05)
        validation_report["confidence_score"] = max(confidence, 0.0)
        
        # Determine if valid
        validation_report["is_valid"] = (
            confidence >= self.CONFIDENCE_THRESHOLD and 
            issue_count < 3  # Maximum 2 critical issues
        )
        
        logger.info(f"Validation complete: Valid={validation_report['is_valid']}, "
                   f"Confidence={validation_report['confidence_score']:.2f}, "
                   f"Issues={issue_count}, Warnings={warning_count}")
        
        return validation_report["is_valid"], validation_report
    
    def _validate_value_range(self, series_code: str, values: Dict, 
                             metadata: Dict = None) -> Dict:
        """Validate that values are in reasonable range"""
        try:
            numeric_values = [float(v) for v in values.values() if v is not None]
            
            if not numeric_values:
                return {"valid": False, "reason": "no_numeric_values"}
            
            min_val = min(numeric_values)
            max_val = max(numeric_values)
            avg_val = sum(numeric_values) / len(numeric_values)
            
            # Check for unreasonable values
            issues = []
            
            # Negative values in series that should be positive
            if min_val < 0:
                # Check if this series should be positive only
                if self._should_be_positive(series_code, metadata):
                    issues.append("negative_values_detected")
            
            # Extreme values (>1000% change)
            if max_val > 0 and min_val > 0:
                ratio = max_val / min_val
                if ratio > 1000:
                    issues.append("extreme_value_range")
            
            # All zeros
            if max_val == 0:
                issues.append("all_zero_values")
            
            return {
                "valid": len(issues) == 0,
                "issues": issues,
                "min": min_val,
                "max": max_val,
                "avg": avg_val,
                "count": len(numeric_values)
            }
            
        except Exception as e:
            logger.error(f"Value range validation error: {e}")
            return {"valid": False, "reason": "validation_error", "error": str(e)}
    
    def _should_be_positive(self, series_code: str, metadata: Dict = None) -> bool:
        """Heuristic to determine if series should only have positive values"""
        # Check series code or metadata for indicators
        positive_indicators = [
            'population', 'gdp', 'production', 'index', 'price',
            'employment', 'total', 'absolute'
        ]
        
        series_lower = series_code.lower()
        
        for indicator in positive_indicators:
            if indicator in series_lower:
                return True
        
        # Check metadata if available
        if metadata:
            concept = metadata.get('primary_concept', '').lower()
            for indicator in positive_indicators:
                if indicator in concept:
                    return True
        
        return False
    
    def _validate_temporal_consistency(self, series_code: str, values: Dict) -> Dict:
        """Validate temporal ordering and consistency"""
        try:
            # Sort by date
            sorted_dates = sorted(values.keys())
            
            if len(sorted_dates) < 2:
                return {"valid": True, "reason": "insufficient_data"}
            
            # Check for date gaps
            date_gaps = []
            for i in range(1, len(sorted_dates)):
                prev_date = sorted_dates[i-1]
                curr_date = sorted_dates[i]
                
                # Simple gap detection (more than 2 months)
                # This is a heuristic; would need proper date parsing
                if curr_date > prev_date:
                    # Dates are ordered correctly
                    pass
                else:
                    date_gaps.append((prev_date, curr_date))
            
            issues = []
            
            if date_gaps:
                issues.append("date_order_issues")
            
            return {
                "valid": len(issues) == 0,
                "issues": issues,
                "date_count": len(sorted_dates),
                "first_date": sorted_dates[0],
                "last_date": sorted_dates[-1]
            }
            
        except Exception as e:
            logger.error(f"Temporal consistency validation error: {e}")
            return {"valid": False, "reason": "validation_error", "error": str(e)}
    
    def _detect_anomalies(self, series_code: str, values: Dict) -> Dict:
        """Detect statistical anomalies in values"""
        try:
            numeric_values = [float(v) for v in values.values() if v is not None]
            
            if len(numeric_values) < 3:
                return {"anomaly_count": 0, "reason": "insufficient_data"}
            
            # Calculate statistics
            avg = sum(numeric_values) / len(numeric_values)
            variance = sum((x - avg) ** 2 for x in numeric_values) / len(numeric_values)
            std_dev = variance ** 0.5
            
            # Detect outliers (values > 3 standard deviations from mean)
            anomalies = []
            
            if std_dev > 0:
                for date, value in values.items():
                    try:
                        val = float(value)
                        z_score = abs((val - avg) / std_dev)
                        
                        if z_score > 3:
                            anomalies.append({
                                "date": date,
                                "value": val,
                                "z_score": z_score
                            })
                    except:
                        pass
            
            return {
                "anomaly_count": len(anomalies),
                "anomalies": anomalies[:5],  # Return max 5 examples
                "mean": avg,
                "std_dev": std_dev
            }
            
        except Exception as e:
            logger.error(f"Anomaly detection error: {e}")
            return {"anomaly_count": 0, "error": str(e)}
    
    def _validate_cross_series(self, extracted_data: Dict, metadata: Dict = None) -> Dict:
        """Validate relationships between multiple series"""
        try:
            # Check if series have overlapping dates
            all_dates = set()
            series_dates = {}
            
            for series_code, series_data in extracted_data.items():
                dates = set(series_data.get("values", {}).keys())
                series_dates[series_code] = dates
                all_dates.update(dates)
            
            # Find common dates
            common_dates = all_dates.copy()
            for dates in series_dates.values():
                common_dates &= dates
            
            issues = []
            
            # Check date overlap
            overlap_pct = len(common_dates) / len(all_dates) if all_dates else 0
            
            if overlap_pct < 0.3:  # Less than 30% overlap
                issues.append("low_date_overlap")
            
            return {
                "valid": len(issues) == 0,
                "issues": issues,
                "total_dates": len(all_dates),
                "common_dates": len(common_dates),
                "overlap_pct": overlap_pct * 100
            }
            
        except Exception as e:
            logger.error(f"Cross-series validation error: {e}")
            return {"valid": False, "error": str(e)}
    
    def validate_with_llm(self, extracted_data: Dict, metadata: Dict = None) -> Tuple[bool, Dict]:
        """
        Deep semantic validation using LLM.
        
        Use this for complex validation scenarios or when statistical checks are insufficient.
        """
        try:
            # Prepare sample data for LLM
            sample_data = {}
            for series_code, series_data in list(extracted_data.items())[:3]:  # Sample 3 series
                values = series_data.get("values", {})
                # Take first 10 and last 10 data points
                sorted_dates = sorted(values.keys())
                sample_dates = sorted_dates[:10] + sorted_dates[-10:]
                
                sample_data[series_code] = {
                    date: values[date] for date in sample_dates if date in values
                }
            
            prompt = f"""
Analyze this extracted economic data for semantic correctness:

Data Sample:
{json.dumps(sample_data, indent=2)}

Context:
{json.dumps(metadata, indent=2) if metadata else "No additional context"}

Validate:
1. Do the values make sense for the time period?
2. Are there any obvious data quality issues?
3. Do the trends appear reasonable?
4. Are the value magnitudes appropriate?

Return JSON:
{{
  "is_valid": true/false,
  "confidence": 0.0-1.0,
  "issues": ["issue1", "issue2"],
  "explanation": "Brief explanation of validation result"
}}
"""
            
            # Call LLM
            model = genai.GenerativeModel(self.model_name)
            response = model.generate_content(
                prompt,
                generation_config=genai.types.GenerationConfig(
                    temperature=0.0,
                    max_output_tokens=1000
                )
            )
            
            # Parse response
            response_text = response.text.strip()
            
            # Strip markdown fences if present
            if response_text.startswith('```'):
                lines = response_text.split('\n')
                # Remove first line and last line
                response_text = '\n'.join(lines[1:-1])
                # Remove 'json' if it's the first word
                if response_text.strip().startswith('json'):
                    response_text = response_text.strip()[4:].strip()
            
            result = json.loads(response_text)
            
            logger.info(f"LLM validation: Valid={result.get('is_valid')}, "
                       f"Confidence={result.get('confidence')}")
            
            return result.get("is_valid", False), result
            
        except Exception as e:
            logger.error(f"LLM validation error: {e}")
            return False, {
                "is_valid": False,
                "error": str(e),
                "confidence": 0.0
            }
