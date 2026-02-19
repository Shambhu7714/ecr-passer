"""
Quality Auditor Agent - Comprehensive quality assessment of parsed output
Combines multiple quality checks and generates audit reports
"""

from typing import Dict, List, Any, Tuple
from datetime import datetime
from core.logger import get_logger
import json

logger = get_logger()


class QualityAuditor:
    """
    Performs comprehensive quality audits on parsed data.
    
    Audit Dimensions:
    - Completeness: Are all expected fields present?
    - Accuracy: Do values match source data?
    - Consistency: Are formats and patterns consistent?
    - Validity: Do values pass semantic checks?
    - Timeliness: Is data current and up-to-date?
    """
    
    def __init__(self):
        # Quality thresholds
        self.COMPLETENESS_THRESHOLD = 0.8    # 80% of expected data
        self.ACCURACY_THRESHOLD = 0.95        # 95% accuracy
        self.CONSISTENCY_THRESHOLD = 0.9      # 90% consistency
        self.OVERALL_PASS_THRESHOLD = 0.85    # 85% overall score to pass
    
    def audit(self, parsed_output: Dict, source_metadata: Dict = None,
              validation_results: Dict = None) -> Tuple[bool, Dict]:
        """
        Perform comprehensive quality audit.
        
        Returns:
            (passed, audit_report)
        """
        logger.info("Starting quality audit")
        
        audit_report = {
            "timestamp": datetime.now().isoformat(),
            "overall_score": 0.0,
            "passed": False,
            "dimensions": {},
            "critical_issues": [],
            "warnings": [],
            "recommendations": []
        }
        
        # Dimension 1: Completeness Check
        completeness = self._check_completeness(parsed_output, source_metadata)
        audit_report["dimensions"]["completeness"] = completeness
        
        # Dimension 2: Accuracy Check
        accuracy = self._check_accuracy(parsed_output, source_metadata)
        audit_report["dimensions"]["accuracy"] = accuracy
        
        # Dimension 3: Consistency Check
        consistency = self._check_consistency(parsed_output)
        audit_report["dimensions"]["consistency"] = consistency
        
        # Dimension 4: Validity Check (use validation results if available)
        if validation_results:
            validity = self._extract_validity_score(validation_results)
        else:
            validity = self._check_validity(parsed_output)
        audit_report["dimensions"]["validity"] = validity
        
        # Dimension 5: Format Compliance
        format_compliance = self._check_format_compliance(parsed_output)
        audit_report["dimensions"]["format_compliance"] = format_compliance
        
        # Calculate overall score (weighted average)
        weights = {
            "completeness": 0.25,
            "accuracy": 0.30,
            "consistency": 0.20,
            "validity": 0.15,
            "format_compliance": 0.10
        }
        
        overall_score = sum(
            audit_report["dimensions"][dim]["score"] * weights[dim]
            for dim in weights
        )
        
        audit_report["overall_score"] = overall_score
        audit_report["passed"] = overall_score >= self.OVERALL_PASS_THRESHOLD
        
        # Collect critical issues
        for dim, results in audit_report["dimensions"].items():
            if results.get("critical_issues"):
                audit_report["critical_issues"].extend(results["critical_issues"])
            if results.get("warnings"):
                audit_report["warnings"].extend(results["warnings"])
        
        # Generate recommendations
        audit_report["recommendations"] = self._generate_recommendations(audit_report)
        
        # Log summary
        logger.info(f"Quality audit complete: "
                   f"Passed={audit_report['passed']}, "
                   f"Score={overall_score:.2f}, "
                   f"Critical Issues={len(audit_report['critical_issues'])}")
        
        return audit_report["passed"], audit_report
    
    def _check_completeness(self, parsed_output: Dict, source_metadata: Dict = None) -> Dict:
        """Check data completeness"""
        result = {
            "score": 0.0,
            "critical_issues": [],
            "warnings": [],
            "details": {}
        }
        
        # Check if output is empty
        if not parsed_output:
            result["critical_issues"].append("Empty output - no data parsed")
            result["score"] = 0.0
            return result
        
        series_count = len(parsed_output)
        total_data_points = sum(
            len(series.get("values", {})) 
            for series in parsed_output.values()
        )
        
        result["details"]["series_count"] = series_count
        result["details"]["total_data_points"] = total_data_points
        
        # Check against expected counts (if metadata available)
        if source_metadata:
            expected_series = source_metadata.get("expected_series_count")
            expected_points = source_metadata.get("expected_data_points")
            
            if expected_series:
                series_completeness = min(series_count / expected_series, 1.0)
                result["details"]["series_completeness"] = series_completeness
                
                if series_completeness < 0.5:
                    result["critical_issues"].append(
                        f"Low series completeness: {series_completeness:.0%} "
                        f"(expected {expected_series}, got {series_count})"
                    )
                elif series_completeness < 0.8:
                    result["warnings"].append(
                        f"Moderate series completeness: {series_completeness:.0%}"
                    )
            
            if expected_points:
                points_completeness = min(total_data_points / expected_points, 1.0)
                result["details"]["points_completeness"] = points_completeness
                
                if points_completeness < 0.5:
                    result["critical_issues"].append(
                        f"Low data point completeness: {points_completeness:.0%}"
                    )
        
        # Check for series with no data
        empty_series = [
            code for code, data in parsed_output.items()
            if not data.get("values")
        ]
        
        if empty_series:
            result["warnings"].append(
                f"{len(empty_series)} series have no data points"
            )
            result["details"]["empty_series"] = empty_series[:5]  # Sample
        
        # Calculate score
        if result["critical_issues"]:
            result["score"] = 0.3  # Major issues
        elif result["warnings"]:
            result["score"] = 0.7  # Minor issues
        else:
            result["score"] = 1.0  # Complete
        
        return result
    
    def _check_accuracy(self, parsed_output: Dict, source_metadata: Dict = None) -> Dict:
        """Check data accuracy (spot checks)"""
        result = {
            "score": 0.95,  # Default high score (assume accurate unless issues found)
            "critical_issues": [],
            "warnings": [],
            "details": {}
        }
        
        # Check for obviously incorrect values
        for series_code, series_data in parsed_output.items():
            values = series_data.get("values", {})
            
            # Check for NaN, Infinity, or empty strings
            invalid_values = []
            for date, value in values.items():
                if value is None or value == '' or str(value).lower() in ['nan', 'inf', '-inf']:
                    invalid_values.append(date)
            
            if invalid_values:
                result["warnings"].append(
                    f"Series {series_code} has {len(invalid_values)} invalid values"
                )
                result["score"] -= 0.05
        
        result["score"] = max(result["score"], 0.0)
        
        return result
    
    def _check_consistency(self, parsed_output: Dict) -> Dict:
        """Check internal consistency"""
        result = {
            "score": 1.0,
            "critical_issues": [],
            "warnings": [],
            "details": {}
        }
        
        if not parsed_output:
            return result
        
        # Check date format consistency
        all_dates = []
        for series_data in parsed_output.values():
            all_dates.extend(series_data.get("values", {}).keys())
        
        if all_dates:
            # Check if dates follow ISO format
            iso_pattern = r'^\d{4}-\d{2}-\d{2}$'
            import re
            
            iso_dates = sum(1 for d in all_dates if re.match(iso_pattern, str(d)))
            iso_ratio = iso_dates / len(all_dates)
            
            result["details"]["iso_date_ratio"] = iso_ratio
            
            if iso_ratio < 0.8:
                result["warnings"].append(
                    f"Date format inconsistency: {iso_ratio:.0%} in ISO format"
                )
                result["score"] -= 0.1
        
        # Check value type consistency (all numeric)
        for series_code, series_data in parsed_output.items():
            values = series_data.get("values", {})
            
            numeric_count = 0
            for value in values.values():
                try:
                    float(value)
                    numeric_count += 1
                except:
                    pass
            
            if values:
                numeric_ratio = numeric_count / len(values)
                
                if numeric_ratio < 0.9:
                    result["warnings"].append(
                        f"Series {series_code} has mixed value types "
                        f"({numeric_ratio:.0%} numeric)"
                    )
                    result["score"] -= 0.1
        
        result["score"] = max(result["score"], 0.0)
        
        return result
    
    def _check_validity(self, parsed_output: Dict) -> Dict:
        """Basic validity checks"""
        result = {
            "score": 1.0,
            "critical_issues": [],
            "warnings": [],
            "details": {}
        }
        
        # Check for duplicate dates in series
        for series_code, series_data in parsed_output.items():
            dates = list(series_data.get("values", {}).keys())
            unique_dates = set(dates)
            
            if len(dates) != len(unique_dates):
                result["critical_issues"].append(
                    f"Series {series_code} has duplicate dates"
                )
                result["score"] -= 0.2
        
        result["score"] = max(result["score"], 0.0)
        
        return result
    
    def _extract_validity_score(self, validation_results: Dict) -> Dict:
        """Extract validity score from validation results"""
        return {
            "score": validation_results.get("confidence_score", 0.0),
            "critical_issues": [
                issue.get("details", {}).get("reason", "Unknown issue")
                for issue in validation_results.get("issues_found", [])
            ],
            "warnings": [
                w.get("message", "Unknown warning")
                for w in validation_results.get("warnings", [])
            ],
            "details": {
                "series_checked": validation_results.get("series_checked", 0),
                "issues_found": len(validation_results.get("issues_found", [])),
                "is_valid": validation_results.get("is_valid", False)
            }
        }
    
    def _check_format_compliance(self, parsed_output: Dict) -> Dict:
        """Check output format compliance"""
        result = {
            "score": 1.0,
            "critical_issues": [],
            "warnings": [],
            "details": {}
        }
        
        # Expected format: {series_code: {values: {date: value}}}
        for series_code, series_data in parsed_output.items():
            # Check structure
            if not isinstance(series_data, dict):
                result["critical_issues"].append(
                    f"Series {series_code} has incorrect structure"
                )
                result["score"] -= 0.2
                continue
            
            if "values" not in series_data:
                result["critical_issues"].append(
                    f"Series {series_code} missing 'values' key"
                )
                result["score"] -= 0.2
                continue
            
            values = series_data["values"]
            if not isinstance(values, dict):
                result["critical_issues"].append(
                    f"Series {series_code} values not in dict format"
                )
                result["score"] -= 0.2
        
        result["score"] = max(result["score"], 0.0)
        
        return result
    
    def _generate_recommendations(self, audit_report: Dict) -> List[str]:
        """Generate actionable recommendations based on audit results"""
        recommendations = []
        
        # Completeness recommendations
        completeness = audit_report["dimensions"]["completeness"]
        if completeness["score"] < 0.8:
            recommendations.append(
                "Review source file preprocessing - data may be getting dropped"
            )
        
        # Accuracy recommendations
        accuracy = audit_report["dimensions"]["accuracy"]
        if accuracy["score"] < 0.9:
            recommendations.append(
                "Review value extraction logic - invalid values detected"
            )
        
        # Consistency recommendations
        consistency = audit_report["dimensions"]["consistency"]
        if consistency["score"] < 0.9:
            recommendations.append(
                "Standardize date formats and value types across all series"
            )
        
        # Validity recommendations
        validity = audit_report["dimensions"]["validity"]
        if validity["score"] < 0.8:
            recommendations.append(
                "Enable semantic validation to catch data quality issues"
            )
        
        # Overall recommendations
        if audit_report["overall_score"] < 0.7:
            recommendations.append(
                "Consider routing this file type to Full Agentic track for better handling"
            )
        
        return recommendations
    
    def generate_report(self, audit_report: Dict) -> str:
        """Generate human-readable audit report"""
        lines = []
        
        lines.append("=" * 70)
        lines.append("QUALITY AUDIT REPORT")
        lines.append("=" * 70)
        lines.append(f"Timestamp: {audit_report['timestamp']}")
        lines.append(f"Overall Score: {audit_report['overall_score']:.2%}")
        lines.append(f"Status: {'PASSED [OK]' if audit_report['passed'] else 'FAILED [ERR]'}")
        lines.append("")
        
        lines.append("Quality Dimensions:")
        lines.append("-" * 70)
        for dim, results in audit_report["dimensions"].items():
            status = "[OK]" if results["score"] >= 0.8 else "[ERR]"
            lines.append(f"  {status} {dim.replace('_', ' ').title()}: {results['score']:.2%}")
        
        lines.append("")
        
        if audit_report["critical_issues"]:
            lines.append("Critical Issues:")
            lines.append("-" * 70)
            for issue in audit_report["critical_issues"]:
                lines.append(f"  [ERR] {issue}")
            lines.append("")
        
        if audit_report["warnings"]:
            lines.append("Warnings:")
            lines.append("-" * 70)
            for warning in audit_report["warnings"][:10]:  # Show max 10
                lines.append(f"  [WARN] {warning}")
            
            if len(audit_report["warnings"]) > 10:
                lines.append(f"  ... and {len(audit_report['warnings']) - 10} more warnings")
            lines.append("")
        
        if audit_report["recommendations"]:
            lines.append("Recommendations:")
            lines.append("-" * 70)
            for i, rec in enumerate(audit_report["recommendations"], 1):
                lines.append(f"  {i}. {rec}")
            lines.append("")
        
        lines.append("=" * 70)
        
        return "\n".join(lines)
