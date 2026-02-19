"""
Example: Hybrid Agentic Parser with Multi-Track Routing

This script demonstrates the full hybrid agentic architecture:
- Complexity-based routing (Fast/Hybrid/Full tracks)
- Pattern caching for fast processing
- Semantic validation
- Hierarchy extraction
- Quality auditing
- Continuous learning feedback loop
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from agents.hybrid_supervisor import HybridSupervisor
from core.logger import get_logger

logger = get_logger()


def main():
    """
    Run the hybrid agentic parser on sample files
    """
    
    print("=" * 80)
    print("HYBRID AGENTIC PARSER - Multi-Track Architecture Demo")
    print("=" * 80)
    print()
    
    # Configuration
    mapping_file = "config/Argentina_Map_Updated.xlsx"
    pattern_file = "config/Patterns.xlsx" if os.path.exists("config/Patterns.xlsx") else None

    # Sample input files   use files that actually exist in data/
    test_files = [
        f for f in [
            "data/sample.xlsx",
            "data/sample1.xlsx",
            "data/sample2.xlsx",
        ] if os.path.exists(f)
    ]

    if not test_files:
        print("[WARN] No input files found in data/. Please place Excel files in the data/ folder.")
        print("    Expected: data/sample.xlsx, data/sample1.xlsx, or data/sample2.xlsx")
        return

    # Initialize hybrid supervisor
    print("[INIT] Initializing Hybrid Agentic Supervisor...")
    print()
    
    supervisor = HybridSupervisor(
        mapping_file=mapping_file,
        pattern_file=pattern_file,
        enable_hybrid=True  # Enable multi-track routing
    )
    
    print("[OK] Hybrid supervisor initialized")
    print()
    
    # Process each file
    results = []
    
    for input_file in test_files:
        if not os.path.exists(input_file):
            logger.warning(f"[WARN] File not found: {input_file}")
            continue
        
        print(f"\n{'=' * 80}")
        print(f"Processing: {os.path.basename(input_file)}")
        print(f"{'=' * 80}\n")
        
        try:
            # Run hybrid pipeline
            output_path = supervisor.run_pipeline(input_file, base_year=2024)
            
            if output_path:
                print(f"\n[OK] SUCCESS: Output saved to {output_path}")
                results.append({
                    "file": input_file,
                    "status": "success",
                    "output": output_path
                })
            else:
                print(f"\n[ERR] FAILED: Processing failed")
                results.append({
                    "file": input_file,
                    "status": "failed"
                })
        
        except Exception as e:
            print(f"\n[ERR] ERROR: {str(e)}")
            logger.error(f"Processing error: {e}", exc_info=True)
            results.append({
                "file": input_file,
                "status": "error",
                "error": str(e)
            })
    
    # Summary
    print("\n" + "=" * 80)
    print("PROCESSING SUMMARY")
    print("=" * 80)
    
    success_count = sum(1 for r in results if r["status"] == "success")
    failed_count = sum(1 for r in results if r["status"] == "failed")
    error_count = sum(1 for r in results if r["status"] == "error")
    
    print(f"\nTotal Files: {len(results)}")
    print(f"  [OK] Successful: {success_count}")
    print(f"  [ERR] Failed: {failed_count}")
    print(f"  [WARN] Errors: {error_count}")
    
    # Routing statistics
    print("\n" + "-" * 80)
    print("ROUTING STATISTICS")
    print("-" * 80)
    
    if supervisor.router:
        router_stats = supervisor.router.get_statistics()
        print(f"\nTrack Distribution:")
        print(f"  Fast Track: {router_stats.get('fast_track_count', 0)} "
              f"({router_stats.get('fast_track_pct', 0):.1f}%)")
        print(f"  Hybrid Track: {router_stats.get('hybrid_track_count', 0)} "
              f"({router_stats.get('hybrid_track_pct', 0):.1f}%)")
        print(f"  Full Agentic: {router_stats.get('full_agentic_count', 0)} "
              f"({router_stats.get('full_agentic_pct', 0):.1f}%)")
        
        # Cache statistics
        cache_stats = supervisor.router.get_cache_statistics()
        if cache_stats:
            print(f"\nCache Performance:")
            print(f"  Total Patterns: {cache_stats.get('total_patterns', 0)}")
            print(f"  Cache Hits: {cache_stats.get('cache_hits', 0)}")
            print(f"  Hit Rate: {cache_stats.get('hit_rate_pct', 0):.1f}%")
            print(f"  Fast Track Count: {cache_stats.get('fast_track_count', 0)}")
        
        # Feedback insights
        feedback_insights = supervisor.router.get_feedback_insights()
        if feedback_insights and "metrics" in feedback_insights:
            metrics = feedback_insights["metrics"]
            print(f"\nLearning Metrics:")
            print(f"  Total Processed: {metrics.get('total_files_processed', 0)}")
            print(f"  Success Rate: "
                  f"{metrics.get('successful_parses', 0) / max(metrics.get('total_files_processed', 1), 1):.1%}")
            print(f"  Avg Processing Time: {metrics.get('avg_processing_time', 0):.2f}s")
            print(f"  Avg Quality Score: {metrics.get('avg_quality_score', 0):.2%}")
    
    print("\n" + "=" * 80)
    print()


def demonstrate_components():
    """
    Demonstrate individual components of the hybrid architecture
    """
    print("\n" + "=" * 80)
    print("HYBRID ARCHITECTURE COMPONENTS DEMO")
    print("=" * 80 + "\n")
    
    # 1. Complexity Analyzer
    print("1. COMPLEXITY ANALYZER")
    print("-" * 80)
    from core.complexity_analyzer import ComplexityAnalyzer
    from core.preprocessing import preprocess_excel
    
    analyzer = ComplexityAnalyzer()
    
    test_file = next(
        (f for f in ["data/sample.xlsx", "data/sample1.xlsx", "data/sample2.xlsx"]
         if os.path.exists(f)), None
    )
    if test_file:
        df = preprocess_excel(test_file)
        track, score, details = analyzer.analyze(test_file, df)
        
        print(f"File: {os.path.basename(test_file)}")
        print(f"  Recommended Track: {track.value}")
        print(f"  Complexity Score: {score:.1f}")
        print(f"  Reason: {details.get('reason')}")
    
    # 2. Cache Manager
    print("\n2. CACHE MANAGER")
    print("-" * 80)
    from core.cache_manager import PatternCache
    
    cache = PatternCache()
    stats = cache.get_statistics()
    
    print(f"Total Patterns: {stats.get('total_patterns', 0)}")
    print(f"Cache Hit Rate: {stats.get('hit_rate_pct', 0):.1f}%")
    
    patterns = cache.list_patterns(min_confidence=0.7)
    if patterns:
        print(f"\nTop Patterns:")
        for pattern in patterns[:3]:
            print(f"    {pattern['file_pattern']}: "
                  f"{pattern['confidence']:.2%} confidence "
                  f"({pattern['success_count']} successes)")
    
    # 3. Feedback Loop
    print("\n3. FEEDBACK LOOP")
    print("-" * 80)
    from core.feedback_loop import FeedbackLoop
    
    feedback = FeedbackLoop()
    insights = feedback.get_insights_summary()
    
    print(f"Successful Patterns: {insights.get('successful_patterns', 0)}")
    print(f"Problematic Patterns: {insights.get('problematic_patterns', 0)}")
    print(f"Error Patterns: {insights.get('error_patterns', 0)}")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Hybrid Agentic Parser Demo")
    parser.add_argument("--demo", action="store_true", 
                       help="Demonstrate individual components")
    parser.add_argument("--file", type=str, 
                       help="Process a specific file")
    
    args = parser.parse_args()
    
    if args.demo:
        demonstrate_components()
    elif args.file:
        # Process specific file
        supervisor = HybridSupervisor(
            mapping_file="config/Argentina_Map_Updated.xlsx",
            pattern_file="config/Patterns.xlsx" if os.path.exists("config/Patterns.xlsx") else None,
            enable_hybrid=True
        )
        output = supervisor.run_pipeline(args.file)
        if output:
            print(f"[OK] Output: {output}")
    else:
        # Run main demo
        main()
