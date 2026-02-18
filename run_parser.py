import argparse
import os
import sys
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv is optional
from agents.supervisor import ParserSupervisor

def main():
    parser = argparse.ArgumentParser(
          description="ECR Time-Series Parser",
          formatter_class=argparse.RawDescriptionHelpFormatter,
          epilog="""
     Examples:
         python run_parser.py --input sample.xlsx --mapping config/mapping.xlsx
     """,
     )
    parser.add_argument('--input', "-i", required=True, help='Path to the input Excel file')
    parser.add_argument('--mapping', "-m", required=False, default='config/Argentina_Map_Updated.xlsm',
                        help='Path to the mapping file (default: config/Argentina_Map_Updated.xlsm)')
    parser.add_argument('--pattern', "-p", required=False, default='config/Patterns.xlsx', 
                        help='Path to the pattern file (default: config/Patterns.xlsx)')

    args = parser.parse_args()

    # Validate input file paths
    if not os.path.isfile(args.input):
        print(f"Error: Input file '{args.input}' does not exist.")
        sys.exit(1)

    if not os.path.isfile(args.mapping):
        print(f"Error: Mapping file '{args.mapping}' does not exist.")
        sys.exit(1)

    try:
        supervisor = ParserSupervisor(args.mapping, args.pattern)
        output_file = supervisor.run_pipeline(args.input)
        if output_file:
            print(f"Parsing completed successfully. Output saved to: {output_file}")
            sys.exit(0)
        else:
            print("Parsing failed. No output file generated.")
            sys.exit(1)
    except Exception as e:
        print(f"An error occurred during parsing: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)     

if __name__ == "__main__":
    main()