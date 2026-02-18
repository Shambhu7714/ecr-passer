import pandas as pd
import json
import os
from datetime import datetime

class OutputWriter:
    def __init__(self, output_dir="output"):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def write(self, data, base_filename, subfolder=None):
        """
        Saves data to JSON format (primary) and optionally Excel.
        
        Args:
            data: Dictionary format { "SERIES_CODE": { "values": { "date": value } } }
            base_filename: Base name for output files
            subfolder: Optional subfolder (e.g., "review_queue" for low-confidence results)
        """
        print(f"\n{'='*60}")
        print(f"📦 STEP 6: OUTPUT GENERATION")
        print(f"{'='*60}")
        
        # Create subfolder if specified
        if subfolder:
            output_path = os.path.join(self.output_dir, subfolder)
            if not os.path.exists(output_path):
                os.makedirs(output_path)
            print(f"📁 Saving to subfolder: {subfolder}/")
        else:
            output_path = self.output_dir
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 1. Save JSON (primary format)
        json_path = os.path.join(output_path, f"{base_filename}_{timestamp}.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"✅ JSON output saved: {json_path}")
        print(f"   📊 Total series: {len(data)}")
        
        # Calculate total data points
        total_points = sum(len(series['values']) for series in data.values() if 'values' in series)
        print(f"   📈 Total data points: {total_points}")
        
        # 2. Save Excel (optional, for human readability)
        excel_path = os.path.join(output_path, f"{base_filename}_{timestamp}.xlsx")
        self._save_excel(data, excel_path)
        
        return json_path
    
    def _save_excel(self, data, excel_path):
        """Convert JSON format to Excel for human readability."""
        try:
            # Convert to long format DataFrame
            rows = []
            for series_code, series_data in data.items():
                if 'values' not in series_data:
                    continue
                for date, value in series_data['values'].items():
                    rows.append({
                        'series_code': series_code,
                        'date': date,
                        'value': value
                    })
            
            if rows:
                df = pd.DataFrame(rows)
                df.to_excel(excel_path, index=False)
                print(f"✅ Excel output saved: {excel_path}")
            else:
                print(f"⚠️ No data to save to Excel")
        except Exception as e:
            print(f"⚠️ Could not save Excel: {str(e)}")