import pandas as pd
import os
from datetime import datetime
from tqdm import tqdm
import time
import glob

class Timer:
    """Class to track execution time of different stages"""
    def __init__(self):
        self.start_time = time.time()
        self.stage_times = {}
    
    def mark_stage(self, stage_name):
        current_time = time.time()
        self.stage_times[stage_name] = current_time - self.start_time
        return current_time - self.start_time
    
    def print_summary(self):
        print("\nExecution Time Summary:")
        print("-" * 50)
        prev_time = 0
        for stage, total_time in self.stage_times.items():
            stage_duration = total_time - prev_time
            print(f"{stage:<30} : {stage_duration:>8.2f} seconds")
            prev_time = total_time
        print("-" * 50)
        print(f"Total execution time: {self.stage_times[list(self.stage_times.keys())[-1]]:.2f} seconds")

def get_mapping():
    """Returns mapping of CSV patterns to Excel sheet names."""
    return {
        'QDS-above-70-crossed-40d': 'QDS above 70 G40',
        'QDS-0-69-crossed-40d': 'QDS below 70 G40',
        'QDS-0-69-less-40d': 'QDS below 70 L40',
        'QDS-above-70-less-40d': 'QDS above 70 L40'
    }

def clean_filename(filename):
    """Remove 'processed' prefix from filename if present."""
    return filename.replace('processed_', '')

def process_excel_file(excel_path):
    """Process the Excel file by removing oldest date rows."""
    print("Loading Excel file... (This might take a few minutes for large files)")
    
    # Read Excel file using chunks to handle large file
    excel_data = {}
    for sheet_name in pd.ExcelFile(excel_path).sheet_names:
        chunks = []
        for chunk in pd.read_excel(excel_path, sheet_name=sheet_name, chunksize=10000):
            chunks.append(chunk)
        excel_data[sheet_name] = pd.concat(chunks, ignore_index=True)
    
    # Find oldest date across all sheets
    oldest_date = None
    for sheet_data in excel_data.values():
        if not sheet_data.empty:
            sheet_date = pd.to_datetime(sheet_data.iloc[:, 0]).min()
            if oldest_date is None or sheet_date < oldest_date:
                oldest_date = sheet_date
    
    # Remove rows with oldest date from all sheets
    for sheet_name in excel_data:
        excel_data[sheet_name] = excel_data[sheet_name][
            pd.to_datetime(excel_data[sheet_name].iloc[:, 0]) > oldest_date
        ]
    
    return excel_data

def main():
    # Initialize timer
    timer = Timer()
    
    # Get current working directory
    cwd = os.getcwd()
    excel_path = os.path.join(cwd, "NA Trend Report.xlsx")
    
    # Process Excel file
    print("Step 1: Processing Excel file")
    excel_data = process_excel_file(excel_path)
    timer.mark_stage("Excel Loading and Initial Processing")
    
    # Get CSV files from current directory
    csv_files = glob.glob(os.path.join(cwd, "*.csv"))
    mapping = get_mapping()
    
    print("\nStep 2: Processing CSV files and updating Excel sheets...")
    csv_count = 0
    for csv_file in tqdm(csv_files):
        base_name = clean_filename(os.path.basename(csv_file))
        
        # Check if file matches any pattern
        for pattern, sheet_name in mapping.items():
            if pattern in base_name:
                # Read CSV in chunks
                csv_chunks = pd.read_csv(csv_file, chunksize=10000)
                csv_data = pd.concat(csv_chunks, ignore_index=True)
                
                # Get header from Excel
                excel_header = excel_data[sheet_name].columns
                
                # Ensure CSV data types match Excel
                for col in csv_data.columns:
                    if col in excel_data[sheet_name].columns:
                        try:
                            csv_data[col] = csv_data[col].astype(
                                excel_data[sheet_name][col].dtype
                            )
                        except:
                            pass  # Keep original dtype if conversion fails
                
                # Update Excel data
                excel_data[sheet_name] = pd.concat(
                    [excel_data[sheet_name], csv_data],
                    ignore_index=True
                )
                csv_count += 1
                break
    
    timer.mark_stage("CSV Processing")
    print(f"\nProcessed {csv_count} CSV files")
    
    # Save updated Excel file
    print("\nStep 3: Saving Excel file...")
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        for sheet_name, data in tqdm(excel_data.items()):
            data.to_excel(writer, sheet_name=sheet_name, index=False)
    
    timer.mark_stage("Excel Saving")
    
    # Print timing summary
    timer.print_summary()

if __name__ == "__main__":
    main()
