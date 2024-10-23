import sys
import os
import glob
import time
import dask.dataframe as dd
import pandas as pd
from openpyxl import load_workbook
from tqdm import tqdm
from dask.diagnostics import ProgressBar
from datetime import datetime, timedelta
import site

# Add user's site-packages to path if not found in global
user_site_packages = site.getusersitepackages()
if user_site_packages not in sys.path:
    sys.path.append(user_site_packages)

# Timer class for tracking execution time
class Timer:
    """Class to track execution time of different stages."""
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
            print(f"{stage:<30} : {str(timedelta(seconds=stage_duration))}")
            prev_time = total_time
        print("-" * 50)
        total_time = self.stage_times[list(self.stage_times.keys())[-1]]
        print(f"Total execution time: {str(timedelta(seconds=total_time))}")

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

def load_excel_sheets(excel_path, mapping):
    """Loads Excel sheets using pandas to Dask."""
    try:
        print("Loading Excel sheets... This might take some time.")
        wb = load_workbook(excel_path, read_only=True)
        sheet_names = wb.sheetnames
        wb.close()

        # Load the sheets into Dask DataFrames
        excel_data = {}
        for sheet_name in tqdm(mapping.values(), desc="Loading Excel sheets"):
            if sheet_name in sheet_names:
                df = pd.read_excel(excel_path, sheet_name=sheet_name, dtype=str)
                ddf = dd.from_pandas(df, npartitions=4)  # Convert pandas to Dask
                excel_data[sheet_name] = ddf
            else:
                print(f"Warning: Sheet '{sheet_name}' not found in Excel file.")

        return excel_data
    except Exception as e:
        print(f"Error loading Excel file: {str(e)}")
        return {}

def process_csv_files_and_update_excel(excel_data, mapping, csv_files):
    """
    Process CSV files and update corresponding Excel sheets using Dask.
    """
    try:
        csv_count = 0
        with ProgressBar():
            for csv_file in tqdm(csv_files, desc="Processing CSV files"):
                base_name = clean_filename(os.path.basename(csv_file))

                # Check if file matches any pattern
                for pattern, sheet_name in mapping.items():
                    if pattern in base_name:
                        print(f"Processing file '{csv_file}' for sheet '{sheet_name}'")
                        try:
                            # Read CSV with Dask
                            csv_data = dd.read_csv(csv_file, dtype=str)

                            # Combine CSV and Excel data
                            if sheet_name in excel_data:
                                combined_data = dd.concat([excel_data[sheet_name], csv_data])

                                # Update the Excel data
                                excel_data[sheet_name] = combined_data

                            csv_count += 1
                        except Exception as e:
                            print(f"Error processing CSV file '{csv_file}' for sheet '{sheet_name}': {str(e)}")
                        break
        return csv_count
    except Exception as e:
        print(f"Error updating Excel file: {str(e)}")
        return 0

def save_updated_excel(excel_data, excel_path):
    """Save the updated Excel sheets back to a new Excel file with a timestamp."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_excel_path = os.path.join(os.getcwd(), f"NA Trend Report_{timestamp}.xlsx")

        print(f"\nSaving the updated Excel sheets to '{new_excel_path}'. This might take some time.")
        with pd.ExcelWriter(new_excel_path, engine='openpyxl') as writer:
            for sheet_name, data in tqdm(excel_data.items(), desc="Saving sheets"):
                # Compute the final Dask DataFrame
                df = data.compute()
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        print(f"Excel file saved as: {new_excel_path}")
    except Exception as e:
        print(f"Error saving Excel file: {str(e)}")

def main():
    try:
        # Initialize timer
        timer = Timer()

        # Get current working directory
        cwd = os.getcwd()
        excel_path = os.path.join(cwd, "NA Trend Report.xlsx")

        # Check if Excel file exists
        if not os.path.exists(excel_path):
            print(f"Excel file not found: {excel_path}")
            return

        # Step 1: Load the Excel sheets into memory using Dask
        print("Step 1: Loading Excel sheets...")
        mapping = get_mapping()
        excel_data = load_excel_sheets(excel_path, mapping)
        timer.mark_stage("Excel Loading")

        # Get CSV files from the current directory
        csv_files = glob.glob(os.path.join(cwd, "*.csv"))
        if not csv_files:
            print("Warning: No CSV files found in the current directory.")
            return

        # Step 2: Process CSV files and update corresponding Excel sheets
        print("\nStep 2: Processing CSV files and updating Excel sheets...")
        csv_count = process_csv_files_and_update_excel(excel_data, mapping, csv_files)
        timer.mark_stage("CSV Processing")
        print(f"\nProcessed {csv_count} CSV files.")

        # Step 3: Save the updated Excel file with a timestamp
        print("\nStep 3: Saving updated Excel file...")
        save_updated_excel(excel_data, excel_path)
        timer.mark_stage("Excel Saving")

        # Print timing summary
        timer.print_summary()

    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
