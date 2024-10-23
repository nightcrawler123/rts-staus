import sys
import subprocess
import importlib
import os
import glob
import logging
from datetime import datetime
import shutil
from tqdm import tqdm
import gc  # For garbage collection

# ==========================
# 1. Setup and Dependencies
# ==========================

# List of required packages
required_packages = [
    'pandas',
    'tqdm',
    'openpyxl'
]

# Function to install missing packages
def install_packages(packages):
    for package in packages:
        try:
            importlib.import_module(package)
        except ImportError:
            print(f"Package '{package}' not found. Installing...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# Install missing packages
install_packages(required_packages)

# Now import the installed packages
import pandas as pd
import openpyxl

# Setup logging at the very beginning to capture all events
logging.basicConfig(
    filename='data_processing_pandas.log',
    filemode='w',  # Overwrite log file each run
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

logging.info("Pandas-Based Data Processing Script Started.")
print("Pandas-Based Data Processing Script Started.")

# ==========================
# 2. Define CSV to Sheet Mapping
# ==========================

def map_csv_to_sheet(sheet_name):
    """
    Maps a sheet name to itself.
    Since CSVs are generated from sheet names, mapping is direct.
    """
    return sheet_name

# ==========================
# 3. Convert Excel Sheets to CSV
# ==========================

def convert_excel_to_csv(excel_path, temp_dir):
    """
    Converts each sheet in the Excel file to separate CSV files for faster processing.
    """
    try:
        # Ensure temp directory exists
        os.makedirs(temp_dir, exist_ok=True)
    
        # Load the Excel workbook
        excel_file = pd.ExcelFile(excel_path)
        sheet_names = excel_file.sheet_names
        logging.info(f"Found sheets: {sheet_names}")
        print(f"Found sheets: {sheet_names}")
    
        for sheet in tqdm(sheet_names, desc="Converting Sheets to CSV"):
            df = pd.read_excel(excel_path, sheet_name=sheet, engine='openpyxl')
            csv_path = os.path.join(temp_dir, f"{sheet}.csv")
            df.to_csv(csv_path, index=False, encoding='utf-8')
            logging.info(f"Converted sheet '{sheet}' to CSV at '{csv_path}'")
            print(f"Converted sheet '{sheet}' to CSV at '{csv_path}'")
    
    except Exception as e:
        logging.error(f"Error converting Excel to CSV: {e}")
        print(f"Error converting Excel to CSV: {e}")
        sys.exit(1)

# ==========================
# 4. Process CSV Files with Pandas
# ==========================

def process_csv_files(temp_dir, processed_dir, original_excel_columns):
    """
    Processes each CSV file using Pandas:
    - Deletes rows with the oldest date.
    - Removes duplicate rows.
    - Handles missing values.
    - Aligns columns with original Excel sheets.
    """
    try:
        os.makedirs(processed_dir, exist_ok=True)
        csv_files = glob.glob(os.path.join(temp_dir, "*.csv"))
    
        for csv_file in tqdm(csv_files, desc="Processing CSV Files"):
            sheet_name = os.path.splitext(os.path.basename(csv_file))[0]
            target_sheet = map_csv_to_sheet(sheet_name)
    
            # Check if the target sheet exists in original_excel_columns
            if target_sheet not in original_excel_columns:
                print(f"No corresponding Excel sheet for CSV '{csv_file}'. Skipping.")
                logging.warning(f"No corresponding Excel sheet for CSV '{csv_file}'. Skipping.")
                continue
    
            # Read CSV with Pandas
            try:
                df = pd.read_csv(csv_file, encoding='utf-8')
            except Exception as e:
                logging.error(f"Error reading CSV '{csv_file}': {e}")
                print(f"Error reading CSV '{csv_file}': {e}")
                continue
    
            if df.empty:
                print(f"CSV file '{csv_file}' is empty. Skipping.")
                logging.warning(f"CSV file '{csv_file}' is empty. Skipping.")
                continue
    
            # Ensure column names are strings and stripped
            df.columns = [str(col).strip() for col in df.columns]
    
            # Assume the first column is the date column
            date_column = df.columns[0]
            print(f"Identified date column in CSV: '{date_column}'")
            logging.info(f"Identified date column in CSV: '{date_column}'")
    
            # Convert the date column to datetime
            df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    
            # Find the oldest date
            oldest_date = df[date_column].min()
            if pd.isna(oldest_date):
                print(f"No valid dates found in CSV '{csv_file}'. Skipping deletion.")
                logging.warning(f"No valid dates found in CSV '{csv_file}'. Skipping deletion.")
            else:
                # Filter out rows with the oldest date
                initial_row_count = len(df)
                df = df[df[date_column] != oldest_date]
                final_row_count = len(df)
                rows_deleted = initial_row_count - final_row_count
                print(f"Deleted {rows_deleted} rows with the oldest date '{oldest_date.date()}' from CSV '{csv_file}'.")
                logging.info(f"Deleted {rows_deleted} rows with the oldest date '{oldest_date.date()}' from CSV '{csv_file}'.")
    
            # Handle missing values
            for col in df.columns:
                if df[col].dtype in ['float64', 'int64']:
                    mean_val = df[col].mean()
                    df[col].fillna(mean_val, inplace=True)
                elif pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col].fillna(pd.Timestamp('1970-01-01'), inplace=True)
                else:
                    df[col].fillna("Unknown", inplace=True)
    
            # Remove duplicate rows
            initial_row_count = len(df)
            df.drop_duplicates(inplace=True)
            final_row_count = len(df)
            duplicates_removed = initial_row_count - final_row_count
            if duplicates_removed > 0:
                print(f"Removed {duplicates_removed} duplicate rows from CSV '{csv_file}'.")
                logging.info(f"Removed {duplicates_removed} duplicate rows from CSV '{csv_file}'.")
    
            # Align columns with original Excel columns
            original_columns = original_excel_columns[target_sheet]
    
            # Add missing columns with 'Unknown'
            missing_columns = set(original_columns) - set(df.columns)
            for col in missing_columns:
                df[col] = "Unknown"
                logging.warning(f"Column '{col}' missing in CSV '{csv_file}'. Filled with 'Unknown'.")
                print(f"Column '{col}' missing in CSV '{csv_file}'. Filled with 'Unknown'.")
    
            # Reorder columns to match Excel sheet
            df = df[original_columns]
    
            # Save processed CSV
            processed_csv_path = os.path.join(processed_dir, f"{sheet_name}_processed.csv")
            df.to_csv(processed_csv_path, index=False, encoding='utf-8')
            logging.info(f"Processed CSV saved at '{processed_csv_path}'")
            print(f"Processed CSV saved at '{processed_csv_path}'")
    
            # Clear memory
            del df
            gc.collect()
    
    except Exception as e:
        logging.error(f"Error processing CSV files with Pandas: {e}")
        print(f"Error processing CSV files with Pandas: {e}")
        sys.exit(1)

# ==========================
# 5. Append Processed CSVs to Original Excel Sheets
# ==========================

def append_processed_csvs_to_excel(processed_dir, final_excel_path, original_excel_columns):
    """
    Appends processed CSV data to the original Excel sheets and saves to a new Excel file.
    """
    try:
        # Load the original Excel data into Pandas DataFrames
        excel_dfs = {}
        for sheet, cols in original_excel_columns.items():
            processed_csv = os.path.join(processed_dir, f"{sheet}_processed.csv")
            if os.path.exists(processed_csv):
                try:
                    df = pd.read_csv(processed_csv, encoding='utf-8')
                    excel_dfs[sheet] = df
                except Exception as e:
                    logging.error(f"Error reading processed CSV '{processed_csv}': {e}")
                    print(f"Error reading processed CSV '{processed_csv}': {e}")
                    continue
            else:
                logging.warning(f"Expected processed CSV for sheet '{sheet}' not found. Skipping.")
                print(f"Expected processed CSV for sheet '{sheet}' not found. Skipping.")
    
        # Process each processed CSV and append to the corresponding DataFrame
        processed_csv_files = glob.glob(os.path.join(processed_dir, "*_processed.csv"))
        for processed_csv in tqdm(processed_csv_files, desc="Appending to Excel Sheets"):
            sheet_name = os.path.basename(processed_csv).replace('_processed.csv', '')
            target_sheet = map_csv_to_sheet(sheet_name)
            if not target_sheet:
                print(f"No mapping found for processed CSV '{processed_csv}'. Skipping.")
                logging.warning(f"No mapping found for processed CSV '{processed_csv}'. Skipping.")
                continue
    
            if target_sheet not in excel_dfs:
                print(f"Sheet '{target_sheet}' not loaded. Skipping CSV '{processed_csv}'.")
                logging.warning(f"Sheet '{target_sheet}' not loaded. Skipping CSV '{processed_csv}'.")
                continue
    
            # Read the processed CSV
            try:
                pl_df_new = pd.read_csv(processed_csv, encoding='utf-8')
            except Exception as e:
                logging.error(f"Error reading processed CSV '{processed_csv}': {e}")
                print(f"Error reading processed CSV '{processed_csv}': {e}")
                continue
    
            # Append to the existing DataFrame
            excel_dfs[target_sheet] = pd.concat([excel_dfs[target_sheet], pl_df_new], ignore_index=True)
    
            logging.info(f"Appended data from '{processed_csv}' to sheet '{target_sheet}'.")
            print(f"Appended data from '{processed_csv}' to sheet '{target_sheet}'.")
    
            # Clear memory
            del pl_df_new
            gc.collect()
    
        # Save all DataFrames to a new Excel file
        with pd.ExcelWriter(final_excel_path, engine='openpyxl') as writer:
            for sheet, df in excel_dfs.items():
                df.to_excel(writer, sheet_name=sheet, index=False)
                logging.info(f"Saved sheet '{sheet}' with {df.shape[0]} rows.")
                print(f"Saved sheet '{sheet}' with {df.shape[0]} rows.")
    
        print(f"\nFinal Excel file saved at '{final_excel_path}'")
        logging.info(f"Final Excel file saved at '{final_excel_path}'")
    
    except Exception as e:
        logging.error(f"Error appending processed CSVs to Excel: {e}")
        print(f"Error appending processed CSVs to Excel: {e}")
        sys.exit(1)

# ==========================
# 6. Main Execution Flow
# ==========================

def main():
    try:
        # Define current working directory
        cwd = os.getcwd()
        print(f"Current Working Directory: {cwd}")
        logging.info(f"Current Working Directory: {cwd}")
    
        # Define the path to the Excel file
        excel_filename = 'NA Trend Report.xlsx'
        excel_path = os.path.join(cwd, excel_filename)
    
        # Check if the Excel file exists
        if not os.path.isfile(excel_path):
            print(f"Excel file '{excel_filename}' not found in the current directory.")
            logging.error(f"Excel file '{excel_filename}' not found in the current directory.")
            sys.exit(1)
    
        # Define temporary directories
        temp_csv_dir = os.path.join(cwd, "temp_csv")
        processed_csv_dir = os.path.join(cwd, "processed_csv")
        os.makedirs(temp_csv_dir, exist_ok=True)
        os.makedirs(processed_csv_dir, exist_ok=True)
    
        # Step 1: Convert Excel sheets to CSV for faster processing
        print("\n--- Step 1: Converting Excel Sheets to CSV ---")
        logging.info("Starting Step 1: Converting Excel Sheets to CSV")
        convert_excel_to_csv(excel_path, temp_csv_dir)
    
        # Step 2: Define original Excel sheet columns to ensure alignment
        # This assumes that the original Excel sheets have consistent columns
        # Adjust as necessary
        excel_file = pd.ExcelFile(excel_path)
        original_excel_columns = {}
        for sheet in excel_file.sheet_names:
            df = excel_file.parse(sheet_name=sheet)
            original_columns = list(df.columns)
            original_excel_columns[sheet] = original_columns
            logging.info(f"Original columns for sheet '{sheet}': {original_columns}")
        excel_file.close()
    
        # Step 3: Process CSV files using Pandas
        print("\n--- Step 2: Processing CSV Files with Pandas ---")
        logging.info("Starting Step 2: Processing CSV Files with Pandas")
        process_csv_files(temp_csv_dir, processed_csv_dir, original_excel_columns)
    
        # Step 4: Append processed CSVs to Excel sheets and save to a new Excel file
        print("\n--- Step 3: Appending Processed CSVs to Excel Sheets ---")
        logging.info("Starting Step 3: Appending Processed CSVs to Excel Sheets")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        final_excel_path = os.path.join(cwd, f"NA Trend Report_Final_{timestamp}.xlsx")
        append_processed_csvs_to_excel(processed_csv_dir, final_excel_path, original_excel_columns)
    
        # Optional: Clean up temporary directories
        try:
            shutil.rmtree(temp_csv_dir)
            shutil.rmtree(processed_csv_dir)
            print("\nCleaned up temporary directories.")
            logging.info("Cleaned up temporary directories.")
        except Exception as e:
            logging.warning(f"Error cleaning up temporary directories: {e}")
            print(f"Error cleaning up temporary directories: {e}")
    
        # Final message
        print("\nData processing completed successfully.")
        logging.info("Data processing completed successfully.")
    
    except Exception as e:
        logging.error(f"An unexpected error occurred in the main execution: {e}")
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
