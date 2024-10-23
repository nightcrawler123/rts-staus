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
    'openpyxl',
    'tqdm'
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
    filename='data_processing.log',
    filemode='w',  # Overwrite log file each run
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

logging.info("Script started.")
print("Script started.")

# ==========================
# 2. Define CSV to Sheet Mapping
# ==========================

def map_csv_to_sheet(csv_filename):
    """
    Maps a CSV filename to the corresponding Excel sheet name based on predefined patterns.
    Adjust the mapping dictionary as per your actual file and sheet names.
    """
    basename = os.path.splitext(csv_filename)[0]
    mapping = {
        'QDS-above-70-crossed-40d': 'QDS above 70 G40',
        'QDS-0-69-crossed-40d': 'QDS below 70 G40',
        'QDS-0-69-less-40d': 'QDS below 70 L40',
        'QDS-above-70-less-40d': 'QDS above 70 L40'
    }
    return mapping.get(basename, None)

# ==========================
# 3. Handle Duplicate Columns
# ==========================

def handle_duplicate_columns(df):
    """
    Renames duplicate columns by appending suffixes to ensure uniqueness.
    For example, if 'Sales' appears twice, they become 'Sales' and 'Sales_1'.
    """
    try:
        cols = pd.Series(df.columns)
        for dup in cols[cols.duplicated()].unique():
            dup_indices = cols[cols == dup].index.tolist()
            for i, idx in enumerate(dup_indices):
                if i != 0:
                    new_name = f"{dup}_{i}"
                    logging.warning(f"Duplicate column '{dup}' found. Renaming to '{new_name}'.")
                    cols[idx] = new_name
                    print(f"Duplicate column '{dup}' found. Renaming to '{new_name}'.")
        df.columns = cols
        return df
    except Exception as e:
        logging.error(f"Error handling duplicate columns: {e}")
        print(f"Error handling duplicate columns: {e}")
        return df

# ==========================
# 4. Process Excel Sheets
# ==========================

def find_and_delete_oldest_date_rows(excel_path):
    """
    Processes each sheet in the Excel file by deleting rows with the oldest date in the first column.
    Also removes duplicate rows.
    Handles duplicate column headers by renaming them to ensure uniqueness.
    Prints and logs the details of deletions.
    Returns a dictionary of processed DataFrames.
    """
    try:
        # Load the Excel file
        xl = pd.ExcelFile(excel_path, engine='openpyxl')
        sheet_names = xl.sheet_names
        logging.info(f"Found sheets: {sheet_names}")
        print(f"Found sheets: {sheet_names}")

        processed_sheets = {}

        for sheet in tqdm(sheet_names, desc="Processing Excel Sheets"):
            print(f"\nProcessing sheet: {sheet}")
            logging.info(f"Processing sheet: {sheet}")

            # Read the sheet into a DataFrame
            df = xl.parse(sheet_name=sheet)

            if df.empty:
                print(f"Sheet '{sheet}' is empty. Skipping.")
                logging.warning(f"Sheet '{sheet}' is empty. Skipping.")
                processed_sheets[sheet] = df
                continue

            # Handle duplicate column headers by renaming them
            df = handle_duplicate_columns(df)

            # Assume the first column contains dates
            date_column = df.columns[0]
            print(f"Identified date column: '{date_column}'")
            logging.info(f"Identified date column: '{date_column}'")

            # Convert the first column to datetime
            df[date_column] = pd.to_datetime(df[date_column], errors='coerce')

            # Find the oldest date
            oldest_date = df[date_column].min()
            if pd.isnull(oldest_date):
                print(f"No valid dates found in sheet '{sheet}'. Skipping deletion.")
                logging.warning(f"No valid dates found in sheet '{sheet}'. Skipping deletion.")
            else:
                # Identify rows with the oldest date
                rows_to_delete = df[df[date_column] == oldest_date]
                num_rows_deleted = rows_to_delete.shape[0]

                # Delete these rows
                df = df[df[date_column] != oldest_date]

                print(f"Deleted {num_rows_deleted} rows with the oldest date '{oldest_date.date()}' from sheet '{sheet}'.")
                logging.info(f"Deleted {num_rows_deleted} rows with the oldest date '{oldest_date.date()}' from sheet '{sheet}'.")

            # Remove duplicate rows
            initial_row_count = df.shape[0]
            df.drop_duplicates(inplace=True)
            final_row_count = df.shape[0]
            duplicates_removed = initial_row_count - final_row_count

            if duplicates_removed > 0:
                print(f"Removed {duplicates_removed} duplicate rows from sheet '{sheet}'.")
                logging.info(f"Removed {duplicates_removed} duplicate rows from sheet '{sheet}'.")

            # Assign the processed DataFrame to the dictionary
            processed_sheets[sheet] = df

            # Log the current state
            logging.info(f"Sheet '{sheet}' now has {df.shape[0]} rows and {df.shape[1]} columns.")

            # Clear memory
            del df
            gc.collect()

        return processed_sheets

    except Exception as e:
        logging.error(f"Error processing Excel sheets: {e}")
        print(f"Error processing Excel sheets: {e}")
        sys.exit(1)

# ==========================
# 5. Process CSV Files
# ==========================

def append_csv_to_excel_sheet(processed_sheets, csv_path, excel_path):
    """
    Appends data from a CSV file to the corresponding Excel sheet.
    Handles missing values, inconsistent data types, and special characters.
    Provides console feedback and logs the operations.
    """
    try:
        csv_filename = os.path.basename(csv_path)
        sheet_name = map_csv_to_sheet(csv_filename)

        if not sheet_name:
            print(f"No mapping found for CSV file '{csv_filename}'. Skipping.")
            logging.warning(f"No mapping found for CSV file '{csv_filename}'. Skipping.")
            return

        if sheet_name not in processed_sheets:
            print(f"Sheet '{sheet_name}' not found in Excel file. Skipping CSV '{csv_filename}'.")
            logging.warning(f"Sheet '{sheet_name}' not found in Excel file. Skipping CSV '{csv_filename}'.")
            return

        print(f"\nAppending CSV '{csv_filename}' to sheet '{sheet_name}'")
        logging.info(f"Appending CSV '{csv_filename}' to sheet '{sheet_name}'")

        # Read the CSV file
        df_csv = pd.read_csv(csv_path, encoding='utf-8')

        if df_csv.empty:
            print(f"CSV file '{csv_filename}' is empty. Skipping.")
            logging.warning(f"CSV file '{csv_filename}' is empty. Skipping.")
            return

        # Handle duplicate columns in CSV
        df_csv = handle_duplicate_columns(df_csv)

        # Assume the first column is the date column
        date_column = df_csv.columns[0]
        print(f"Identified date column in CSV: '{date_column}'")
        logging.info(f"Identified date column in CSV: '{date_column}'")

        # Convert the date column to datetime
        df_csv[date_column] = pd.to_datetime(df_csv[date_column], errors='coerce')

        # Handle missing values
        # For numerical columns, fill missing values with the mean
        # For categorical/text columns, fill missing values with 'Unknown'
        for col in df_csv.columns:
            if pd.api.types.is_numeric_dtype(df_csv[col]):
                mean_val = df_csv[col].mean()
                df_csv[col].fillna(mean_val, inplace=True)
            elif pd.api.types.is_datetime64_any_dtype(df_csv[col]):
                df_csv[col].fillna(pd.Timestamp('1970-01-01'), inplace=True)
            else:
                df_csv[col].fillna('Unknown', inplace=True)

        # Remove duplicate rows
        initial_row_count = df_csv.shape[0]
        df_csv.drop_duplicates(inplace=True)
        final_row_count = df_csv.shape[0]
        duplicates_removed = initial_row_count - final_row_count

        if duplicates_removed > 0:
            print(f"Removed {duplicates_removed} duplicate rows from CSV '{csv_filename}'.")
            logging.info(f"Removed {duplicates_removed} duplicate rows from CSV '{csv_filename}'.")

        # Align CSV columns with Excel sheet columns
        excel_columns = processed_sheets[sheet_name].columns.tolist()
        csv_columns = df_csv.columns.tolist()

        # Ensure the CSV has the same columns as Excel sheet
        # If not, handle accordingly (e.g., add missing columns or reorder)
        for col in excel_columns:
            if col not in csv_columns:
                df_csv[col] = 'Unknown'  # Fill missing columns with 'Unknown'
                logging.warning(f"Column '{col}' missing in CSV '{csv_filename}'. Filled with 'Unknown'.")
                print(f"Column '{col}' missing in CSV '{csv_filename}'. Filled with 'Unknown'.")
        df_csv = df_csv[excel_columns]  # Reorder columns to match Excel sheet

        # Append the CSV data to the corresponding Excel sheet DataFrame
        processed_sheets[sheet_name] = pd.concat([processed_sheets[sheet_name], df_csv], ignore_index=True)

        print(f"Appended {df_csv.shape[0]} rows from CSV '{csv_filename}' to sheet '{sheet_name}'.")
        logging.info(f"Appended {df_csv.shape[0]} rows from CSV '{csv_filename}' to sheet '{sheet_name}'.")

        # Clear memory
        del df_csv
        gc.collect()

    except Exception as e:
        logging.error(f"Error appending CSV '{csv_path}' to Excel sheet: {e}")
        print(f"Error appending CSV '{csv_path}' to Excel sheet: {e}")

# ==========================
# 6. Save the Updated Excel File
# ==========================

def save_to_new_excel(processed_sheets, original_excel_path):
    """
    Saves the processed DataFrames to a new Excel file with a timestamp.
    Creates a backup of the original Excel file.
    """
    try:
        # Create backup of the original Excel file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f"{os.path.splitext(original_excel_path)[0]}_backup_{timestamp}.xlsx"
        shutil.copy2(original_excel_path, backup_path)
        print(f"Backup of the original Excel file created at '{backup_path}'")
        logging.info(f"Backup of the original Excel file created at '{backup_path}'")

        # Define the new Excel file path
        new_excel_path = f"{os.path.splitext(original_excel_path)[0]}_Final_{timestamp}.xlsx"

        # Write all processed sheets to the new Excel file
        with pd.ExcelWriter(new_excel_path, engine='openpyxl') as writer:
            for sheet_name, df in processed_sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"Saved sheet '{sheet_name}' with {df.shape[0]} rows.")
                logging.info(f"Saved sheet '{sheet_name}' with {df.shape[0]} rows.")

        print(f"\nFinal Excel file saved at '{new_excel_path}'")
        logging.info(f"Final Excel file saved at '{new_excel_path}'")

    except Exception as e:
        logging.error(f"Error saving the new Excel file: {e}")
        print(f"Error saving the new Excel file: {e}")
        sys.exit(1)

# ==========================
# 7. Main Execution Flow
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

        # Step 1: Process Excel sheets by deleting oldest date rows and removing duplicates
        print("\n--- Step 1: Processing Excel Sheets ---")
        logging.info("Starting Step 1: Processing Excel Sheets")
        processed_sheets = find_and_delete_oldest_date_rows(excel_path)

        # Step 2: Process each CSV file and append data to the corresponding Excel sheet
        print("\n--- Step 2: Processing CSV Files ---")
        logging.info("Starting Step 2: Processing CSV Files")

        # Find all CSV files in the current directory
        csv_pattern = os.path.join(cwd, "*.csv")
        csv_files = glob.glob(csv_pattern)

        if not csv_files:
            print("No CSV files found in the current directory.")
            logging.warning("No CSV files found in the current directory.")
        else:
            for csv_file in tqdm(csv_files, desc="Appending CSV Files"):
                append_csv_to_excel_sheet(processed_sheets, csv_file, excel_path)

        # Step 3: Save the processed data to a new Excel file
        print("\n--- Step 3: Saving the Updated Excel File ---")
        logging.info("Starting Step 3: Saving the Updated Excel File")
        save_to_new_excel(processed_sheets, excel_path)

        # Final message
        print("\nData processing completed successfully.")
        logging.info("Data processing completed successfully.")

    except Exception as e:
        logging.error(f"An unexpected error occurred in the main execution: {e}")
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
