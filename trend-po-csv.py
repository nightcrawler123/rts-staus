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

# Setup logging to capture detailed information
logging.basicConfig(
    filename='data_processing_pandas.log',
    filemode='w',  # Overwrite log file each run
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

logging.info("Pandas-Based Data Processing Script Started.")
print("Pandas-Based Data Processing Script Started.")

# ==========================
# 2. Define Helper Functions
# ==========================

def delete_oldest_date_rows(df, date_column):
    """
    Deletes all rows with the oldest date in the specified date column.
    
    Parameters:
        df (pd.DataFrame): The DataFrame to process.
        date_column (str): The name of the date column.
        
    Returns:
        df (pd.DataFrame): The DataFrame after deletion.
        oldest_date (datetime): The oldest date that was deleted.
    """
    # Convert date column to datetime if not already
    if not pd.api.types.is_datetime64_any_dtype(df[date_column]):
        df[date_column] = pd.to_datetime(df[date_column], format='%m/%d/%Y', errors='coerce')
    
    # Find the oldest date
    oldest_date = df[date_column].min()
    
    if pd.isna(oldest_date):
        print(f"No valid dates found in column '{date_column}'. No rows deleted.")
        logging.warning(f"No valid dates found in column '{date_column}'. No rows deleted.")
        return df, None
    
    # Delete all rows with the oldest date
    initial_row_count = len(df)
    df = df[df[date_column] != oldest_date]
    final_row_count = len(df)
    rows_deleted = initial_row_count - final_row_count
    
    print(f"Deleted {rows_deleted} rows with the oldest date '{oldest_date.strftime('%m/%d/%Y')}'.")
    logging.info(f"Deleted {rows_deleted} rows with the oldest date '{oldest_date.strftime('%m/%d/%Y')}'.")
    
    return df, oldest_date

def append_new_data(existing_df, new_df, date_column, excel_row_limit=1048576):
    """
    Appends new data to the existing DataFrame. If appending exceeds the Excel row limit,
    deletes the oldest date rows until there is enough space.
    
    Parameters:
        existing_df (pd.DataFrame): The existing DataFrame.
        new_df (pd.DataFrame): The new DataFrame to append.
        date_column (str): The name of the date column.
        excel_row_limit (int): Maximum number of rows allowed in Excel.
        
    Returns:
        updated_df (pd.DataFrame): The updated DataFrame after appending.
    """
    # Ensure date columns are datetime
    if not pd.api.types.is_datetime64_any_dtype(existing_df[date_column]):
        existing_df[date_column] = pd.to_datetime(existing_df[date_column], format='%m/%d/%Y', errors='coerce')
    if not pd.api.types.is_datetime64_any_dtype(new_df[date_column]):
        new_df[date_column] = pd.to_datetime(new_df[date_column], format='%m/%d/%Y', errors='coerce')
    
    # Calculate total rows after appending
    total_rows = len(existing_df) + len(new_df)
    
    # Delete oldest date rows until within limit
    while total_rows > excel_row_limit:
        # Find the oldest date
        oldest_date = existing_df[date_column].min()
        if pd.isna(oldest_date):
            print("No valid dates to delete. Cannot append more data.")
            logging.error("No valid dates to delete. Cannot append more data.")
            break
        
        # Delete rows with the oldest date
        initial_row_count = len(existing_df)
        existing_df = existing_df[existing_df[date_column] != oldest_date]
        final_row_count = len(existing_df)
        rows_deleted = initial_row_count - final_row_count
        
        print(f"Deleted {rows_deleted} rows with the oldest date '{oldest_date.strftime('%m/%d/%Y')}' to make space.")
        logging.info(f"Deleted {rows_deleted} rows with the oldest date '{oldest_date.strftime('%m/%d/%Y')}' to make space.")
        
        # Update total rows
        total_rows = len(existing_df) + len(new_df)
    
    # Append the new data
    updated_df = pd.concat([existing_df, new_df], ignore_index=True)
    print(f"Appended {len(new_df)} new rows. Total rows now: {len(updated_df)}.")
    logging.info(f"Appended {len(new_df)} new rows. Total rows now: {len(updated_df)}.")
    
    return updated_df

# ==========================
# 3. Main Processing Functions
# ==========================

def process_excel_file(excel_path, new_data_dir, final_excel_path):
    """
    Processes the Excel file by deleting oldest date rows and appending new data.
    
    Parameters:
        excel_path (str): Path to the original Excel file.
        new_data_dir (str): Directory containing new CSV files to append.
        final_excel_path (str): Path to save the final Excel file.
    """
    try:
        # Read the Excel file
        excel_file = pd.ExcelFile(excel_path, engine='openpyxl')
        sheet_names = excel_file.sheet_names
        logging.info(f"Found sheets: {sheet_names}")
        print(f"Found sheets: {sheet_names}")
        
        # Dictionary to hold processed DataFrames
        processed_dfs = {}
        
        for sheet in tqdm(sheet_names, desc="Processing Sheets"):
            # Read each sheet into a DataFrame
            df = pd.read_excel(excel_path, sheet_name=sheet, engine='openpyxl')
            
            if df.empty:
                print(f"Sheet '{sheet}' is empty. Skipping.")
                logging.warning(f"Sheet '{sheet}' is empty. Skipping.")
                processed_dfs[sheet] = df
                continue
            
            # Assume the first column is the date column
            date_column = df.columns[0]
            
            # Delete oldest date rows
            df, deleted_date = delete_oldest_date_rows(df, date_column)
            
            # Append new data if available
            new_csv_path = os.path.join(new_data_dir, f"{sheet}.csv")
            if os.path.exists(new_csv_path):
                new_df = pd.read_csv(new_csv_path, encoding='utf-8')
                
                if new_df.empty:
                    print(f"New data CSV for sheet '{sheet}' is empty. No data appended.")
                    logging.warning(f"New data CSV for sheet '{sheet}' is empty. No data appended.")
                else:
                    # Ensure column alignment
                    missing_cols = set(df.columns) - set(new_df.columns)
                    for col in missing_cols:
                        new_df[col] = "Unknown"
                    
                    # Reorder new_df columns to match df
                    new_df = new_df[df.columns]
                    
                    # Handle data types if necessary
                    # (Assuming data types are consistent; otherwise, add type conversion here)
                    
                    # Append new data, ensuring Excel row limit
                    df = append_new_data(df, new_df, date_column)
            else:
                print(f"No new data CSV found for sheet '{sheet}'. No data appended.")
                logging.warning(f"No new data CSV found for sheet '{sheet}'. No data appended.")
            
            # Remove duplicate rows
            initial_row_count = len(df)
            df.drop_duplicates(inplace=True)
            final_row_count = len(df)
            duplicates_removed = initial_row_count - final_row_count
            if duplicates_removed > 0:
                print(f"Removed {duplicates_removed} duplicate rows from sheet '{sheet}'.")
                logging.info(f"Removed {duplicates_removed} duplicate rows from sheet '{sheet}'.")
            
            # Assign the processed DataFrame to the dictionary
            processed_dfs[sheet] = df
            
            # Clear memory
            del df
            gc.collect()
        
        # Write all processed DataFrames to a new Excel file
        with pd.ExcelWriter(final_excel_path, engine='openpyxl') as writer:
            for sheet, df in processed_dfs.items():
                df.to_excel(writer, sheet_name=sheet, index=False)
                logging.info(f"Saved sheet '{sheet}' with {len(df)} rows.")
                print(f"Saved sheet '{sheet}' with {len(df)} rows.")
        
        print(f"\nFinal Excel file saved at '{final_excel_path}'")
        logging.info(f"Final Excel file saved at '{final_excel_path}'")
    
    except Exception as e:
        logging.error(f"Error processing Excel file: {e}")
        print(f"Error processing Excel file: {e}")
        sys.exit(1)

# ==========================
# 4. Main Execution Flow
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
    
        # Define the directory containing new CSV data to append
        new_data_dir = os.path.join(cwd, "new_data_csv")
        if not os.path.isdir(new_data_dir):
            print(f"New data directory '{new_data_dir}' not found. Creating it.")
            logging.warning(f"New data directory '{new_data_dir}' not found. Creating it.")
            os.makedirs(new_data_dir, exist_ok=True)
    
        # Define the path to save the final Excel file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        final_excel_filename = f"NA Trend Report_Final_{timestamp}.xlsx"
        final_excel_path = os.path.join(cwd, final_excel_filename)
    
        # Process the Excel file
        process_excel_file(excel_path, new_data_dir, final_excel_path)
    
    except Exception as e:
        logging.error(f"An unexpected error occurred in the main execution: {e}")
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
