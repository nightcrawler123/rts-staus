import sys
import subprocess
import importlib
import os
import glob
import logging
from datetime import datetime
import shutil
from tqdm import tqdm

# List of required packages
required_packages = [
    'polars',
    'tqdm',
    'openpyxl',
    'pyarrow',
    'pandas'  # Needed for Excel writing compatibility
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
import polars as pl
import pandas as pd

# Setup logging
logging.basicConfig(
    filename='data_processing.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def map_csv_to_sheet(csv_filename):
    """
    Maps a CSV filename to the corresponding Excel sheet name based on predefined patterns.
    """
    basename = os.path.splitext(csv_filename)[0]
    mapping = {
        'QDS-above-70-crossed-40d': 'QDS above 70 G40',
        'QDS-0-69-crossed-40d': 'QDS below 70 G40',
        'QDS-0-69-less-40d': 'QDS below 70 L40',
        'QDS-above-70-less-40d': 'QDS above 70 L40'
    }
    return mapping.get(basename, None)

def read_excel_sheets(excel_path):
    """
    Reads all sheets from the Excel file into a dictionary of Polars DataFrames.
    All data is read as strings to handle mixed-type cells.
    Provides progress feedback for each sheet being read.
    """
    try:
        # Use pandas to read Excel sheets with optimized parameters
        print("Starting to read Excel sheets...")
        logging.info(f"Starting to read Excel file '{excel_path}'")
        excel = pd.ExcelFile(excel_path, engine='openpyxl')
        sheet_names = excel.sheet_names
        logging.info(f"Found {len(sheet_names)} sheets: {sheet_names}")
        print(f"Found {len(sheet_names)} sheets: {sheet_names}")

        polars_dict = {}
        # Iterate through sheets with a progress bar
        for sheet in tqdm(sheet_names, desc="Reading Excel Sheets"):
            print(f"Reading sheet: {sheet}")
            sheet_start_time = datetime.now()
            df = excel.parse(sheet_name=sheet, dtype=str, engine_kwargs={'read_only': True, 'data_only': True})
            polars_df = pl.from_pandas(df)
            polars_dict[sheet] = polars_df
            sheet_end_time = datetime.now()
            logging.info(f"Read sheet '{sheet}' in {sheet_end_time - sheet_start_time}")
            print(f"Finished reading sheet: {sheet} in {sheet_end_time - sheet_start_time}")
        print("Completed reading all Excel sheets.")
        return polars_dict
    except Exception as e:
        logging.error(f"Error reading Excel file '{excel_path}': {e}")
        sys.exit(f"Error reading Excel file '{excel_path}': {e}")

def read_csv_file(csv_path):
    """
    Reads a CSV file into a Polars DataFrame with all columns as strings.
    Logs any malformed rows or read errors.
    """
    try:
        print(f"Reading CSV file: {csv_path}")
        df = pl.read_csv(
            csv_path,
            try_parse_dates=False,  # Disable automatic date parsing
            ignore_errors=False,     # Do not ignore errors to capture bad rows
            low_memory=False,        # Disable low_memory to improve type inference
            n_threads=4,             # Utilize all available cores
            dtype_backend='string'   # Read all data as strings to handle mixed types
        )
        logging.info(f"Successfully read CSV file '{csv_path}' with {df.height} rows and {df.width} columns.")
        print(f"Successfully read CSV file: {csv_path} with {df.height} rows and {df.width} columns.")
        return df
    except pl.errors.ParseError as pe:
        logging.error(f"Parse error in file '{csv_path}': {pe}")
    except Exception as e:
        logging.error(f"Unexpected error reading CSV file '{csv_path}': {e}")
    return None

def parse_columns(df, date_column='Date'):
    """
    Parses columns to their appropriate data types.
    Specifically handles the Date column and attempts to parse numerical columns.
    Logs any parsing errors.
    """
    try:
        # Parse the Date column
        if date_column in df.columns:
            df = df.with_column(
                pl.col(date_column).str.strptime(pl.Date, fmt="%Y-%m-%d", strict=False).alias(date_column)
            )
            logging.info(f"Parsed '{date_column}' column to Date type.")
        else:
            logging.warning(f"Date column '{date_column}' not found in DataFrame.")
        
        # Attempt to parse other columns to numeric where possible
        for col in df.columns:
            if col == date_column:
                continue
            # Attempt to parse to Float, then Int if possible
            df = df.with_column(
                pl.col(col).str.parse_float(strict=False).alias(f"{col}_float")
            )
            df = df.with_column(
                pl.col(col).str.parse_int(strict=False).alias(f"{col}_int")
            )
            # Combine parsed columns back to original, preferring integers over floats over strings
            df = df.with_columns([
                pl.when(pl.col(f"{col}_int").is_not_null()).then(pl.col(f"{col}_int"))
                .otherwise(
                    pl.when(pl.col(f"{col}_float").is_not_null()).then(pl.col(f"{col}_float"))
                    .otherwise(pl.col(col))
                ).alias(col)
            ])
            # Drop temporary parsed columns
            df = df.drop([f"{col}_float", f"{col}_int"])
        logging.info("Completed parsing columns to appropriate data types.")
        return df
    except Exception as e:
        logging.error(f"Error parsing columns: {e}")
        return df  # Return DataFrame even if parsing fails

def merge_data(excel_df, csv_df, date_column='Date'):
    """
    Merges CSV DataFrame into Excel DataFrame based on the date column.
    """
    try:
        # Ensure the date column is in datetime format
        if date_column in excel_df.columns:
            excel_df = excel_df.with_column(
                pl.col(date_column).str.strptime(pl.Date, fmt="%Y-%m-%d", strict=False).alias(date_column)
            )
        else:
            logging.warning(f"Date column '{date_column}' not found in Excel DataFrame.")
        
        if date_column in csv_df.columns:
            csv_df = csv_df.with_column(
                pl.col(date_column).str.strptime(pl.Date, fmt="%Y-%m-%d", strict=False).alias(date_column)
            )
        else:
            logging.warning(f"Date column '{date_column}' not found in CSV DataFrame.")
        
        # Perform a join on the date column
        merged_df = excel_df.join(csv_df, on=date_column, how='outer')
        
        logging.info("Successfully merged CSV data into Excel sheet.")
        print("Successfully merged CSV data into Excel sheet.")
        return merged_df
    except Exception as e:
        logging.error(f"Error merging data: {e}")
        return excel_df  # Return original if merge fails

def remove_oldest_rows(excel_sheets_dict, date_column='Date'):
    """
    Removes rows with the oldest date from each sheet.
    """
    for sheet_name, df in excel_sheets_dict.items():
        try:
            print(f"Processing sheet '{sheet_name}' to remove oldest rows...")
            # Check if date column exists
            if date_column not in df.columns:
                logging.warning(f"Date column '{date_column}' not found in sheet '{sheet_name}'. Skipping removal of oldest rows.")
                continue
            
            # Ensure the date column is of Date type
            if df[date_column].dtype != pl.Date:
                df = df.with_column(
                    pl.col(date_column).str.strptime(pl.Date, fmt="%Y-%m-%d", strict=False).alias(date_column)
                )
            
            # Find the minimum date, ignoring nulls
            min_date_series = df.select(pl.col(date_column).min()).to_series()
            min_date = min_date_series.drop_nulls().to_list()[0] if min_date_series.drop_nulls().to_list() else None
            
            if min_date:
                # Filter out rows with the minimum date
                df_filtered = df.filter(pl.col(date_column) != min_date)
                excel_sheets_dict[sheet_name] = df_filtered
                logging.info(f"Removed rows with date '{min_date}' from sheet '{sheet_name}'.")
                print(f"Removed rows with date '{min_date}' from sheet '{sheet_name}'.")
            else:
                logging.warning(f"No valid dates found in sheet '{sheet_name}'. Skipping removal of oldest rows.")
        except Exception as e:
            logging.error(f"Error removing oldest rows in sheet '{sheet_name}': {e}")

def save_final_excel(excel_sheets_dict, original_excel_path):
    """
    Saves the final Excel file with a timestamp and creates a backup of the original.
    """
    try:
        # Create backup
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f"{os.path.splitext(original_excel_path)[0]}_backup_{timestamp}.xlsx"
        # Copy the original file to backup instead of renaming to keep it intact
        shutil.copy2(original_excel_path, backup_path)
        logging.info(f"Backup created at '{backup_path}'")
        print(f"Backup of the original Excel file created at '{backup_path}'")
        
        # Convert Polars DataFrames to pandas DataFrames
        pandas_dict = {}
        print("Converting Polars DataFrames to pandas DataFrames...")
        for sheet, df in excel_sheets_dict.items():
            pandas_df = df.to_pandas()
            pandas_dict[sheet] = pandas_df
            logging.info(f"Converted sheet '{sheet}' to pandas DataFrame.")
        
        # Save to new Excel with timestamp
        final_excel_path = f"{os.path.splitext(original_excel_path)[0]}_Final_{timestamp}.xlsx"
        print(f"Saving merged data to '{final_excel_path}'...")
        with pd.ExcelWriter(final_excel_path, engine='openpyxl') as writer:
            for sheet, df in pandas_dict.items():
                df.to_excel(writer, sheet_name=sheet, index=False)
                logging.info(f"Saved sheet '{sheet}' to final Excel file.")
        logging.info(f"Final Excel file saved at '{final_excel_path}'")
        print(f"Final Excel file saved at '{final_excel_path}'")
    except Exception as e:
        logging.error(f"Error saving final Excel file: {e}")
        print(f"Error saving final Excel file: {e}")

def validate_schema(excel_df, csv_df, expected_columns):
    """
    Validates that both DataFrames have the expected columns.
    Logs discrepancies.
    """
    excel_columns = set(excel_df.columns)
    csv_columns = set(csv_df.columns)
    missing_in_csv = excel_columns - csv_columns
    missing_in_excel = csv_columns - excel_columns

    if missing_in_csv:
        logging.warning(f"Columns missing in CSV: {missing_in_csv}")
    if missing_in_excel:
        logging.warning(f"Columns missing in Excel: {missing_in_excel}")

def main():
    # Start overall timer
    overall_start = datetime.now()

    # Define current working directory
    cwd = os.getcwd()

    excel_file = os.path.join(cwd, 'NA Trend Report.xlsx')
    csv_pattern = os.path.join(cwd, "*.csv")

    # Check if Excel file exists
    if not os.path.isfile(excel_file):
        logging.error(f"Excel file '{excel_file}' not found in the current directory.")
        sys.exit(f"Excel file '{excel_file}' not found in the current directory.")

    # Read Excel sheets
    print("Reading Excel file...")
    excel_start = datetime.now()
    excel_sheets = read_excel_sheets(excel_file)
    excel_end = datetime.now()
    elapsed_excel = excel_end - excel_start
    print(f"Excel file read in {elapsed_excel}")
    logging.info(f"Excel file '{excel_file}' read in {elapsed_excel}")

    # Find all CSV files matching the pattern
    csv_files = glob.glob(csv_pattern)

    # Exclude the Excel file if it has a .csv extension
    csv_files = [f for f in csv_files if os.path.basename(f) != 'NA Trend Report.xlsx']

    if not csv_files:
        logging.error("No CSV files found in the current directory.")
        sys.exit("No CSV files found.")

    # Process each CSV file
    print("Processing CSV files...")
    for csv_file in tqdm(csv_files, desc="CSV Files"):
        sheet_name = map_csv_to_sheet(os.path.basename(csv_file))
        if not sheet_name:
            logging.warning(f"No mapping found for CSV file '{csv_file}'. Skipping.")
            print(f"No mapping found for CSV file '{csv_file}'. Skipping.")
            continue

        if sheet_name not in excel_sheets:
            logging.warning(f"Sheet '{sheet_name}' not found in Excel file. Skipping CSV '{csv_file}'.")
            print(f"Sheet '{sheet_name}' not found in Excel file. Skipping CSV '{csv_file}'.")
            continue

        csv_df = read_csv_file(csv_file)
        if csv_df is None:
            logging.error(f"Failed to read CSV file '{csv_file}'. Skipping.")
            print(f"Failed to read CSV file '{csv_file}'. Skipping.")
            continue

        # Parse columns to handle mixed data types
        csv_df = parse_columns(csv_df)

        # Optional: Validate schema
        # Define expected columns if known, else skip
        # expected_columns = {'Date', 'Column1', 'Column2', ...}
        # validate_schema(excel_sheets[sheet_name], csv_df, expected_columns)

        # Merge CSV data into the corresponding Excel sheet
        merged_df = merge_data(excel_sheets[sheet_name], csv_df)
        excel_sheets[sheet_name] = merged_df
        logging.info(f"Merged CSV file '{csv_file}' into sheet '{sheet_name}'.")
        print(f"Merged CSV file '{csv_file}' into sheet '{sheet_name}'.")

    # Remove oldest rows
    print("Removing oldest rows from each sheet...")
    remove_start = datetime.now()
    remove_oldest_rows(excel_sheets)
    remove_end = datetime.now()
    elapsed_remove = remove_end - remove_start
    print(f"Oldest rows removed in {elapsed_remove}")
    logging.info(f"Oldest rows removed in {elapsed_remove}")

    # Save final Excel file
    print("Saving final Excel file...")
    save_start = datetime.now()
    save_final_excel(excel_sheets, excel_file)
    save_end = datetime.now()
    elapsed_save = save_end - save_start
    print(f"Final Excel file saved in {elapsed_save}")
    logging.info(f"Final Excel file saved in {elapsed_save}")

    # End overall timer
    overall_end = datetime.now()
    total_time = overall_end - overall_start
    print(f"Script completed in {total_time}")
    logging.info(f"Script completed in {total_time}")

if __name__ == "__main__":
    main()
