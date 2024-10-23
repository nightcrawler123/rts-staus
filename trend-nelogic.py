import sys
from site import getusersitepackages
sys.path.append(getusersitepackages())

import polars as pl
import pandas as pd
import os
from tqdm import tqdm
import glob
import time

# Suppress Polars warnings (optional)
import warnings
warnings.filterwarnings('ignore')

# Start the timer
start_time = time.time()

# Define the mapping of CSV filename patterns to sheet names
pattern_to_sheet = {
    'QDS-above-70-crossed-40d': 'QDS above 70 G40',
    'QDS-0-69-crossed-40d': 'QDS below 70 G40',
    'QDS-0-69-less-40d': 'QDS below 70 L40',
    'QDS-above-70-less-40d': 'QDS above 70 L40',
}

current_dir = os.getcwd()
print(f"Current working directory: {current_dir}")

# Input Excel file
excel_file = os.path.join(current_dir, 'NA Trend Report.xlsx')

# Output Excel file (new file)
output_excel_file = os.path.join(current_dir, 'NA Trend Report Processed.xlsx')

sheets_to_process = list(pattern_to_sheet.values())

# Create a directory to store temporary Parquet files
parquet_dir = os.path.join(current_dir, 'parquet_files')
os.makedirs(parquet_dir, exist_ok=True)

# Step 1: Convert Excel sheets to Parquet files
print("Converting Excel sheets to Parquet files...")
for sheet_name in tqdm(sheets_to_process, desc='Converting Sheets'):
    parquet_path = os.path.join(parquet_dir, f"{sheet_name}.parquet")
    # Read Excel sheet with mangle_dupe_cols=True to handle duplicate columns
    df = pd.read_excel(
        excel_file,
        sheet_name=sheet_name,
        dtype=str,
        mangle_dupe_cols=True  # This will rename duplicate columns
    )
    # Handle missing values
    df = df.fillna('')

    # Clean column names
    df.columns = [col.strip().replace('\n', ' ').replace('\r', ' ') for col in df.columns]

    # Remove special characters from column names
    df.columns = [col.encode('ascii', 'ignore').decode('ascii') for col in df.columns]

    # Remove special characters from data and convert all columns to strings
    for col in df.columns:
        df[col] = df[col].astype(str)
        df[col] = df[col].str.encode('ascii', 'ignore').str.decode('ascii')

    # Convert pandas DataFrame to Polars DataFrame
    pl_df = pl.from_pandas(df)

    # Write Polars DataFrame to Parquet
    pl_df.write_parquet(parquet_path)

# Step 2: Process Parquet files with Polars
print("Processing Parquet files with Polars...")
for sheet_name in tqdm(sheets_to_process, desc='Processing Parquet Files'):
    parquet_path = os.path.join(parquet_dir, f"{sheet_name}.parquet")
    pl_df = pl.read_parquet(parquet_path)

    # Convert the first column to datetime
    first_col = pl_df.columns[0]
    pl_df = pl_df.with_column(
        pl.col(first_col)
        .str.strip()
        .str.strptime(pl.Datetime, fmt="%Y-%m-%d", strict=False)
        .alias(first_col)
    )

    # Get the oldest date
    oldest_date = pl_df.select(pl.col(first_col).min()).to_series()[0]

    # Filter out rows with the oldest date
    pl_df = pl_df.filter(pl.col(first_col) != oldest_date)

    # Write the cleaned data back to Parquet
    pl_df.write_parquet(parquet_path)

# Step 3: Append data from other CSV files
print("Appending data from CSV files...")
for sheet_name in tqdm(sheets_to_process, desc='Appending Data'):
    matched_files = []
    for pattern, target_sheet in pattern_to_sheet.items():
        if target_sheet == sheet_name:
            # Find matching CSV files
            csv_files = glob.glob(os.path.join(current_dir, '*.csv'))
            for csv_file in csv_files:
                if pattern in os.path.basename(csv_file):
                    matched_files.append(csv_file)
            break

    if matched_files:
        parquet_path = os.path.join(parquet_dir, f"{sheet_name}.parquet")
        pl_df = pl.read_parquet(parquet_path)

        # Read and concatenate matched CSV files
        for csv_file in matched_files:
            # Read CSV file, skip first row (header), handle missing values, inconsistent data types, special characters
            csv_df = pd.read_csv(csv_file, skiprows=1, header=None, dtype=str)
            csv_df = csv_df.fillna('')

            # Clean column names if needed
            # Since CSV files have no headers after skiprows=1, we need to assign column names
            csv_df.columns = pl_df.columns[:len(csv_df.columns)]

            # Remove special characters from data and convert all columns to strings
            for col in csv_df.columns:
                csv_df[col] = csv_df[col].astype(str)
                csv_df[col] = csv_df[col].str.encode('ascii', 'ignore').str.decode('ascii')

            # Convert pandas DataFrame to Polars DataFrame
            pl_csv_df = pl.from_pandas(csv_df)

            # Append to the main DataFrame
            pl_df = pl.concat([pl_df, pl_csv_df], how='vertical')

        # Write the combined data back to Parquet
        pl_df.write_parquet(parquet_path)

# Step 4: Recombine Parquet files into a new Excel workbook
print("Recombining Parquet files into a new Excel workbook...")
with pd.ExcelWriter(output_excel_file, engine='openpyxl', mode='w') as writer:
    for sheet_name in tqdm(sheets_to_process, desc='Writing to Excel'):
        parquet_path = os.path.join(parquet_dir, f"{sheet_name}.parquet")
        if os.path.exists(parquet_path):
            pl_df = pl.read_parquet(parquet_path)
            # Convert Polars DataFrame to pandas DataFrame
            df = pl_df.to_pandas()
            # Ensure all columns are strings
            df = df.astype(str)
            df.to_excel(writer, sheet_name=sheet_name, index=False)

# Optional: Remove the parquet_files directory after processing
# import shutil
# shutil.rmtree(parquet_dir)

# Calculate and display the total execution time
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Script completed in {elapsed_time:.2f} seconds.")
