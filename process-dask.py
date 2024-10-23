import sys
from site import getusersitepackages
sys.path.append(getusersitepackages())

import pandas as pd
import dask.dataframe as dd
from tqdm import tqdm
import glob
import os
import time

# Start the timer
start_time = time.time()

# Define the mapping of CSV filename patterns to sheet names
pattern_to_sheet = {
    'QDS-above-70-crossed-40d': 'QDS above 70 G40',
    'QDS-0-69-crossed-40d': 'QDS below 70 G40',
    'QDS-0-69-less-40d': 'QDS below 70 L40',
    'QDS-above-70-less-40d': 'QDS above 70 L40',
}

# Get the current working directory
current_dir = os.getcwd()
print(f"Current working directory: {current_dir}")

excel_file = os.path.join(current_dir, 'NA Trend Report.xlsx')
sheets_to_process = list(pattern_to_sheet.values())

parquet_dir = os.path.join(current_dir, 'parquet_files')
os.makedirs(parquet_dir, exist_ok=True)

# Step 1: Convert Excel sheets to Parquet files using Dask
print("Converting Excel sheets to Parquet files...")
for sheet_name in tqdm(sheets_to_process, desc='Converting Sheets'):
    parquet_path = os.path.join(parquet_dir, sheet_name)
    if not os.path.exists(parquet_path):
        # Read the Excel sheet into a pandas DataFrame
        df = pd.read_excel(excel_file, sheet_name=sheet_name, dtype=str)
        # Convert the pandas DataFrame to a Dask DataFrame
        ddf = dd.from_pandas(df, npartitions=1)
        # Write the Dask DataFrame to Parquet
        ddf.to_parquet(parquet_path, write_index=False)

# Step 2: Process Parquet files with Dask
print("Processing Parquet files with Dask...")
for sheet_name in tqdm(sheets_to_process, desc='Processing Parquet Files'):
    parquet_path = os.path.join(parquet_dir, sheet_name)
    ddf = dd.read_parquet(parquet_path, dtype=str)

    # Delete rows with the oldest date in the first column
    first_col = ddf.columns[0]
    ddf[first_col] = dd.to_datetime(ddf[first_col], errors='coerce')
    oldest_date = ddf[first_col].min().compute()
    ddf = ddf[ddf[first_col] != oldest_date]

    # Save the cleaned data back to Parquet
    ddf.to_parquet(parquet_path, write_index=False, overwrite=True)

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
        parquet_path = os.path.join(parquet_dir, sheet_name)
        ddf_main = dd.read_parquet(parquet_path, dtype=str)

        # Read and concatenate matched CSV files
        for csv_file in matched_files:
            ddf_csv = dd.read_csv(csv_file, skiprows=1, dtype=str, assume_missing=True)
            ddf_main = dd.concat([ddf_main, ddf_csv], axis=0, interleave_partitions=True)

        # Save the combined data back to Parquet
        ddf_main.to_parquet(parquet_path, write_index=False, overwrite=True)

# Step 4: Recombine Parquet files into Excel workbook
print("Recombining Parquet files into Excel workbook...")
with pd.ExcelWriter(excel_file, engine='openpyxl', mode='w') as writer:
    for sheet_name in tqdm(sheets_to_process, desc='Writing to Excel'):
        parquet_path = os.path.join(parquet_dir, sheet_name)
        if os.path.exists(parquet_path):
            ddf = dd.read_parquet(parquet_path, dtype=str)
            df = ddf.compute()
            df.to_excel(writer, sheet_name=sheet_name, index=False)

# Calculate and display the total execution time
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Script completed in {elapsed_time:.2f} seconds.")
