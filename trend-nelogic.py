import sys
from site import getusersitepackages
sys.path.append(getusersitepackages())

import vaex
import pandas as pd
import os
from tqdm import tqdm
import glob
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

current_dir = os.getcwd()
print(f"Current working directory: {current_dir}")

# Input Excel file
excel_file = os.path.join(current_dir, 'NA Trend Report.xlsx')

# Output Excel file (new file)
output_excel_file = os.path.join(current_dir, 'NA Trend Report Processed.xlsx')

sheets_to_process = list(pattern_to_sheet.values())

hdf5_dir = os.path.join(current_dir, 'hdf5_files')
os.makedirs(hdf5_dir, exist_ok=True)

# Step 1: Convert Excel sheets to HDF5 files
print("Converting Excel sheets to HDF5 files...")
for sheet_name in tqdm(sheets_to_process, desc='Converting Sheets'):
    hdf5_path = os.path.join(hdf5_dir, f"{sheet_name}.hdf5")
    # Read Excel sheet with mangle_dupe_cols=True to handle duplicate columns
    df = pd.read_excel(
        excel_file,
        sheet_name=sheet_name,
        dtype=str,
        mangle_dupe_cols=True  # This will rename duplicate columns
    )
    # Convert all columns to strings to handle mixed types
    for col in df.columns:
        df[col] = df[col].astype(str)
    # Convert the pandas DataFrame to a Vaex DataFrame
    vaex_df = vaex.from_pandas(df, copy_index=False)
    # Export the Vaex DataFrame to HDF5
    vaex_df.export_hdf5(hdf5_path, progress=False)

# Step 2: Process HDF5 files with Vaex
print("Processing HDF5 files with Vaex...")
for sheet_name in tqdm(sheets_to_process, desc='Processing HDF5 Files'):
    hdf5_path = os.path.join(hdf5_dir, f"{sheet_name}.hdf5")
    vdf = vaex.open(hdf5_path)

    # Convert the first column to datetime
    first_col = vdf.column_names[0]
    vdf[first_col] = vdf[first_col].astype('datetime64[ns]')

    # Get the oldest date
    oldest_date = vdf[first_col].min()
    # Filter out rows with the oldest date
    vdf = vdf[vdf[first_col] != oldest_date]

    # Save the cleaned data back to HDF5
    vdf.export_hdf5(hdf5_path, progress=False)

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
        hdf5_path = os.path.join(hdf5_dir, f"{sheet_name}.hdf5")
        vdf_main = vaex.open(hdf5_path)

        # Read and concatenate matched CSV files
        for csv_file in matched_files:
            vdf_csv = vaex.from_csv(csv_file, skiprows=1, convert=True, chunk_size=5_000_000)
            # Convert all columns to strings to handle mixed types
            for col in vdf_csv.get_column_names():
                vdf_csv[col] = vdf_csv[col].astype(str)
            vdf_main = vdf_main.concat(vdf_csv)

        # Save the combined data back to HDF5
        vdf_main.export_hdf5(hdf5_path, progress=False)

# Step 4: Recombine HDF5 files into a new Excel workbook
print("Recombining HDF5 files into a new Excel workbook...")
with pd.ExcelWriter(output_excel_file, engine='openpyxl', mode='w') as writer:
    for sheet_name in tqdm(sheets_to_process, desc='Writing to Excel'):
        hdf5_path = os.path.join(hdf5_dir, f"{sheet_name}.hdf5")
        if os.path.exists(hdf5_path):
            vdf = vaex.open(hdf5_path)
            # Convert Vaex DataFrame to pandas DataFrame
            df = vdf.to_pandas_df()
            # Ensure all columns are strings to handle mixed types
            for col in df.columns:
                df[col] = df[col].astype(str)
            df.to_excel(writer, sheet_name=sheet_name, index=False)

# Optional: Remove the hdf5_files directory after processing
# import shutil
# shutil.rmtree(hdf5_dir)

# Calculate and display the total execution time
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Script completed in {elapsed_time:.2f} seconds.")
