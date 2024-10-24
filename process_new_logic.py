import pandas as pd
import os
from tqdm import tqdm
import glob
import time

# Get the current working directory
current_dir = os.getcwd()
print(f"Current working directory: {current_dir}")

# Define the mapping of CSV filename patterns to sheet names
pattern_to_sheet = {
    'QDS-above-70-crossed-40d': 'QDS above 70 G40',
    'QDS-0-69-crossed-40d': 'QDS below 70 G40',
    'QDS-0-69-less-40d': 'QDS below 70 L40',
    'QDS-above-70-less-40d': 'QDS above 70 L40',
}

# Paths relative to the current working directory
excel_file = os.path.join(current_dir, 'NA Trend Report.xlsx')
sheets_to_process = list(pattern_to_sheet.values())

# Create a directory to store CSV files
csv_dir = os.path.join(current_dir, 'temp_csv_files')
os.makedirs(csv_dir, exist_ok=True)

def convert_sheet_to_csv(sheet_name):
    # Convert a sheet to CSV
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    csv_path = os.path.join(csv_dir, f"{sheet_name}.csv")
    df.to_csv(csv_path, index=False)

def process_csv_file(sheet_name):
    csv_path = os.path.join(csv_dir, f"{sheet_name}.csv")
    df = pd.read_csv(csv_path, low_memory=False)  # Set low_memory=False

    # Delete rows with the oldest date in the first column
    first_col = df.columns[0]
    oldest_date = df[first_col].min()
    df = df[df[first_col] != oldest_date]

    # Save the cleaned CSV back
    df.to_csv(csv_path, index=False)

def append_csv_data(sheet_name):
    # Match CSV files based on patterns and append data
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
        # Read the main CSV file
        csv_path = os.path.join(csv_dir, f"{sheet_name}.csv")
        df_main = pd.read_csv(csv_path, low_memory=False)  # Set low_memory=False

        # Append data from matched CSV files
        for csv_file in matched_files:
            df_csv = pd.read_csv(csv_file, skiprows=1, low_memory=False)  # Set low_memory=False
            df_main = pd.concat([df_main, df_csv], ignore_index=True)

        # Save the combined data back to CSV
        df_main.to_csv(csv_path, index=False)

def recombine_csv_to_excel():
    # Combine all CSV files back into an Excel workbook
    with pd.ExcelWriter(excel_file, engine='openpyxl', mode='w') as writer:
        for sheet_name in sheets_to_process:
            csv_path = os.path.join(csv_dir, f"{sheet_name}.csv")
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path, low_memory=False)  # Set low_memory=False
                df.to_excel(writer, sheet_name=sheet_name, index=False)

if __name__ == '__main__':
    # Start the timer
    start_time = time.time()

    # Step 1: Convert sheets to CSV files
    print("Converting Excel sheets to CSV files...")
    for sheet_name in tqdm(sheets_to_process, desc='Converting Sheets'):
        convert_sheet_to_csv(sheet_name)

    # Step 2: Process CSV files (delete oldest date)
    print("Processing CSV files (deleting oldest date)...")
    for sheet_name in tqdm(sheets_to_process, desc='Processing CSVs'):
        process_csv_file(sheet_name)

    # Step 3: Append data from other CSV files
    print("Appending data from other CSV files...")
    for sheet_name in tqdm(sheets_to_process, desc='Appending Data'):
        append_csv_data(sheet_name)

    # Step 4: Recombine CSV files into Excel workbook
    print("Recombining CSV files into Excel workbook...")
    recombine_csv_to_excel()

    # Optional: Remove temporary CSV files
    # import shutil
    # shutil.rmtree(csv_dir)

    # Calculate and display the total execution time
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Script completed in {elapsed_time:.2f} seconds.")
