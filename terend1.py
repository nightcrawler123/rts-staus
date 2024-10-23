import sys
import site
import pandas as pd
import glob
import os
from tqdm import tqdm
import time
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import gc

# Ensure user site-packages are included
def add_user_site_packages():
    try:
        user_site = site.getusersitepackages()
        if user_site not in sys.path:
            sys.path.append(user_site)
    except AttributeError:
        # Fallback for environments where getusersitepackages is not available
        user_base = site.getuserbase()
        user_site = os.path.join(user_base, 'lib', f'python{sys.version_info.major}.{sys.version_info.minor}', 'site-packages')
        if user_site not in sys.path:
            sys.path.append(user_site)

add_user_site_packages()

# Start time at the very beginning of the script
start_time = time.time()

def delete_oldest_date_rows(sheet_df):
    """
    Delete rows from the DataFrame that have the oldest date in the first column.
    Converts all columns to strings to handle mixed-type columns.
    """
    if not sheet_df.empty:
        # Convert all columns to string to handle mixed types
        sheet_df = sheet_df.astype(str)
        
        # Convert the first column to datetime, coerce errors to NaT
        sheet_df.iloc[:, 0] = pd.to_datetime(sheet_df.iloc[:, 0], errors='coerce', dayfirst=True)
        # Drop rows where the first column is NaT
        sheet_df.dropna(subset=[sheet_df.columns[0]], inplace=True)
        if sheet_df.empty:
            return sheet_df  # Return empty DataFrame if no valid dates
        # Find the oldest date
        oldest_date = sheet_df.iloc[:, 0].min()
        # Remove rows with the oldest date
        sheet_df = sheet_df[sheet_df.iloc[:, 0] != oldest_date]
    return sheet_df

def map_csv_to_sheet(csv_filename):
    """
    Map CSV file name patterns to corresponding Excel sheet names.
    """
    if "QDS-above-70-crossed-40d" in csv_filename:
        return "QDS above 70 G40"
    elif "QDS-0-69-crossed-40d" in csv_filename:
        return "QDS below 70 G40"
    elif "QDS-0-69-less-40d" in csv_filename:
        return "QDS below 70 L40"
    elif "QDS-above-70-less-40d" in csv_filename:
        return "QDS above 70 L40"
    else:
        return None  # No matching sheet

def process_excel_sheets(wb):
    """
    Process each sheet in the workbook to delete rows with the oldest date.
    Converts all columns to strings to handle mixed-type columns.
    """
    for sheet_name in tqdm(wb.sheetnames, desc="Processing Excel Sheets", unit="sheet"):
        ws = wb[sheet_name]
        # Read the sheet data into a DataFrame
        data = ws.values
        try:
            columns = next(data)  # Assumes first row is header
        except StopIteration:
            # Empty sheet
            continue
        df = pd.DataFrame(data, columns=columns)
        
        # Delete rows with the oldest date
        df = delete_oldest_date_rows(df)
        
        # Convert all columns to strings
        df = df.astype(str)
        
        # Clear existing data in the sheet (except header)
        ws.delete_rows(2, ws.max_row)
        
        if not df.empty:
            # Append updated DataFrame to the sheet
            for row in dataframe_to_rows(df, index=False, header=False):
                ws.append(row)
        
        # Force garbage collection
        del df
        gc.collect()

def append_csv_to_sheet(wb, csv_file):
    """
    Append data from a CSV file to the corresponding sheet in the workbook.
    Converts all columns to strings to handle mixed-type columns.
    """
    sheet_name = map_csv_to_sheet(os.path.basename(csv_file))
    if not sheet_name:
        # Skip CSV files that do not match any pattern
        return
    
    if sheet_name not in wb.sheetnames:
        # Skip if the mapped sheet does not exist
        return
    
    ws = wb[sheet_name]
    
    # Determine if the sheet already has data
    max_row = ws.max_row
    
    # Read CSV in chunks
    chunk_size = 10000  # Adjust based on memory and performance
    try:
        for chunk in pd.read_csv(csv_file, chunksize=chunk_size, dtype=str):
            # If the sheet already has data and this is the first chunk, skip header
            if max_row > 1:
                chunk = chunk.iloc[1:]  # Skip header row
            if chunk.empty:
                continue  # Skip empty chunks
            # Append chunk to the sheet
            for row in dataframe_to_rows(chunk, index=False, header=False):
                ws.append(row)
            # Update max_row to reflect newly added rows
            max_row = ws.max_row
            # Force garbage collection after each chunk
            del chunk
            gc.collect()
    except pd.errors.EmptyDataError:
        # Handle empty CSV files gracefully
        pass

def process_csv_files(wb, csv_files):
    """
    Process and append all CSV files to their respective sheets.
    """
    for csv_file in tqdm(csv_files, desc="Processing CSV Files", unit="file"):
        append_csv_to_sheet(wb, csv_file)
        # Force garbage collection after processing each file
        gc.collect()

def main():
    # Define the input and output Excel file paths
    input_excel_file = 'NA Trend Report.xlsx'
    output_excel_file = 'NA Trend Report Updated.xlsx'
    
    # Find all processed CSV files in the current directory
    csv_files = glob.glob('processed_*.csv')
    
    # Load the workbook
    print("Loading the Excel workbook...")
    wb = load_workbook(input_excel_file)
    
    # Step 1: Delete rows with the oldest date from all sheets
    print("Deleting rows with the oldest date from all sheets...")
    process_excel_sheets(wb)
    
    # Step 2: Append CSV data to corresponding sheets
    print("Appending CSV data to corresponding sheets...")
    process_csv_files(wb, csv_files)
    
    # Save the workbook as a new file
    print(f"Saving the updated Excel workbook as '{output_excel_file}'...")
    wb.save(output_excel_file)
    
    # Close the workbook
    wb.close()
    
    # Calculate and display the total time taken
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Processing completed in {total_time:.2f} seconds.")

if __name__ == "__main__":
    main()
