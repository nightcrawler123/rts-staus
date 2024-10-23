import pandas as pd
import os
from openpyxl import load_workbook
from tqdm import tqdm  # For progress bar

# Load the Excel file using pandas for specific sheets
excel_file = "NA Trend Report.xlsx"
sheet_names = ['QDS above 70 G40', 'QDS below 70 G40', 'QDS below 70 L40', 'QDS above 70 L40']

print("Loading sheets from Excel file...")

# Read only necessary sheets into pandas with progress bar
sheets = {}
for sheet_name in tqdm(sheet_names, desc="Loading sheets"):
    sheets[sheet_name] = pd.read_excel(excel_file, sheet_name=sheet_name)

print("Sheets loaded successfully.")

# Function to delete rows with the oldest date in a DataFrame
def delete_oldest_date_rows(df):
    if not df.empty:
        oldest_date = df.iloc[:, 0].min()  # Get the oldest date from the first column
        return df[df.iloc[:, 0] != oldest_date]  # Keep rows that do not have the oldest date
    return df

print("Deleting rows with the oldest date...")

# Delete rows with the oldest date from each sheet with progress bar
for sheet_name in tqdm(sheets, desc="Deleting rows"):
    sheets[sheet_name] = delete_oldest_date_rows(sheets[sheet_name])

print("Oldest date rows deleted successfully.")

# CSV to tab mapping
csv_to_tab = {
    'QDS-above-70-crossed-40d': 'QDS above 70 G40',
    'QDS-0-69-crossed-40d': 'QDS below 70 G40',
    'QDS-0-69-less-40d': 'QDS below 70 L40',
    'QDS-above-70-less-40d': 'QDS above 70 L40'
}

# Find all processed CSV files in the current directory
csv_files = [f for f in os.listdir() if f.startswith('processed') and f.endswith('.csv')]

print(f"Found {len(csv_files)} processed CSV files. Appending data to sheets...")

# Append CSV data to respective sheets with progress bar
for csv_file in tqdm(csv_files, desc="Processing CSV files"):
    for pattern, tab_name in csv_to_tab.items():
        if pattern in csv_file:
            # Load the CSV data
            print(f"Appending data from {csv_file} to {tab_name}...")
            csv_data = pd.read_csv(csv_file, skiprows=1)
            
            # Append the CSV data to the corresponding sheet
            sheets[tab_name] = pd.concat([sheets[tab_name], csv_data], ignore_index=True)
            
            print(f"Data from {csv_file} appended to sheet {tab_name}")
            break  # Stop after finding the matching tab

print("All CSV data has been appended successfully.")

# Write the modified sheets back to the Excel file
print("Writing updated sheets back to Excel...")

with pd.ExcelWriter(excel_file, engine='openpyxl', mode='a') as writer:
    for sheet_name, df in tqdm(sheets.items(), desc="Writing sheets"):
        df.to_excel(writer, sheet_name=sheet_name, index=False)

print("Processing complete!")
