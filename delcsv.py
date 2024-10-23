import os
import pandas as pd
from datetime import datetime

# Define the patterns to search in filenames
patterns = {
    "QDS-above-70-crossed-40d": "QDS above 70 G40",
    "QDS-0-69-crossed-40d": "QDS below 70 G40",
    "QDS-0-69-less-40d": "QDS below 70 L40",
    "QDS-above-70-less-40d": "QDS above 70 L40"
}

# Define columns to be dropped (M to P, W to AD, AG to AL, AN to AQ are 12, 15-29, 32-37, 39-42 in zero-based index)
cols_to_drop = list(range(12, 16)) + list(range(22, 30)) + list(range(32, 38)) + list(range(39, 43))

# Get today's date in MM/DD/YYYY format
today_date = datetime.now().strftime('%m/%d/%Y')

# Load the 'NA Trend Reports.xlsx' file
excel_file = "NA Trend Reports.xlsx"
if os.path.exists(excel_file):
    excel_data = pd.ExcelFile(excel_file)
else:
    print(f"Excel file {excel_file} not found.")
    exit()

# Function to remove rows with the oldest date from each sheet
def remove_oldest_date_rows():
    for sheet_name in excel_data.sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        if not df.empty:
            oldest_date = df.iloc[:, 0].min()  # Get the oldest date in the first column
            df = df[df.iloc[:, 0] != oldest_date]  # Filter out rows with the oldest date
            with pd.ExcelWriter(excel_file, mode='a', if_sheet_exists='replace') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"Removed rows with the oldest date ({oldest_date}) from sheet: {sheet_name}")
        else:
            print(f"Sheet {sheet_name} is empty, skipping.")

# Run the function to remove the oldest rows
remove_oldest_date_rows()

# Process the CSV files and update the Excel file
for file in os.listdir():
    if file.endswith('.csv') and any(pattern in file for pattern in patterns.keys()):
        print(f"Processing file: {file}")
        
        # Read the CSV file, skipping the first 4 rows, with low_memory=False to handle mixed types
        df = pd.read_csv(file, skiprows=4, low_memory=False)
        
        # Drop the specified columns
        df.drop(df.columns[cols_to_drop], axis=1, inplace=True)
        
        # Insert the new column with today's date at the start of the dataframe
        df.insert(0, 'Date', today_date)
        
        # Match the file pattern to the correct Excel sheet
        for pattern, sheet_name in patterns.items():
            if pattern in file:
                # Load the existing data from the matched sheet in the Excel file
                sheet_df = pd.read_excel(excel_file, sheet_name=sheet_name)
                
                # Append the processed data (excluding the header row)
                updated_df = pd.concat([sheet_df, df.iloc[1:, :]], ignore_index=True)
                
                # Save the updated sheet back to the Excel file
                with pd.ExcelWriter(excel_file, mode='a', if_sheet_exists='replace') as writer:
                    updated_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                print(f"Appended data from {file} to sheet: {sheet_name}")
                break

print("Processing complete.")
