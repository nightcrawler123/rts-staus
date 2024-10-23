import os
import pandas as pd
from datetime import datetime

# Define the patterns to search in filenames
patterns = [
    "QDS-above-70-crossed-40d",
    "QDS-0-69-crossed-40d",
    "QDS-0-69-less-40d",
    "QDS-above-70-less-40d"
]

# Define columns to be dropped (M to P, W to AD, AG to AL, AN to AQ are 12, 15-29, 32-37, 39-42 in zero-based index)
cols_to_drop = list(range(12, 16)) + list(range(22, 30)) + list(range(32, 38)) + list(range(39, 43))

# Get today's date in MM/DD/YYYY format
today_date = datetime.now().strftime('%m/%d/%Y')

# Scan for CSV files in the current directory that match the patterns
for file in os.listdir():
    if file.endswith('.csv') and any(pattern in file for pattern in patterns):
        print(f"Processing file: {file}")
        # Read the CSV file, skipping the first 4 rows, with low_memory=False to handle mixed types
        df = pd.read_csv(file, skiprows=4, low_memory=False)
        
        # Drop the specified columns
        df.drop(df.columns[cols_to_drop], axis=1, inplace=True)
        
        # Insert the new column with today's date at the start of the dataframe
        df.insert(0, 'Date', today_date)
        
        # Save the modified file (overwrite the original file)
        df.to_csv(file, index=False)
        print(f"File overwritten and saved: {file}")

print("Processing complete.")
