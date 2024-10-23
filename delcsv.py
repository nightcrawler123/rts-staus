import os
import pandas as pd

# Define the patterns to search in filenames
patterns = [
    "QDS-above-70-crossed-40d",
    "QDS-0-69-crossed-40d",
    "QDS-0-69-less-40d",
    "QDS-above-70-less-40d"
]

# Define columns to be dropped (M to P, W to AD, AG to AL, AN to AQ are 12, 15-29, 32-37, 39-42 in zero-based index)
cols_to_drop = list(range(12, 16)) + list(range(22, 30)) + list(range(32, 38)) + list(range(39, 43))

# Scan for CSV files in the current directory that match the patterns
for file in os.listdir():
    if file.endswith('.csv') and any(pattern in file for pattern in patterns):
        print(f"Processing file: {file}")
        # Read the CSV file, skipping the first 4 rows, with low_memory=False to handle mixed types
        df = pd.read_csv(file, skiprows=4, low_memory=False)
        
        # Drop the specified columns
        df.drop(df.columns[cols_to_drop], axis=1, inplace=True)
        
        # Save the modified file
        output_file = f"processed_{file}"
        df.to_csv(output_file, index=False)
        print(f"File saved as: {output_file}")

print("Processing complete.")

