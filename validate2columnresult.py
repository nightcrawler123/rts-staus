import pandas as pd

# Load the initial workbook
great40_df = pd.read_excel('Great40.xlsx')

# Load the info workbook
cmdb_df = pd.read_excel('cmdb.xlsx')

# Convert 'NetBIOS' and 'name' to uppercase for case-insensitive matching
great40_df['NetBIOS'] = great40_df['NetBIOS'].str.upper()
cmdb_df['name'] = cmdb_df['name'].str.upper()

# Rename 'asset.install_status' to 'asset_status'
cmdb_df.rename(columns={'asset.install_status': 'asset_status'}, inplace=True)

# Merge dataframes on 'NetBIOS' from great40_df and 'name' from cmdb_df
result_df = pd.merge(great40_df, cmdb_df[['name', 'serial_number', 'asset_status']], 
                     left_on='NetBIOS', right_on='name', how='left')

# Reorder columns to place 'serial_number' beside 'QID'
# Assuming 'QID' is already a column in great40_df, adjust as necessary
columns_order = list(great40_df.columns) + ['serial_number', 'asset_status']
result_df = result_df[columns_order]

# Simple progress graphic
total_rows = len(result_df)
for i in range(0, total_rows, total_rows//10):
    print(f"Processing: {i}/{total_rows} rows")

print("Processing completed.")

# Save the result to a new Excel workbook
result_df.to_excel('output.xlsx', index=False)

print("Output saved to 'output.xlsx'.")
