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

# Find the index of 'QID' column
qid_index = result_df.columns.get_loc('QID')

# Assuming you want to insert 'serial_number' and 'asset_status' after 'QID'
# We create a new column order
columns_before = result_df.columns[:qid_index+1].tolist() # Columns before and including 'QID'
columns_after = result_df.columns[qid_index+1:].tolist() # Columns after 'QID'
# Exclude 'name', 'serial_number', and 'asset_status' from columns_after as they are already accounted for
columns_after = [col for col in columns_after if col not in ['name', 'serial_number', 'asset_status']]

# New column order with 'serial_number' and 'asset_status' right after 'QID'
new_columns_order = columns_before + ['serial_number', 'asset_status'] + columns_after

# Reorder dataframe according to the new column order
result_df = result_df[new_columns_order]

# Simple progress graphic
total_rows = len(result_df)
for i in range(0, total_rows, max(1, total_rows//10)):
    print(f"Processing: {i}/{total_rows} rows")

print("Processing completed.")

# Save the result to a new Excel workbook
result_df.to_excel('output.xlsx', index=False)

print("Output saved to 'output.xlsx'.")
