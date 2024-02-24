import pandas as pd

# Load the data
df_great40 = pd.read_excel('Great40.xlsx')
df_cmdb = pd.read_excel('cmdb.xlsx')

# Perform the merge operation, equivalent to VLOOKUP
# Assuming 'NetBIOS' is in Great40.xlsx and 'name' in cmdb.xlsx
# and you want to add 'serial_number' from cmdb to Great40
result_df = pd.merge(df_great40, df_cmdb[['name', 'serial_number']], left_on='NetBIOS', right_on='name', how='left')

# Drop the redundant 'name' column from the merge if not needed
result_df.drop(columns=['name'], inplace=True)

# Write the result to a new Excel file
result_df.to_excel('output.xlsx', index=False)
