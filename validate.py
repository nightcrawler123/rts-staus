import pandas as pd

# Load the data
df_great40 = pd.read_excel('Great40.xlsx')
df_cmdb = pd.read_excel('cmdb.xlsx')

# Convert 'name' column in df_cmdb to uppercase
df_cmdb['name'] = df_cmdb['name'].str.upper()

# Assuming 'NetBIOS' values in Great40.xlsx are already in uppercase
# If not, you can also convert them using:
# df_great40['NetBIOS'] = df_great40['NetBIOS'].str.upper()

# Perform the merge operation, equivalent to VLOOKUP
# Using 'NetBIOS' from Great40.xlsx and 'name' from cmdb.xlsx
# and adding 'serial_number' from cmdb to Great40
result_df = pd.merge(df_great40, df_cmdb[['name', 'serial_number']], left_on='NetBIOS', right_on='name', how='left')

# Drop the redundant 'name' column from the merge if not needed
result_df.drop(columns=['name'], inplace=True)

# Write the result to a new Excel file
result_df.to_excel('output.xlsx', index=False)
