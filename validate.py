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
import pandas as pd

print("Starting the operation...")

print("Loading data from 'Great40.xlsx'...")
df_great40 = pd.read_excel('Great40.xlsx')
print("Data loaded successfully.")

print("Loading data from 'cmdb.xlsx'...")
df_cmdb = pd.read_excel('cmdb.xlsx')
print("Data loaded successfully.")

print("Converting 'name' column values to uppercase in 'cmdb.xlsx' data...")
df_cmdb['name'] = df_cmdb['name'].str.upper()
print("Conversion completed.")

# Optionally convert 'NetBIOS' to uppercase if needed
# print("Converting 'NetBIOS' column values to uppercase in 'Great40.xlsx' data...")
# df_great40['NetBIOS'] = df_great40['NetBIOS'].str.upper()
# print("Conversion completed.")

print("Performing the merge operation...")
result_df = pd.merge(df_great40, df_cmdb[['name', 'serial_number']], left_on='NetBIOS', right_on='name', how='left')
result_df.drop(columns=['name'], inplace=True)  # Dropping the redundant 'name' column
print("Merge operation completed.")

print("Writing the result to 'output.xlsx'...")
result_df.to_excel('output.xlsx', index=False)
print("Operation completed successfully. The output has been saved to 'output.xlsx'.")
