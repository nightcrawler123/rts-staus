## Steps to Run the VBA Code

### 1. Open the VBA Editor:
- Press `Alt + F11` in Excel.

### 2. Insert a New Module:
- In the VBA editor, click `Insert > Module`.

### 3. Paste the Code:
- Copy the provided VBA code and paste it into the new module.

### 4. Run the Macro:
- Close the VBA editor.
- Press `Alt + F8` in Excel, select `SplitDataByApplication`, and click `Run`.

## Prerequisites for Python Script

### Install Required Packages:
```sh
pip install pandas openpyxl xlrd
```

### Package Descriptions:
- **pandas**: For reading and writing Excel files, as well as handling DataFrames.
- **openpyxl**: For reading and writing `.xlsx` files (this is used internally by pandas for handling Excel files).
- **xlrd**: For reading `.xls` files (this is used internally by pandas for handling older Excel file formats).
