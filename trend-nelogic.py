from openpyxl import load_workbook

def read_excel_sheets(excel_path):
    """
    Reads all sheets from the Excel file into a dictionary of Polars DataFrames using openpyxl in read-only mode.
    """
    try:
        print(f"Opening Excel file '{excel_path}' with openpyxl...")
        logging.info(f"Opening Excel file '{excel_path}' with openpyxl...")
        wb = load_workbook(excel_path, read_only=True, data_only=True)
        print("Excel file opened successfully.")
        sheet_names = wb.sheetnames
        print(f"Found sheets: {sheet_names}")
        logging.info(f"Found sheets: {sheet_names}")
        polars_dict = {}
        for sheet_name in tqdm(sheet_names, desc="Reading Excel Sheets"):
            print(f"Reading sheet: {sheet_name}")
            sheet_start_time = datetime.now()
            ws = wb[sheet_name]

            # Extract data using generator to minimize memory usage
            data = ws.values
            # Get the first line as columns header
            columns = next(data)
            # Convert the rest of the data into a list of rows
            data_rows = list(data)

            # Create Polars DataFrame
            # Use schema=columns to specify column names
            df = pl.DataFrame(data_rows, schema=columns)

            # Optional: Drop any completely empty columns (if any)
            df = df.drop_nulls(subset=df.columns)

            polars_dict[sheet_name] = df
            sheet_end_time = datetime.now()
            elapsed_time = sheet_end_time - sheet_start_time
            print(f"Finished reading sheet: {sheet_name} in {elapsed_time}")
            logging.info(f"Finished reading sheet: {sheet_name} in {elapsed_time}")
            # Free memory
            del ws
            gc.collect()
        print("Completed reading all Excel sheets.")
        wb.close()
        return polars_dict
    except Exception as e:
        logging.error(f"Error reading Excel file '{excel_path}': {e}")
        sys.exit(f"Error reading Excel file '{excel_path}': {e}")
