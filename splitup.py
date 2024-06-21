import os
import shutil
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import time

def log_execution_time(start_time, log_messages):
    end_time = time.time()
    execution_time = end_time - start_time
    execution_message = f"Total execution time: {execution_time:.2f} seconds"
    log_messages.append(execution_message)
    print(execution_message)

def process_application_data(df, app, folder_path, log_messages):
    try:
        # Filter data for the current application
        df_app = df[df['Application'] == app]

        # Create a new file name based on the application name
        file_name = f"{app}.xlsx"
        file_path = os.path.join(folder_path, file_name)

        # Save the filtered data to the new Excel file
        df_app.to_excel(file_path, index=False)

        # Log file creation
        log_messages.append(f"Saved file: {file_path}")
    except Exception as e:
        log_messages.append(f"Error saving file for application {app}: {e}")

def process_sheet(sheet_name, xl, cwd, today, log_messages):
    try:
        # Read the sheet into a DataFrame
        df = xl.parse(sheet_name)

        # Create a folder named after the sheet with the date appended
        folder_name = f"{sheet_name}-{today}"
        folder_path = os.path.join(cwd, folder_name)
        os.makedirs(folder_path, exist_ok=True)

        # Log folder creation
        log_messages.append(f"Created folder: {folder_path}")

        # Get unique application names
        unique_applications = df['Application'].unique()

        # Use ThreadPoolExecutor to process applications in parallel
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(process_application_data, df, app, folder_path, log_messages)
                for app in unique_applications
            ]

            # Wait for all threads to complete
            for future in futures:
                future.result()
    except Exception as e:
        log_messages.append(f"Error processing sheet {sheet_name}: {e}")

def delete_today_folders(cwd, today, log_messages):
    try:
        # Iterate through directories in the current working directory
        for item in os.listdir(cwd):
            item_path = os.path.join(cwd, item)
            if os.path.isdir(item_path) and item.endswith(f"-{today}"):
                shutil.rmtree(item_path)
                log_messages.append(f"Deleted folder: {item_path}")
    except Exception as e:
        log_messages.append(f"Error deleting folder: {e}")

def list_excel_files():
    files = [f for f in os.listdir() if f.endswith('.xlsx') or f.endswith('.xls')]
    if not files:
        print("No Excel files found in the working directory.")
        return None
    print("Excel files found in the working directory:")
    for i, file in enumerate(files, 1):
        print(f"{i}. {file}")
    return files

def select_excel_file(files):
    while True:
        try:
            selection = int(input("Select the number of the Excel file to process: "))
            if 1 <= selection <= len(files):
                return files[selection - 1]
            else:
                print(f"Please select a number between 1 and {len(files)}.")
        except ValueError:
            print("Invalid input. Please enter a number.")

def split_excel_by_application(file_path):
    start_time = time.time()
    try:
        # Load the Excel file
        xl = pd.ExcelFile(file_path)

        # Get the current working directory
        cwd = os.getcwd()

        # Get the current date in the required format
        today = datetime.now().strftime("%d-%b-%y")

        log_messages = []

        # Delete today's folders if they exist
        delete_today_folders(cwd, today, log_messages)

        # Use ThreadPoolExecutor to process sheets in parallel
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(process_sheet, sheet_name, xl, cwd, today, log_messages)
                for sheet_name in xl.sheet_names
            ]

            # Wait for all threads to complete
            for future in futures:
                future.result()

        # Log execution time
        log_execution_time(start_time, log_messages)

        # Save log messages to a log file
        log_file_path = os.path.join(cwd, 'split_excel_log.txt')
        with open(log_file_path, 'w') as log_file:
            for message in log_messages:
                log_file.write(message + '\n')

        print("Data split and files saved successfully. Logs are available in split_excel_log.txt.")

    except Exception as e:
        print(f"An error occurred: {e}")
        log_messages.append(f"An error occurred: {e}")
        # Log execution time in case of an error
        log_execution_time(start_time, log_messages)
        # Save log messages to a log file
        log_file_path = os.path.join(cwd, 'split_excel_log.txt')
        with open(log_file_path, 'w') as log_file:
            for message in log_messages:
                log_file.write(message + '\n')

# List Excel files and prompt user to select one
files = list_excel_files()
if files:
    selected_file = select_excel_file(files)
    if selected_file:
        # Call the function to split the data and save the files
        split_excel_by_application(selected_file)
