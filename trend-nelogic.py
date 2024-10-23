import sys
import os
import site
import warnings
warnings.filterwarnings('ignore')

# Add user's site-packages to path
user_site_packages = site.getusersitepackages()
if user_site_packages not in sys.path:
    sys.path.append(user_site_packages)

def check_imports():
    required_packages = {
        'pandas': 'pandas',
        'openpyxl': 'openpyxl',
        'tqdm': 'tqdm',
        'dask': 'dask'
    }
    
    missing_packages = []
    for package, import_name in required_packages.items():
        try:
            __import__(import_name)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print("Missing required packages:", ", ".join(missing_packages))
        print("\nPlease install them using:")
        print(f"pip install --user {' '.join(missing_packages)}")
        sys.exit(1)

check_imports()

import pandas as pd
import dask.dataframe as dd
from datetime import datetime
from tqdm import tqdm
import time
from openpyxl import load_workbook
import glob
from concurrent.futures import ThreadPoolExecutor
import dask.array as da
from dask.diagnostics import ProgressBar

class Timer:
    def __init__(self):
        self.start_time = time.time()
        self.stage_times = {}
    
    def mark_stage(self, stage_name):
        current_time = time.time()
        duration = current_time - self.start_time
        self.stage_times[stage_name] = duration
        print(f"{stage_name} completed in {duration:.2f} seconds")
        return duration

def get_sheet_data_fast(excel_path, sheet_name):
    """Fast sheet reading using openpyxl in read-only mode"""
    wb = load_workbook(filename=excel_path, read_only=True, data_only=True)
    ws = wb[sheet_name]
    
    headers = next(ws.iter_rows(min_row=1, max_row=1, values_only=True))
    data = list(ws.iter_rows(min_row=2, values_only=True))
    wb.close()
    
    return pd.DataFrame(data, columns=headers)

def process_sheet_parallel(args):
    """Process single sheet in parallel"""
    excel_path, sheet_name = args
    try:
        return sheet_name, get_sheet_data_fast(excel_path, sheet_name)
    except Exception as e:
        print(f"Error processing sheet {sheet_name}: {e}")
        return sheet_name, pd.DataFrame()

def get_oldest_date(df):
    """Get oldest date from DataFrame efficiently"""
    try:
        return pd.to_datetime(df.iloc[:, 0]).min()
    except:
        return None

def process_excel_file(excel_path):
    """Process Excel file using parallel processing"""
    print("Loading Excel file efficiently...")
    
    # Get sheet names
    wb = load_workbook(filename=excel_path, read_only=True)
    sheet_names = wb.sheetnames
    wb.close()
    
    # Process sheets in parallel
    with ThreadPoolExecutor() as executor:
        sheet_data = list(tqdm(
            executor.map(process_sheet_parallel, 
                        [(excel_path, sheet) for sheet in sheet_names]),
            total=len(sheet_names),
            desc="Reading sheets"
        ))
    
    # Convert to dictionary
    excel_data = dict(sheet_data)
    
    # Find oldest date using parallel processing
    print("Finding oldest date...")
    dates = []
    for df in excel_data.values():
        if not df.empty:
            date = get_oldest_date(df)
            if date is not None:
                dates.append(date)
    
    oldest_date = min(dates) if dates else None
    if oldest_date is None:
        raise ValueError("No valid dates found")
    
    print(f"Oldest date found: {oldest_date}")
    
    # Remove oldest date rows in parallel
    def filter_dates(df):
        if df.empty:
            return df
        try:
            return df[pd.to_datetime(df.iloc[:, 0]) > oldest_date]
        except:
            return df
    
    with ThreadPoolExecutor() as executor:
        filtered_data = list(executor.map(filter_dates, excel_data.values()))
    
    return dict(zip(excel_data.keys(), filtered_data))

def process_csv_file(csv_file, pattern, sheet_name, excel_data):
    """Process single CSV file"""
    try:
        # Read CSV using Dask for better memory management
        df = dd.read_csv(csv_file).compute()
        
        # Match data types
        for col in df.columns:
            if col in excel_data[sheet_name].columns:
                try:
                    df[col] = df[col].astype(excel_data[sheet_name][col].dtype)
                except:
                    pass
        
        return df
    except Exception as e:
        print(f"Error processing CSV {csv_file}: {e}")
        return None

def main():
    timer = Timer()
    
    try:
        # Get file paths
        cwd = os.getcwd()
        excel_path = os.path.join(cwd, "NA Trend Report.xlsx")
        
        # Process Excel file
        excel_data = process_excel_file(excel_path)
        timer.mark_stage("Excel Processing")
        
        # Get CSV files and mapping
        csv_files = glob.glob(os.path.join(cwd, "*.csv"))
        mapping = {
            'QDS-above-70-crossed-40d': 'QDS above 70 G40',
            'QDS-0-69-crossed-40d': 'QDS below 70 G40',
            'QDS-0-69-less-40d': 'QDS below 70 L40',
            'QDS-above-70-less-40d': 'QDS above 70 L40'
        }
        
        # Process CSV files in parallel
        print("\nProcessing CSV files...")
        csv_tasks = []
        for csv_file in csv_files:
            base_name = os.path.basename(csv_file).replace('processed_', '')
            for pattern, sheet_name in mapping.items():
                if pattern in base_name:
                    csv_tasks.append((csv_file, pattern, sheet_name))
                    break
        
        with ThreadPoolExecutor() as executor:
            csv_results = list(tqdm(
                executor.map(lambda x: process_csv_file(*x, excel_data), csv_tasks),
                total=len(csv_tasks),
                desc="Processing CSV files"
            ))
        
        # Update Excel data with CSV results
        for (csv_file, pattern, sheet_name), csv_data in zip(csv_tasks, csv_results):
            if csv_data is not None:
                excel_data[sheet_name] = pd.concat(
                    [excel_data[sheet_name], csv_data],
                    ignore_index=True
                )
        
        timer.mark_stage("CSV Processing")
        
        # Save results efficiently
        print("\nSaving results...")
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            for sheet_name, data in tqdm(excel_data.items(), desc="Saving sheets"):
                data.to_excel(writer, sheet_name=sheet_name, index=False)
        
        timer.mark_stage("Saving Results")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
