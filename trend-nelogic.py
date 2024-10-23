import sys
import os
import site
import warnings
from typing import Dict, Optional, Tuple
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
        'dask': 'dask',
        'numpy': 'numpy'
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
import numpy as np
import dask.dataframe as dd
from datetime import datetime
from tqdm import tqdm
import time
from openpyxl import load_workbook
import glob
from concurrent.futures import ThreadPoolExecutor, as_completed
import dask.array as da
from dask.diagnostics import ProgressBar

class Timer:
    def __init__(self):
        self.start_time = time.time()
        self.stage_times = {}
    
    def mark_stage(self, stage_name: str) -> float:
        current_time = time.time()
        duration = current_time - self.start_time
        self.stage_times[stage_name] = duration
        print(f"{stage_name} completed in {duration:.2f} seconds")
        return duration

def safe_parse_date(value) -> Optional[pd.Timestamp]:
    """Safely parse date values with multiple formats"""
    if pd.isna(value):
        return None
    
    try:
        return pd.to_datetime(value)
    except (ValueError, TypeError):
        date_formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%m/%d/%Y',
            '%d/%m/%Y'
        ]
        
        for fmt in date_formats:
            try:
                return pd.to_datetime(value, format=fmt)
            except (ValueError, TypeError):
                continue
        return None

def infer_and_convert_types(df: pd.DataFrame, reference_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """Intelligently handle mixed data types with error handling"""
    
    def convert_column(col: str, series: pd.Series) -> pd.Series:
        # First check if it's a date column
        if 'date' in col.lower() or (
            isinstance(series.dtype, pd.core.dtypes.dtypes.ObjectDtype) and 
            series.dropna().astype(str).str.contains(r'\d{4}-\d{2}-\d{2}').any()
        ):
            return pd.Series([safe_parse_date(x) for x in series])
        
        try:
            # Try numeric conversion
            numeric_series = pd.to_numeric(series, errors='coerce')
            if numeric_series.notna().any():
                # Check if it should be integer
                if numeric_series.dropna().apply(lambda x: float(x).is_integer()).all():
                    return numeric_series.astype('Int64')  # Uses nullable integer type
                return numeric_series
        except:
            pass
        
        # Keep as string if all else fails
        return series.astype(str)

    result_df = df.copy()
    
    # If we have a reference DataFrame, use its types as a guide
    if reference_df is not None:
        for col in result_df.columns:
            if col in reference_df.columns:
                try:
                    result_df[col] = result_df[col].astype(reference_df[col].dtype)
                except:
                    result_df[col] = convert_column(col, result_df[col])
    else:
        # If no reference, infer types for each column
        for col in result_df.columns:
            result_df[col] = convert_column(col, result_df[col])
    
    return result_df

def get_sheet_data_fast(excel_path: str, sheet_name: str) -> pd.DataFrame:
    """Fast sheet reading using openpyxl in read-only mode with error handling"""
    try:
        wb = load_workbook(filename=excel_path, read_only=True, data_only=True)
        ws = wb[sheet_name]
        
        headers = next(ws.iter_rows(min_row=1, max_row=1, values_only=True))
        data = list(ws.iter_rows(min_row=2, values_only=True))
        wb.close()
        
        # Create DataFrame with explicit string type for all columns initially
        df = pd.DataFrame(data, columns=headers).astype(str)
        return infer_and_convert_types(df)
    except Exception as e:
        print(f"Error reading sheet {sheet_name}: {str(e)}")
        return pd.DataFrame()

def process_sheet_parallel(args):
    """Process single sheet in parallel"""
    excel_path, sheet_name = args
    try:
        return sheet_name, get_sheet_data_fast(excel_path, sheet_name)
    except Exception as e:
        print(f"Error processing sheet {sheet_name}: {e}")
        return sheet_name, pd.DataFrame()

def get_oldest_date(df):
    """Get oldest date from DataFrame efficiently with type checking"""
    try:
        date_col = pd.to_datetime(df.iloc[:, 0], errors='coerce')
        return date_col.dropna().min()
    except:
        return None

def process_csv_file(csv_file: str, pattern: str, sheet_name: str, excel_data: Dict[str, pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Process single CSV file with robust error handling"""
    try:
        # Read CSV in chunks to handle large files and encoding issues
        chunk_size = 10000
        chunks = []
        
        for chunk in pd.read_csv(csv_file, chunksize=chunk_size, encoding='utf-8', 
                               on_bad_lines='skip', low_memory=False):
            chunks.append(chunk)
        
        if not chunks:
            return None
            
        df = pd.concat(chunks, ignore_index=True)
        df = clean_csv_data(df)
        
        # Get reference data types from Excel
        reference_df = excel_data.get(sheet_name)
        if reference_df is not None:
            df = infer_and_convert_types(df, reference_df)
        
        return df
    except Exception as e:
        print(f"Error processing CSV {csv_file}: {str(e)}")
        try:
            # Attempt to read with different encoding
            df = pd.read_csv(csv_file, encoding='latin1', on_bad_lines='skip')
            df = clean_csv_data(df)
            return infer_and_convert_types(df, excel_data.get(sheet_name))
        except Exception as e2:
            print(f"Second attempt failed: {str(e2)}")
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
            return df[pd.to_datetime(df.iloc[:, 0], errors='coerce') > oldest_date]
        except:
            return df
    
    with ThreadPoolExecutor() as executor:
        filtered_data = list(executor.map(filter_dates, excel_data.values()))
    
    return dict(zip(excel_data.keys(), filtered_data))

def main():
    timer = Timer()
    
    try:
        # Get file paths
        cwd = os.getcwd()
        excel_path = os.path.join(cwd, "NA Trend Report.xlsx")
        
        # Process Excel file
        excel_data = process_excel_file(excel_path)
        timer.mark_stage("Excel Processing")
        
        # Print data types for debugging
        print("\nExcel sheet data types:")
        for sheet_name, df in excel_data.items():
            print(f"\n{sheet_name} types:")
            print(df.dtypes)
        
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
            future_to_csv = {
                executor.submit(process_csv_file, *task, excel_data): task 
                for task in csv_tasks
            }
            
            for future in tqdm(as_completed(future_to_csv), 
                             total=len(csv_tasks),
                             desc="Processing CSV files"):
                csv_file, pattern, sheet_name = future_to_csv[future]
                try:
                    csv_data = future.result()
                    if csv_data is not None and not csv_data.empty:
                        if sheet_name in excel_data:
                            # Ensure unique index before concatenation
                            excel_data[sheet_name] = pd.concat(
                                [excel_data[sheet_name], csv_data],
                                ignore_index=True
                            ).drop_duplicates()
                except Exception as e:
                    print(f"Error processing CSV task: {str(e)}")
        
        timer.mark_stage("CSV Processing")
        
        # Save results efficiently
        print("\nSaving results...")
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            for sheet_name, data in tqdm(excel_data.items(), desc="Saving sheets"):
                if not data.empty:
                    data.to_excel(writer, sheet_name=sheet_name, index=False)
        
        timer.mark_stage("Saving Results")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
