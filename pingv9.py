import os
import time
import socket
import ping3
import concurrent.futures
import openpyxl
from openpyxl import Workbook
from datetime import datetime, timedelta

LOG_RETENTION_DAYS = 7
LOG_FILE = 'ping_log.txt'

def ping_host(hostname):
    try:
        ip = socket.gethostbyname(hostname)
        response = ping3.ping(ip)
        status = 'online' if response else 'offline'
    except (socket.gaierror, ping3.errors.HostUnknown):
        ip = 'Bad Host'
        status = 'offline'
    return hostname, ip, status

def create_excel(data, output_file):
    wb = Workbook()
    ws = wb.active
    ws.title = 'Ping Results'
    ws.append(['HostName', 'IP', 'Online/Offline'])

    for row in data:
        ws.append(row)
    
    wb.save(output_file)

def log_message(message, log_file):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"{timestamp} - {message}"
    
    # Append log entry
    with open(log_file, 'a') as log:
        log.write(log_entry + "\n")
    
    print(log_entry)  # Print message with timestamp
    
    # Manage log retention
    purge_old_logs(log_file)

def purge_old_logs(log_file):
    if not os.path.exists(log_file):
        return
    
    # Read current log entries
    with open(log_file, 'r') as log:
        log_entries = log.readlines()
    
    # Filter out entries older than LOG_RETENTION_DAYS
    current_time = datetime.now()
    retained_entries = []
    for entry in log_entries:
        try:
            entry_time = datetime.strptime(entry.split(' - ')[0], '%Y-%m-%d %H:%M:%S')
            if current_time - entry_time < timedelta(days=LOG_RETENTION_DAYS):
                retained_entries.append(entry)
        except ValueError:
            # Ignore malformed log entries
            continue
    
    # Write back the retained entries
    with open(log_file, 'w') as log:
        log.writelines(retained_entries)

def select_txt_file():
    txt_files = [f for f in os.listdir() if f.endswith('.txt')]
    if not txt_files:
        print("No .txt files found in the current directory.")
        return None
    
    print("Select a .txt file from the following list:")
    for i, file in enumerate(txt_files, 1):
        print(f"{i}. {file}")
    
    while True:
        try:
            choice = int(input("Enter the number corresponding to the file: "))
            if 1 <= choice <= len(txt_files):
                return txt_files[choice - 1]
            else:
                print("Invalid choice. Please enter a number from the list.")
        except ValueError:
            print("Invalid input. Please enter a number.")

def main(log_file):
    # Select input file
    input_file = select_txt_file()
    if input_file is None:
        return
    
    start_time = time.time()

    with open(input_file, 'r') as file:
        hostnames = [line.strip() for line in file]

    total_hostnames = len(hostnames)
    log_message(f"Starting to ping {total_hostnames} hostnames...", log_file)
    print(f"Total number of machines/hostnames: {total_hostnames}")
    
    results = []
    completed_count = 0
    online_count = 0
    offline_count = 0
    bad_host_count = 0

    def update_progress(status, ip):
        nonlocal completed_count, online_count, offline_count, bad_host_count
        completed_count += 1
        if status == 'online':
            online_count += 1
        elif ip == 'Bad Host':
            bad_host_count += 1
        else:
            offline_count += 1
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"{timestamp} - Progress: {completed_count}/{total_hostnames} ({(completed_count/total_hostnames)*100:.2f}%)", end='\r')
        print(f"\nOnline: {online_count} | Offline: {offline_count} | Bad Host: {bad_host_count}", end='\r')
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_host = {executor.submit(ping_host, hostname): hostname for hostname in hostnames}
        for future in concurrent.futures.as_completed(future_to_host):
            hostname = future_to_host[future]
            try:
                result = future.result()
                results.append(result)
                update_progress(result[2], result[1])
            except Exception as exc:
                log_message(f"{hostname} generated an exception: {exc}", log_file)
            finally:
                update_progress('offline', 'Bad Host')
    
    # Generate output file name with date and time
    date_str = datetime.now().strftime('%d-%b-%y_%H-%M-%S')
    output_file = f'ping_results_{date_str}.xlsx'
    
    create_excel(results, output_file)
    
    end_time = time.time()
    total_time = end_time - start_time

    log_message(f"Finished pinging. Total time: {total_time:.2f} seconds", log_file)
    log_message(f"Total hostnames/machines pinged: {total_hostnames}", log_file)
    log_message(f"Online: {online_count} | Offline: {offline_count} | Bad Host: {bad_host_count}", log_file)
    log_message(f"Output Excel file: {output_file}", log_file)
    print(f"\nOutput Excel file: {output_file}")

if __name__ == "__main__":
    main(LOG_FILE)
