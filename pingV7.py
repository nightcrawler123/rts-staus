import os
import time
import socket
import ping3
import concurrent.futures
import openpyxl
from openpyxl import Workbook
from datetime import datetime

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
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"{timestamp} - {message}"
    with open(log_file, 'a') as log:
        log.write(log_entry + "\n")
    print(log_entry)  # Print message with timestamp

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
    
    # Clear log file
    open(log_file, 'w').close()
    
    start_time = time.time()

    with open(input_file, 'r') as file:
        hostnames = [line.strip() for line in file]

    total_hostnames = len(hostnames)
    log_message(f"Starting to ping {total_hostnames} hostnames...", log_file)
    print(f"Total number of machines/hostnames: {total_hostnames}")
    
    results = []
    completed_count = 0

    def update_progress():
        nonlocal completed_count
        completed_count += 1
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"{timestamp} - Progress: {completed_count}/{total_hostnames} ({(completed_count/total_hostnames)*100:.2f}%)", end='\r')
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_host = {executor.submit(ping_host, hostname): hostname for hostname in hostnames}
        for future in concurrent.futures.as_completed(future_to_host):
            hostname = future_to_host[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as exc:
                log_message(f"{hostname} generated an exception: {exc}", log_file)
            finally:
                update_progress()
    
    # Generate output file name with date and time
    date_str = datetime.now().strftime('%d-%b-%y_%H-%M-%S')
    output_file = f'ping_results_{date_str}.xlsx'
    
    create_excel(results, output_file)
    
    end_time = time.time()
    total_time = end_time - start_time

    log_message(f"Finished pinging. Total time: {total_time:.2f} seconds", log_file)
    log_message(f"Total hostnames/machines pinged: {total_hostnames}", log_file)
    log_message(f"Output Excel file: {output_file}", log_file)
    print(f"\nOutput Excel file: {output_file}")

if __name__ == "__main__":
    log_file = 'ping_log.txt'  # Log file
    
    main(log_file)
