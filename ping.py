import os
import time
import socket
import ping3
import concurrent.futures
import openpyxl
from openpyxl import Workbook

def ping_host(hostname):
    try:
        ip = socket.gethostbyname(hostname)
        response = ping3.ping(ip)
        status = 'online' if response else 'offline'
    except (socket.gaierror, ping3.errors.HostUnknown):
        ip = 'N/A'
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
    with open(log_file, 'a') as log:
        log.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")
    print(message)

def main(input_file, output_file, log_file):
    # Clear log file
    open(log_file, 'w').close()
    
    start_time = time.time()

    with open(input_file, 'r') as file:
        hostnames = [line.strip() for line in file]

    log_message(f"Starting to ping {len(hostnames)} hostnames...", log_file)
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_host = {executor.submit(ping_host, hostname): hostname for hostname in hostnames}
        for future in concurrent.futures.as_completed(future_to_host):
            hostname = future_to_host[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as exc:
                log_message(f"{hostname} generated an exception: {exc}", log_file)
    
    create_excel(results, output_file)
    
    end_time = time.time()
    total_time = end_time - start_time

    log_message(f"Finished pinging. Total time: {total_time:.2f} seconds", log_file)
    log_message(f"Total hostnames/machines pinged: {len(hostnames)}", log_file)

if __name__ == "__main__":
    input_file = 'hostnames.txt'  # Input text file with hostnames
    output_file = 'ping_results.xlsx'  # Output Excel file
    log_file = 'ping_log.txt'  # Log file
    
    main(input_file, output_file, log_file)
