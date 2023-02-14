import csv
import subprocess
import concurrent.futures
import datetime
import time

input_file = input("Enter the path to the input file: ")
with open(input_file, "r") as file:
    ips = file.read().splitlines()

output_date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
output_file = f"ping-results_{output_date}.csv" # Add the output date and time to the output file name

def ping(ip):
    response = subprocess.run(['ping', '-n', '1', '-w', '1000', '-l', '32', ip], capture_output=True) # Send one ping packet with 32 bytes and wait up to 1 second for a response
    stdout = response.stdout.decode().strip()
    stderr = response.stderr.decode().strip()
    if "Reply from " in stdout:
        status = "Online"
    elif "could not find host" in stderr:
        status = "Bad hostname"
    elif "Request timed out" in stdout:
        status = "Request timeout"
    else:
        status = "Offline"
    return {'IP': ip, 'Status': status}

start_time = time.monotonic()

with concurrent.futures.ThreadPoolExecutor() as executor:
    results = list(executor.map(ping, ips))

end_time = time.monotonic()
script_runtime = end_time - start_time

with open(output_file, 'w', newline='') as csv_file:
    fieldnames = ['IP', 'Status']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow(result)

print(f"Script runtime: {script_runtime:.2f} seconds")
