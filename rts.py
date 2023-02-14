import csv
import subprocess

with open("C:/Path/To/IPList.txt", "r") as file: # Replace with the path to your IP list file
    ips = file.read().splitlines()

output_file = "ping-results.csv" # Replace with your desired output file name and path

results = []
for ip in ips:
    response = subprocess.call(['ping', '-n', '1', '-w', '1000', ip]) # Send one ping packet and wait up to 1 second for a response
    if response == 0:
        status = "Online"
    else:
        status = "Offline"
    results.append({'IP': ip, 'Status': status})

with open(output_file, 'w', newline='') as csv_file:
    fieldnames = ['IP', 'Status']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow(result)
