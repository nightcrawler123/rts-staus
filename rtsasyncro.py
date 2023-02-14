import asyncio
import csv
import os
import subprocess
import time

async def ping(ip):
    proc = await asyncio.create_subprocess_shell(f'ping -n 1 -w 1000 -l 32 {ip}', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=1.2)
    if "Reply from" in stdout.decode():
        return ip, "Online"
    elif "host unreachable" in stdout.decode():
        return ip, "Offline"
    elif "Ping request could not find host" in stdout.decode():
        return ip, "Bad hostname"
    else:
        return ip, "Request timeout"

async def main():
    # Ask user for input file
    input_file = input("Enter input file path: ")
    
    # Open input and output file
    with open(input_file, 'r') as f:
        ips = f.readlines()
    ips = [ip.strip() for ip in ips]
    
    current_time = time.strftime("%Y%m%d-%H%M%S")
    output_file = f"ping_result_{current_time}.csv"

    # Send ping requests asynchronously
    ping_results = await asyncio.gather(*(ping(ip) for ip in ips))

    # Write ping results to CSV file
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["IP", "Status"])
        writer.writerows(ping_results)
        
    print(f"Ping results saved to {output_file}")

if __name__ == '__main__':
    start_time = time.monotonic()
    asyncio.run(main())
    end_time = time.monotonic()
    print(f"Script runtime: {end_time - start_time} seconds")
