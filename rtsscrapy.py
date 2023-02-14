import csv
import datetime
import socket
from scapy.all import sr1, IP, ICMP

# Get input file name from user
input_file = input("Enter input file name: ")

# Set ping parameters
ping_timeout = 1  # seconds
ping_size = 32  # bytes

# Get current date and time for output file name
output_file = f"ping_results_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"

# Open input and output files
with open(input_file, "r") as in_file, open(output_file, "w", newline="") as out_file:
    # Create CSV writer
    csv_writer = csv.writer(out_file)
    # Write header row to CSV file
    csv_writer.writerow(["IP", "Status"])

    # Loop through each IP in input file
    for ip in in_file:
        ip = ip.strip()  # Remove whitespace

        # Try to resolve hostname to IP address
        try:
            ip_address = socket.gethostbyname(ip)
        except socket.gaierror:
            # Hostname could not be resolved
            status = "Bad hostname"
        else:
            # Send ICMP ping request
            reply = sr1(IP(dst=ip_address)/ICMP(), timeout=ping_timeout, verbose=False)

            # Check if ping was successful
            if reply is None:
                # Request timed out
                status = "Request timeout"
            elif reply.type == 0:
                # Ping was successful
                status = "Online"
            else:
                # Host is up, but did not reply to ping
                status = "Offline"

        # Write IP and status to CSV file
        csv_writer.writerow([ip, status])

print("Ping results written to", output_file)
print("Script runtime:", datetime.datetime.now() - start_time)
