from scp import SCPClient
from threading import Lock, Thread

import base64
import binascii
import paramiko
import os
import sys
import time
import traceback
from xml.dom import minidom

USER = 'ig'
PASSWORD = '123456'
NUM_THREADS = 4

list_lock = Lock()
write_lock = Lock()


# This function helps to process SSH command and resuts the stdout content
def execute_command(ssh, command):
    (stdin, stdout, stderr) = ssh.exec_command(command)
    return [line.strip() for line in stdout.readlines()]


# This method handles connection to the single host
def handle_host(host):
    # Init connection
    ssh = paramiko.SSHClient()
    try:
        # Init SSH
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, username=USER, password=PASSWORD)
        transport = ssh.get_transport()
        channel = transport.open_session()
        channel.setblocking(1)
        channel.settimeout(30)

        # Parse file
        doc = minidom.parse(r'configuration.xml')
        dispensers = doc.getElementsByTagName('DISPENSER')

        # In case it's frontdesk, there is nothing to do
        if len(dispensers) == 0:
            raise ValueError('Invalid dispenser file')

        # Get colorants
        data = {}
        for colorant in doc.getElementsByTagName('COLORANT'):
            cnt_code = colorant.attributes['code'].value
            for canister in colorant.getElementsByTagName('CANISTER'):
                index = int(canister.attributes['id'].value)
                level = float(canister.attributes['cur_q'].value)
                data[index] = (cnt_code, level)

        for index in sorted(data.keys()):
            cnt_code, level = data[index]
            print('%d - %s - %.2f ml' % (index, cnt_code, level,))
    finally:
        del ssh


# Thread processing method
def process_addresses(lst, num_items, list_lock, write_lock):
    while True:
        # Get the item from list of IP addresses
        with list_lock:
            if not lst:
                break

            i, host = lst.pop(0)

        # Send commands to the host
        try:
            # Try to connect 5 times and then raise an error
            for j in range(5):
                try:
                    handle_host(host)
                    break
                except:
                    if j < 2:
                        time.sleep(5.0)
                        continue

                    raise

            with write_lock:
                print
                '%04d / %04d => OK:      %s' % (i, num_items, host,)

        except Exception as e:
            with write_lock:
                print
                '%04d / %04d => FAILURE: %s' % (i, num_items, host,)


# Check arguments
if len(sys.argv) < 2:
    print
    'App has to be started with text file containing IP addresses'
    sys.exit(1)

# Check file exists
path = sys.argv[1]
if not os.path.isfile(path):
    print
    'Definition file not existing'
    sys.exit(1)

# Load the IP addresses
ip_addresses = [(i, address) for i, address in enumerate(open(path, 'r').read().split(r','), start=1)]

# Create and start threads
threads = [Thread(target=process_addresses, args=(ip_addresses, len(ip_addresses), list_lock, write_lock)) for i in
           range(NUM_THREADS)]
for t in threads:
    t.start()

# Wait until they are processed
for t in threads:
    t.join()