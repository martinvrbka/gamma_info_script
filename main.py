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
import pandas

USER = 'ig'
PASSWORD = '123456'
NUM_THREADS = 8
list_lock = Lock()
write_lock = Lock()


# This function helps to process SSH command and resuts the stdout content
def execute_command(ssh, command):
    (stdin, stdout, stderr) = ssh.exec_command(command)
    return [line.strip() for line in stdout.readlines()], [line.strip() for line in stderr.readlines()]


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

        # Get DB files
        db_files, _ = execute_command(ssh, "ls -a /home/ig/Deso/rcc/machine_data/")

        sql_queries_last_refill = """SELECT c.canisterindex, c.cntcode, max(r.refilldate) lastrefilldate FROM canister c JOIN refill r ON c.canisterid = r.canisterid GROUP BY c.canisterindex ORDER BY c.canisterindex;"""

        sql_queries_dispensed_2021 = "SELECT ch.channelindex, cntcode, round(SUM(IFNULL(volume, 0)), 2) totalvolume_ml FROM channel ch LEFT JOIN channel_action cha ON ch.channelid = cha.channelid AND cha.channelactiontypeid = 1 AND CAST(strftime('%Y', cha.action_date) AS INTEGER) == 2021 GROUP BY ch.channelindex ORDER BY ch.channelindex;"

        sql_queries_dispensed_last_365 = "SELECT ch.channelindex, cntcode, round(SUM(IFNULL(volume, 0)), 2) totalvolume_ml FROM channel ch LEFT JOIN channel_action cha ON ch.channelid = cha.channelid AND cha.channelactiontypeid = 1 AND cha.action_date >= DATE('now','-1 year') GROUP BY ch.channelindex ORDER BY ch.channelindex;"

        sql_queries_shop_location = "SELECT value FROM (SELECT value FROM pos_info WHERE key = 'name' UNION ALL SELECT value FROM pos_info WHERE key = 'city' UNION ALL SELECT value FROM pos_info WHERE key = 'address' UNION ALL SELECT value FROM pos_info WHERE key = 'address2') LIMIT 1;"


        # Loop over the databases and apply same SQLs for each of them if the file exists
        for path, password in [(r'history.db', r'EB._DU[y,M?[m<Ny#8[B?1\b9,>fs\$L9HYwj0Td:#%sn?Bi@YEawT?[lIwQ<sQn7')]:

            # If file missing, ignore entry and continue
            if path not in db_files:
                continue

            # Set chmod and chown for DB path in case it's assigned to root
            execute_command(ssh, "echo 123456 | sudo -S sh -c 'chmod 666 /home/ig/Deso/rcc/machine_data/history.db'")
            execute_command(ssh, "echo 123456 | sudo -S sh -c 'chown ig:ig /home/ig/Deso/rcc/machine_data/history.db'")

            # Password is not uccepted for some reason
            last_refill, stderr = execute_command(ssh,
                                                  r"""echo "PRAGMA KEY='%s'; %s" | sqlcipher /home/ig/Deso/rcc/machine_data/%s""" % (
                                                  password, sql_queries_last_refill, path))

            # Password is not uccepted for some reason
            dispensed_2021, stderr = execute_command(ssh,
                                                  r"""echo "PRAGMA KEY='%s'; %s" | sqlcipher /home/ig/Deso/rcc/machine_data/%s""" % (
                                                  password, sql_queries_dispensed_2021, path))

            # Password is not uccepted for some reason
            dispensed_365, stderr = execute_command(ssh,
                                                  r"""echo "PRAGMA KEY='%s'; %s" | sqlcipher /home/ig/Deso/rcc/machine_data/%s""" % (
                                                  password, sql_queries_dispensed_last_365, path))


            # Password is not uccepted for some reason
            shop_location, stderr = execute_command(ssh,
                                                  r"""echo "PRAGMA KEY='QdfOfezP'; %s" | sqlcipher /home/ig/Deso/redlike/server/db/local.db""" % (
                                                  sql_queries_shop_location))

            print(stderr)

            if stderr:
                raise Exception('Error during SQL execution')
            if (last_refill or dispensed_2021 or dispensed_365 or shop_location) == []:
                raise Exception("Value is missing")

            print(last_refill)
            print(dispensed_2021)
            print(dispensed_365)
            print(shop_location)

        #XML configuration extraction

        with open("data.xml", "w") as fp:
            pass
        xml_path = os.path.dirname(os.path.realpath(__file__)) + "\data.xml"
        print(xml_path)

        # Init SCP
        scp = SCPClient(transport)
        print("init ok")

        # Read machine config
        scp.get('/usr/lib/evodriver/bin/EVOlocal/config/configuration.xml', xml_path)
        doc = minidom.parse(xml_path)

        # Parse file for colorant levels
        try:
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
        except:
            colorant_levels = "No colorant levels present"
            print(colorant_levels)

        # Parse file for serial number
        try:
            dispensers = doc.getElementsByTagName('DISPENSER')

            # In case it's frontdesk, there is nothing to do
            if len(dispensers) == 0:
                raise ValueError('Invalid dispenser file')

            # Get SN
            sn = dispensers[0].attributes['serial_number'].value
            print(sn)
        except:
            sn = "No dispenser present"
            print(sn)

        pos_databases = [
            "main_ppg_be.db", "main_coloris_gamma_nl.db", "main_akzo_nl.db",
            "main_ppg_nl.db", "main_coloris_gamma_be.db", "main_akzo_be.db"
        ]
        dir_content = execute_command(ssh, "echo 123456 | sudo -S sh -c 'ls /home/ig/Deso/redlike/server/db/'")
        pos_formula = [db for db in dir_content if db in pos_databases]
        print(pos_formula)

    finally:
        ssh.close()
        del ssh

    # Creating/adding to csv
    last_refill = {"Last refill": last_refill}
    dispensed_365 = {"Dispensed last 365 days": dispensed_365}
    dispensed_2021 = {"Dispensed in 2021": dispensed_2021}
    shop_location = {"Shop location": shop_location}



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
            # Try to connect 3 times and then raise an error
            for j in range(3):
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
                '%04d / %04d => FAILURE: %s ---- %s' % (i, num_items, host, e,)


# Check arguments
if len(sys.argv) < 2:
    print
    'App has to be started with text file containing IP addresses and path to image to be shown'
    sys.exit(1)
# Check file exists
ip_file_path = sys.argv[1]
if not os.path.isfile(ip_file_path):
    print
    'Definition file does not exist'
    sys.exit(1)
# Load the IP addresses
ip_addresses = [(i, address) for i, address in enumerate(open(ip_file_path, 'r').read().split(r','), start=1)]

# Create and start threads
threads = [Thread(target=process_addresses, args=(ip_addresses, len(ip_addresses), list_lock, write_lock)) for i in
           range(NUM_THREADS)]
for t in threads:
    t.start()
# Wait until they are processed
for t in threads:
    t.join()