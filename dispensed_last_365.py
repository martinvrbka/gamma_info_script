from threading import Lock, Thread
import paramiko
import os
import sys
import time

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

        sql_queries = "SELECT ch.channelindex, cntcode, round(SUM(IFNULL(volume, 0)), 2) totalvolume_ml FROM" \
                      " channel ch LEFT JOIN channel_action cha ON" \
                      " ch.channelid = cha.channelid AND cha.channelactiontypeid = 1 AND" \
                      " cha.action_date >= DATE('now','-1 year') GROUP BY ch.channelindex ORDER BY ch.channelindex;"

        # Loop over the databases and apply same SQLs for each of them if the file exists
        for path, password in [(r'history.db', r'EB._DU[y,M?[m<Ny#8[B?1\b9,>fs$L9HYwj0Td:#%sn?Bi@YEawT?[lIwQ<sQn7')]:

            # If file missing, ignore entry and continue
            if path not in db_files:
                raise Exception('Error database file not found')
                continue

            # Set chmod and chown for DB path in case it's assigned to root
            execute_command(ssh, "echo 123456 | sudo -S sh -c 'chmod 644 /home/ig/Deso/redlike/server/db/%s'" % (path,))
            execute_command(ssh,
                            "echo 123456 | sudo -S sh -c 'chown ig:ig /home/ig/Deso/redlike/server/db/%s'" % (path,))

            # Run SQL queries
            stdout, stderr = execute_command(ssh,
                                             r"""echo "PRAGMA KEY='%s'; %s" | sqlcipher /home/ig/Deso/redlike/server/db/%s""" % (
                                             password, sql_queries, path))
            if stderr:
                print
                'Error during SQL execution for Host: %s, DB: %s, error: %s' % (host, path, stderr,)
                raise Exception('Error during SQL execution')
            if stdout == []:
                raise Exception('Table pos_info in DB: %s does not contain name value in key column ' % path)
                continue

    finally:
        ssh.close()
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