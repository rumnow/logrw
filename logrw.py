#!/usr/bin/env python
### Roman Ushakov 30-08-2018
### OPS-11370

import psycopg2
import sys

if len(sys.argv) != 7:
    print("I need 6 papams: host, dbname, user, password, noc, file for parse")
    sys.exit(0)

start_str = "Peer Connection Initiated"  # String to connect
end_str = "Inactivity timeout"  # String to disconnect
fname = sys.argv[6] #"admin-openvpn.log"     # File to check
noc = sys.argv[5]
host = sys.argv[1]
dbname = sys.argv[2]
user = sys.argv[3]
password = sys.argv[4]

try:
    conn = psycopg2.connect("dbname='%s' user='%s' "
                            "host='%s' password='%s'"
                            % (dbname, user, host, password))
except:
    print "I am unable to connect to the database"
    exit(404)

cur = conn.cursor()

try:
    with open(fname + ".size", "rb") as f:
        old_size = int(f.read())
except:
    old_size = 0

try:
    with open(fname, "r") as f:
        f.seek(0, 2)
        fsize = f.tell()
        if old_size == fsize: exit(1)
        f.seek(old_size, 0)
        lines = f.readlines()
except:
    print "File not found or no change: ", fname
    exit(403)

for line in lines:
    if start_str in line:
        list_line = str.split(line)
        sdate = list_line[4] + "-" + list_line[1] + "-" \
                + list_line[2] + " " + list_line[3]
        ip = str.split(list_line[6], ':')[0]
        #print "   Connect " + sdate, ip, str(list_line[7][1:-1])
        l_query = ["connect", sdate, ip, str(list_line[7][1:-1]), str(noc)]
        insert_query = "INSERT INTO public.stats VALUES ('"\
            + str("','".join(l_query)) + "');"
#        print insert_query
        cur.execute(insert_query)

    if end_str in line:
        list_line = str.split(line)
        user_ip = str.split(list_line[6], '/')
        if len(user_ip) < 2:
            user_ip = str.split(list_line[7], '/')
        #print str(user_ip)
        if len(user_ip) < 2:
            continue
        ip = str.split(user_ip[1], ':')[0]
        sdate = list_line[4] + "-" + list_line[1] + "-" \
                + list_line[2] + " " + list_line[3]
        #print "Disconnect " + sdate + " " + ip + " " + str(user_ip[0])
        l_query = ["disconnect", sdate, ip, str(user_ip[0]), str(noc)]
        insert_query = "INSERT INTO public.stats VALUES ('"\
            + str("','".join(l_query)) + "');"
#        print insert_query
        cur.execute(insert_query)
cur.execute("COMMIT;")


with open(fname + ".size", "wb") as f:
    old_size = f.write(str(fsize))  #fsize
