import sqlite3
from pprint import pprint
conn = sqlite3.connect('dhl_data.db')


c = conn.cursor()

# Create table
# need to figure out how to add IRQ issue - Maybe. May not need issue in the future if fix takes effect.
# c.execute('''CREATE TABLE bridges
#              (een text, b_esn text primary key, firmware text)''')
monit_matches = {"monit daemon awakened": 0,
                 "'bridge' process is running": 0,
                 "zombie": 0,
                 "matches resource limit": 0,
                 "Monit started": 0,
                 "Monit reloaded": 0,
                 "restart action done": 0,
                 "trying to restart": 0,
                 "user request": 0,
                 "monit process failures": 0,
                 "bridge_alive": 0,
                 "Awakened by User defined signal": 0,
                 "HTTP server": 0}

c.execute('''SELECT *
FROM
    monit
INNER JOIN monit_restarts ON monit.b_esn = monit_restarts.b_esn
WHERE monit_restarts.monit_id == monit.monit_id
OR monit_restarts.monit_id - 1 == monit.monit_id
OR monit_restarts.monit_id - 2 == monit.monit_id
OR monit_restarts.monit_id - 3 == monit.monit_id
OR monit_restarts.monit_id - 4 == monit.monit_id''')

# Insert a row of data
# c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
all_rows = c.fetchall()
print("matches return {}".format(len(all_rows)))
# for row in all_rows:
#     pprint(row)


for row in all_rows:
    if "awakened" in row[1]:
        monit_matches["monit daemon awakened"] += 1
    elif "'bridge' process is running" in row[1]:
        monit_matches["'bridge' process is running"] += 1
    elif "zombie" in row[1]:
        monit_matches["zombie"] += 1
    elif "matches resource limit" in row[1]:
        monit_matches["matches resource limit"] += 1
    elif "Monit started" in row[1]:
        monit_matches["Monit started"] += 1
    elif "Monit reloaded" in row[1]:
        monit_matches["Monit reloaded"] += 1
    elif "restart action done" in row[1]:
        monit_matches["restart action done"] += 1
    elif "trying to restart" in row[1]:
        monit_matches["trying to restart"] += 1
    elif "user request" in row[1]:
        monit_matches["user request"] += 1
    elif "monit:" in row[1]:
        monit_matches["monit process failures"] += 1
    elif "bridge_alive" in row[1]:
        monit_matches["bridge_alive"] += 1
    elif "Awakened by User defined signal" in row[1]:
        monit_matches["Awakened by User defined signal"] += 1
    elif "HTTP server" in row[1]:
        monit_matches["HTTP server"] += 1
    else:
        try:
            monit_matches[row[1]] += 1
        except:
            monit_matches[row[1]] = 1
pprint(monit_matches)





# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()