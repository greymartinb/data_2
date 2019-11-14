import sqlite3
conn = sqlite3.connect('dhl_data.db')


c = conn.cursor()

# Create table
# need to figure out how to add IRQ issue - Maybe. May not need issue in the future if fix takes effect.
# c.execute('''CREATE TABLE bridges
#              (een text, b_esn text primary key, firmware text)''')
#
# by User defined signal




c.execute('''SELECT at, b_esn, monit
             FROM monit
             WHERE monit LIKE "bridge_connected %" ''')

# Insert a row of data
# c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")

all_rows = c.fetchall()
print(len(all_rows))
for row in all_rows:
    print row



# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()