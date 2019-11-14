import sqlite3
conn = sqlite3.connect('dhl_data.db')


c = conn.cursor()

# Create table
# need to figure out how to add IRQ issue - Maybe. May not need issue in the future if fix takes effect.
# c.execute('''CREATE TABLE bridges
#              (een text, b_esn text primary key, firmware text)''')


c.execute('''CREATE TABLE streamer
             ( id INTEGER primary key AUTOINCREMENT,
               streamer text,
               b_esn text,
               uuid text,
               at datetime,
               Foreign Key (b_esn) REFERENCES bridges(b_esn),
               Foreign Key (uuid) REFERENCES cams(uuid))''')

# Insert a row of data
# c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")

# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()