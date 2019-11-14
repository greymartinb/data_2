import sqlite3
conn = sqlite3.connect('dhl_data.db')


c = conn.cursor()

# Create table
# need to figure out how to add IRQ issue - Maybe. May not need issue in the future if fix takes effect.
# c.execute('''CREATE TABLE bridges
#              (een text, b_esn text primary key, firmware text)''')


c.execute('''CREATE TABLE cams
             (cams_id INTEGER primary key AUTOINCREMENT,
              c_esn text,
              b_esn text,
              p_esn text,
              switch_guid text,
              name text,
              model text,
              onvif boolean,
              rtsp boolean,
              uuid text,
              make text,
              firmware text,
              at datetime,
              Foreign Key (b_esn) REFERENCES bridges(b_esn),
              Foreign Key (switch_guid) REFERENCES switches(switch_guid),
              Foreign Key (p_esn) REFERENCES parent_cams(b_esn))''')

# Insert a row of data
# c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")

# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()