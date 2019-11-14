import sqlite3
conn = sqlite3.connect('dhl_data.db')


c = conn.cursor()

# Create table
# need to figure out how to add IRQ issue - Maybe. May not need issue in the future if fix takes effect.
# c.execute('''CREATE TABLE bridges
#              (een text, b_esn text primary key, firmware text)''')


c.execute('''SELECT c_esn, id, at
            FROM cams
            WHERE switch_guid IN (SELECT switch_guid FROM switches)''')

# Insert a row of data
# c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
all_rows = c.fetchall()
print("matches switches {}".format(len(all_rows)))


# c.execute('''SELECT c_esn, id, at
#             FROM cams
#             WHERE c_esn IN (SELECT c_esn FROM parent_cams)''')

# # Insert a row of data
# # c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
# all_rows = c.fetchall()
# print("matches parent_cams {}".format(len(all_rows)))

# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()