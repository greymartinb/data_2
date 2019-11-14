import sqlite3
conn = sqlite3.connect('dhl_data.db')
from pprint import pprint


c = conn.cursor()

# Create table


c.execute('''SELECT c_esn, SUM(CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER)), strftime("%H:%M:%S", start), strftime("%H:%M:%S", stop)
            FROM video_gaps
            WHERE CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER) > 10 AND CAST(strftime("%d",stop) as INTEGER) = 29 AND c_esn = '1010a030'
            GROUP BY start
            ORDER BY SUM(CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER)) ASC;''')

# Insert a row of data
# c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
all_rows = c.fetchall()
pprint(all_rows)

# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()