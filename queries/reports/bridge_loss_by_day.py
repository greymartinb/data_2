import sqlite3
conn = sqlite3.connect('dhl_data.db')
from pprint import pprint


c = conn.cursor()

# Create table


c.execute('''SELECT c_esn, b_esn, CAST(strftime("%m",stop) as INTEGER), CAST(strftime("%d",stop) as INTEGER), SUM(CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER))
            FROM video_gaps
            WHERE CAST(strftime("%m",stop) as INTEGER) = 10 AND CAST(strftime("%d",stop) as INTEGER) = 19 and CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER) > 10
            GROUP BY CAST(strftime("%d",stop) as INTEGER)
            ORDER BY CAST(strftime("%d",stop) as INTEGER) DESC;''')

# Insert a row of data
# c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
all_rows = c.fetchall()
pprint(all_rows)

# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()