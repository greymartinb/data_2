import sqlite3
conn = sqlite3.connect('dhl_data.db')
from pprint import pprint
from datetime import datetime, timedelta

# 2019-09-29 23:59:55+00:00

dt_format = ""


c = conn.cursor()

# Create table

videogaps = []


c.execute('''SELECT c_esn, start, stop, CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER)
            FROM video_gaps
            WHERE CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER) > 10 AND CAST(strftime("%d",stop) as INTEGER) = 29 AND b_esn = '10061de1'
            GROUP BY start
            ORDER BY start ASC;''')

# Insert a row of data
# c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
all_rows = c.fetchall()
for row in all_rows:
    vg = {"c_esn": row[0],
          "start": row[1],
          "stop": row[2],
          "dif": row[3]}
    videogaps.append(vg)

for row in videogaps:
    print(row)
# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()