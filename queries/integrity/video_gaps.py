import sqlite3
conn = sqlite3.connect('dhl_data.db')
from pprint import pprint


c = conn.cursor()

# Create table


c.execute('''SELECT c_esn, start, stop, SUM(CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER)), SUM(CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER))/60, CAST(strftime("%m",stop) as INTEGER)
            FROM video_gaps
            WHERE CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER) > 10 AND CAST(strftime("%d",stop) as INTEGER) = 19 AND CAST(strftime("%m",stop) as INTEGER) = 10
            GROUP BY c_esn
            ORDER BY SUM(CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER)) DESC;''')

# Insert a row of data
# c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
all_rows = c.fetchall()
for row in all_rows:
    print(row)


# Save (commit) the changes
conn.commit()
