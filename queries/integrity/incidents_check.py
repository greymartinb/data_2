import sqlite3
conn = sqlite3.connect('dhl_data.db')
from pprint import pprint


c = conn.cursor()

c.execute('''SELECT *
            FROM video_gaps
            WHERE  b_esn = ? and CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER) > 10
            ORDER BY start ASC;''', ("1000fede",))

    # Insert a row of data
    # c.execute("INSERT INTO )

all_rows = c.fetchall()
for row in all_rows:
    print row