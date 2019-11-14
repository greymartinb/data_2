import sqlite3
conn = sqlite3.connect('dhl_data.db')
from pprint import pprint
from datetime import datetime, timedelta

# 2019-09-29 23:59:55+00:00

dt_format = ""


c = conn.cursor()

# Create table

videogaps = []


c.execute('''SELECT incident_guid, total_bridge_cams, b_esn
            FROM incidents
            WHERE CAST(strftime("%m",stop) as INTEGER) = 10 AND b_esn = "10007c0c" AND drop_type = "switch"
            GROUP BY incident_guid
            ORDER BY incident_guid ASC;''')

# Insert a row of data
# c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
all_rows = c.fetchall()
for row in all_rows:
  print row