import sqlite3
conn = sqlite3.connect('dhl_data.db')
from pprint import pprint
import os
import glob
import csv
import xlwt
from datetime import datetime, timedelta

now = datetime.now()

title_format = "%Y-%m-%d"

now_string = datetime.strftime(now, title_format)

c = conn.cursor()

# Create table


c.execute('''SELECT incident_guid, incident_start, incident_stop, b_esn, total_bridge_cams, incident_cams, incident_dif, SUM(CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s", start) as INTEGER)), drop_type, duration_sum, duration_sum/incident_cams
            FROM incidents
            WHERE CAST(strftime("%m",stop) as INTEGER) = 10 AND CAST(strftime("%d",stop) as INTEGER) in (17,18) and drop_type = "bridge"
            GROUP BY incident_guid
            ORDER BY duration_sum DESC;''')

# Insert a row of data
# c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
all_rows = c.fetchall()
print("after")
for row in all_rows:
    print row
