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


c.execute('''SELECT incident_guid, incidents.b_esn, bridges.name, incidents.c_esn, CAST(strftime("%d",stop) as INTEGER), CAST(strftime("%m",stop) as INTEGER),  incident_start, incident_stop,  total_bridge_cams, incident_cams, drop_type, duration_sum, duration_sum/incident_cams
            FROM incidents
            INNER JOIN bridges
            ON bridges.b_esn = incidents.b_esn
            WHERE CAST(strftime("%m",stop) as INTEGER) = 10 AND CAST(strftime("%d",stop) as INTEGER) in (28) AND drop_type = "bridge" AND incident_guid = ?
            GROUP BY c_esn
            ORDER BY duration_sum DESC;''', ("100a54e92019-10-28 04:12:00", ))

# Insert a row of data
# c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
all_rows = c.fetchall()
for row in all_rows:
    print row
