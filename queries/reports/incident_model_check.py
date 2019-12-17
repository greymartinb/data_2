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


c.execute('''SELECT incident_guid, cams.model, cams.make
            FROM incidents
            INNER JOIN cams
            ON cams.c_esn = incidents.c_esn
            WHERE CAST(strftime("%m",stop) as INTEGER) = 12 AND duration_sum/incident_cams > 30 AND CAST(strftime("%d",stop) as INTEGER) = 9 AND incident_cams = 1
            GROUP BY incident_guid
            ORDER BY duration_sum DESC;''')


# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
# conn.close()

all_rows = c.fetchall()

for row in all_rows:
    print row
