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
title = 'ORD-12-24'

c.execute('''SELECT incident_guid, incidents.b_esn, bridges.name, CAST(strftime("%d",stop) as INTEGER), CAST(strftime("%m",stop) as INTEGER), incident_start, incident_stop, total_bridge_cams, incident_cams, drop_type, duration_sum, duration_sum/incident_cams
            FROM incidents
            INNER JOIN bridges
            ON bridges.b_esn = incidents.b_esn
            WHERE CAST(strftime("%m",stop) as INTEGER) in (12) AND incidents.b_esn in ('10007c0c','1000fede','100248df','1002ea86','1004c0ff','10083044','100eed3d') AND duration_sum/incident_cams > 10 AND CAST(strftime("%Y",stop) as INTEGER) = 2024
            GROUP BY incident_guid
            ORDER BY duration_sum DESC;''')

# Insert a row of data
# c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
all_rows = c.fetchall()
# for row in all_rows:
#     print row
with open("reports/{}.csv".format(title), "a") as f:
    writer = csv.writer(f, f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
    header = ["incident_guid", "b_esn", "b_name", "day", "month", "start", "stop", "cam_count", "inc_cams", "drop_type", "duration_sum", "avg"]
    writer.writerow(header)
    for row in all_rows:
        print(row)
        writer.writerow(row)

for idx, row in enumerate(all_rows):
#     if idx < 11:
    b_esn = row[1]
    incident_guid = str(row[0])
    start = str(row[5])
    with open("reports/{}_detailed.csv".format(title), "a") as f:
        writer = csv.writer(f, f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        header = ["vg_id", "incident_guid", "b_esn", "b_name", "c_esn", "s_guid", "day", "month", "start", "stop", "cam_count", "inc_cams", "drop_type", "duration_sum", "avg"]
        if idx == 0:
            writer.writerow(header)
        c.execute('''SELECT video_gaps_id, incident_guid, incidents.b_esn, bridges.name, incidents.c_esn, cams.switch_guid, CAST(strftime("%d",stop) as INTEGER), CAST(strftime("%m",stop) as INTEGER),  start, stop,  total_bridge_cams, incident_cams, drop_type, duration_sum, duration_sum/incident_cams
                FROM incidents
                INNER JOIN bridges
                ON bridges.b_esn = incidents.b_esn
                INNER JOIN cams
                ON incidents.c_esn = cams.c_esn
                WHERE incident_guid = ?
                GROUP BY  video_gaps_id, incident_guid
                ORDER BY duration_sum DESC;''', (incident_guid,))
        all_rows = c.fetchall()
        for row in all_rows:
            writer.writerow(row)

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
# conn.close()
