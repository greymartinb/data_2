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


c.execute('''SELECT incident_guid, incidents.b_esn, bridges.name, CAST(strftime("%d",stop) as INTEGER), CAST(strftime("%m",stop) as INTEGER),  incident_start, incident_stop,  total_bridge_cams, incident_cams, drop_type, duration_sum, duration_sum/incident_cams
            FROM incidents
            INNER JOIN bridges
            ON bridges.b_esn = incidents.b_esn
            WHERE CAST(strftime("%m",stop) as INTEGER) = 10 AND incidents.b_esn = "100a54e9" AND drop_type != "switch"
            GROUP BY incident_guid
            ORDER BY duration_sum DESC;''')

# Insert a row of data
# c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
all_rows = c.fetchall()
# for row in all_rows:
#     print row
with open("reports/DFW_1_summary.csv", "ab") as f:
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
    with open("reports/DFW_1.csv", "ab") as f:
        writer = csv.writer(f, f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        # header = ["incident_guid", "b_esn", "b_name", "c_esn", "day", "month", "start", "stop", "cam_count", "inc_cams", "drop_type", "duration_sum", "avg"]
        writer.writerow(header)
        c.execute('''SELECT incident_guid, incidents.b_esn, bridges.name, incidents.c_esn, CAST(strftime("%d",stop) as INTEGER), CAST(strftime("%m",stop) as INTEGER),  incident_start, incident_stop,  total_bridge_cams, incident_cams, drop_type, duration_sum, duration_sum/incident_cams
                FROM incidents
                INNER JOIN bridges
                ON bridges.b_esn = incidents.b_esn
                WHERE CAST(strftime("%m",stop) as INTEGER) = 10 AND incident_guid = ?
                GROUP BY c_esn
                ORDER BY duration_sum DESC;''', (incident_guid,))
        all_rows = c.fetchall()
        for row in all_rows:
                writer.writerow(row)

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
# conn.close()

# wb = xlwt.Workbook()

# for filename in glob.glob("reports/*_{}.csv".format(now_string)):
#     (f_path, f_name) = os.path.split(filename)
#     f_name = str(f_name.replace("top_bridge_incidents_", "").replace(now_string, "").replace("_.csv", "").replace(" ", "_"))
#     ws = wb.add_sheet(f_name)
#     spamReader = csv.reader(open(filename, 'rb'))
#     for rowx, row in enumerate(spamReader):
#         for colx, value in enumerate(row):
#             ws.write(rowx, colx, value)
#     wb.save("reports/final/top_bridge_incidents_{}.xls".format(now_string))