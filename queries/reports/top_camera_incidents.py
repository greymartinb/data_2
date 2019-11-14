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


c.execute('''SELECT b_esn, c_esn, total_bridge_cams, incident_cams, incident_dif, drop_type, start, stop, duration_sum
            FROM incidents
            WHERE CAST(strftime("%m",stop) as INTEGER) = 11 AND CAST(strftime("%d",stop) as INTEGER) in (1,2) and drop_type = "camera"
            GROUP BY c_esn
            ORDER BY duration_sum DESC;''')

# Insert a row of data
# c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
all_rows = c.fetchall()
print(len(all_rows))
for idx, row in enumerate(all_rows):
    if idx < 10:
        print(row)

# # Save (commit) the changes
with open("reports/top_camera_loss_index_{}.csv".format(now_string), "wb") as f:
    writer = csv.writer(f, f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
    header = ["bridge_esn", "c_esn", "total_bridge_cams", "incident_cams", "incident_dif", "drop_type", "start", "stop", "duration_sum"]
    writer.writerow(header)
    for idx, row in enumerate(all_rows):
        if idx < 11:
            print(row)
            writer.writerow(row)

for idx, row in enumerate(all_rows):
    if idx < 11:
        cam_esn = row[1]
        with open("reports/top_camera_loss_{}_{}.csv".format(cam_esn, now_string), "wb") as f:
            writer = csv.writer(f, f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            header = ["bridge_esn", "c_esn", "start", "stop", "dif"]
            writer.writerow(header)
            c.execute(''' SELECT b_esn, c_esn, start, stop, CAST(strftime("%s", stop) as INTEGER) - CAST(strftime("%s", start) as INTEGER)
                          FROM video_gaps
                          WHERE c_esn = ? AND CAST(strftime("%d",stop) as INTEGER) in (1,2) AND CAST(strftime("%m",stop) as INTEGER) = 11
                          GROUP BY start
                          ORDER BY start DESC;''', (cam_esn,))
            all_rows = c.fetchall()
            for row in all_rows:
                writer.writerow(row)

# # We can also close the connection if we are done with it.
# # Just be sure any changes have been committed or they will be lost.
# conn.close()

wb = xlwt.Workbook()

for filename in glob.glob("reports/*_{}.csv".format(now_string)):
    (f_path, f_name) = os.path.split(filename)
    f_name = f_name.replace("top_camera_loss_", "").replace(now_string, "").replace("_.csv", "")
    ws = wb.add_sheet(f_name)
    spamReader = csv.reader(open(filename, 'rb'))
    for rowx, row in enumerate(spamReader):
        for colx, value in enumerate(row):
            ws.write(rowx, colx, value)
    wb.save("reports/final/top_camera_loss_{}.xls".format(now_string))