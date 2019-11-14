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


c.execute('''SELECT cams.b_esn, bridges.name,  video_gaps.c_esn, cams.name, cams.make, cams.model, SUM(CAST(strftime("%s",video_gaps.stop) as INTEGER) - CAST(strftime("%s", video_gaps.start) as INTEGER))
            FROM video_gaps
            INNER JOIN cams
            ON cams.c_esn = video_gaps.c_esn
            INNER JOIN bridges
            ON bridges.b_esn = video_gaps.b_esn
            WHERE CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER) > 10 AND CAST(strftime("%d",stop) as INTEGER) = 15 AND video_gaps.b_esn != "100c3985" AND video_gaps.b_esn != "10035cb5"
            GROUP BY video_gaps.c_esn
            ORDER BY SUM(CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER)) DESC;''')

# Insert a row of data
# c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
all_rows = c.fetchall()
for idx, row in enumerate(all_rows):
    if idx < 5:
        print(row)

# Save (commit) the changes
with open("reports/top_camera_loss_index_{}.csv".format(now_string), "wb") as f:
    writer = csv.writer(f, f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
    header = ["bridge_esn", "bridge_name", "cam_esn", "cams_name", "cams_make", "cams_model", "total_lost (secs)"]
    writer.writerow(header)
    for idx, row in enumerate(all_rows):
        if idx < 11:
            print(row)
            writer.writerow(row)

for idx, row in enumerate(all_rows):
    if idx < 11:
        cam_esn = row[2]
        with open("reports/top_camera_loss_{}_{}.csv".format(cam_esn, now_string), "wb") as f:
            writer = csv.writer(f, f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            header = ["bridge_esn", "c_esn", "start", "stop", "dif"]
            writer.writerow(header)
            c.execute(''' SELECT b_esn, c_esn, start, stop, CAST(strftime("%s", stop) as INTEGER) - CAST(strftime("%s", start) as INTEGER)
                          FROM video_gaps
                          WHERE c_esn = ? AND CAST(strftime("%d",stop) as INTEGER) = 15
                          GROUP BY start
                          ORDER BY start DESC;''', (cam_esn,))
            all_rows = c.fetchall()
            for row in all_rows:
                writer.writerow(row)

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()

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










