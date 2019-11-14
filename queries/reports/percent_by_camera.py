import sqlite3
conn = sqlite3.connect('dhl_data.db')
from pprint import pprint
from decimal import *
TWOPLACES = Decimal(10) ** -4
import csv


c = conn.cursor()

# Create table

bridges = dict()

# gets canera_count
c.execute('''SELECT bridges.name, cams.b_esn, count(c_esn)
            FROM cams
            INNER JOIN bridges
            ON cams.b_esn = bridges.b_esn
            WHERE cams.name NOT LIKE "Off %"
            GROUP BY cams.b_esn
            ORDER BY cams.b_esn;''')

all_rows = c.fetchall()
for row in all_rows:
    bridges[row[1]] = {"name": row[0],
                       "count": row[2],
                       "esn": row[1],
                       "total_secs": row[2] * 86400,
                       "flagged": False}



pprint(len(bridges.keys()))
# Insert a row of data

c.execute('''SELECT video_gaps.b_esn, bridges.name, cams.c_esn, cams.name, SUM(CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER))
            FROM video_gaps
            INNER JOIN cams
            ON video_gaps.c_esn = cams.c_esn
            INNER JOIN bridges
            ON video_gaps.b_esn = bridges.b_esn
            WHERE CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER) > 10  AND cams.name NOT LIKE "Off %" AND CAST(strftime("%m",stop) as INTEGER) = 10
            GROUP BY video_gaps.c_esn
            ORDER BY SUM(CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER)) DESC;''')

all_rows = c.fetchall()
print(len(all_rows))
for idx, row in enumerate(all_rows):
    print row
    if idx == 20:
        break
#     data = Decimal(float((bridges[row[0]]["total_secs"] - row[2]) / float(bridges[row[0]]["total_secs"]))).quantize(TWOPLACES)
#     string = str(100 * data) + "%"
#     print(row[0], row[1], row[2], string)
#     bridges[row[0]]["flagged"] = True

# for key in bridges.keys():
#     if bridges[key]["flagged"] is not True:
#         print bridges[key]["esn"], bridges[key]["name"], "100.0000%"






# pprint(bridges)


# with open("reports/final/bridge_loss_10-13v10-16.csv", "wb") as f:
#         keys = ["name", "esn", "cam_count", "before_loss", "after_loss", "change", "per_change", "category"]
#         writer = csv.DictWriter(f, fieldnames=keys, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
#         writer.writeheader()
#         for item in bridges:
#             try:
#                 writer.writerow(bridges[item])
#             except:
#                 print(bridges[item])
#                 raise