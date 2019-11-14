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

c.execute('''SELECT video_gaps.b_esn, bridges.name, SUM(CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER)), CAST(strftime("%d",stop) as INTEGER), CAST(strftime("%m",stop) as INTEGER)
            FROM video_gaps
            INNER JOIN cams
            ON video_gaps.c_esn = cams.c_esn
            INNER JOIN bridges
            ON video_gaps.b_esn = bridges.b_esn
            WHERE CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER) > 10 AND CAST(strftime("%d",stop) as INTEGER) = 26 AND cams.name NOT LIKE "Off %" AND CAST(strftime("%m",stop) as INTEGER) = 10
            GROUP BY video_gaps.b_esn
            ORDER BY video_gaps.b_esn;''')

with open("reports/final/bridge_percent_10-23_10-26.csv", "ab") as f:
        keys = ["esn", "name", "loss", "preview_percent", "day", "month"]
        writer = csv.writer(f, f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        # writer.writerow(keys)
        all_rows = c.fetchall()
        print(len(all_rows))
        for row in all_rows:
            # print row
            data = Decimal(float((bridges[row[0]]["total_secs"] - row[2]) / float(bridges[row[0]]["total_secs"]))).quantize(TWOPLACES)
            string = str(100 * data) + "%"
            writer.writerow([row[0], row[1], row[2], string, row[3], row[4]])
            bridges[row[0]]["flagged"] = True

        for key in bridges.keys():
            if bridges[key]["flagged"] is not True:
                writer.writerow([bridges[key]["esn"], bridges[key]["name"], "0", "100.0000%", "26", "10"])






# pprint(bridges)



