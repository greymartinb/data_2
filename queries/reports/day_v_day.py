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

# Insert a row of data
# c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
all_rows = c.fetchall()
for row in all_rows:
    bridges[row[1]] = {"cam_count": row[2],
                        "name": row[0],
                        "esn": row[1],
                       # "expected_secs": row[1]*24*60*60,
                       "before_loss": 1,
                       "change": 0,
                       "after_loss": 1,
                       "category": ""}

# pprint(bridges)

c.execute('''SELECT b_esn, count(c_esn)
            FROM cams
            WHERE name NOT LIKE "Off %"
            GROUP BY b_esn
            ORDER BY b_esn;''')

# Insert a row of data
# c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
all_rows = c.fetchall()
total = 0
for row in all_rows:
    total += row[1]
print total


c.execute('''SELECT b_esn, SUM(CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER))
            FROM video_gaps
            WHERE CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER) > 10 AND CAST(strftime("%d",stop) as INTEGER) = 13
            GROUP BY b_esn
            ORDER BY b_esn;''')

all_rows = c.fetchall()
for row in all_rows:
    bridges[row[0]]["before_loss"] = row[1]


c.execute('''SELECT b_esn, SUM(CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER))
            FROM video_gaps
            WHERE CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER) > 10 AND CAST(strftime("%d",stop) as INTEGER) = 16
            GROUP BY b_esn
            ORDER BY b_esn;''')

all_rows = c.fetchall()
for row in all_rows:
    bridges[row[0]]["after_loss"] = row[1]


worse = 0
better = 0
unknown = 0
for key in bridges.keys():
    if bridges[key]["before_loss"] != 0 and bridges[key]["after_loss"] != 0:
        bridges[key]["change"] = bridges[key]["before_loss"] - bridges[key]["after_loss"]
        data = Decimal(float(bridges[key]["change"] / float(bridges[key]["before_loss"] + 1))).quantize(TWOPLACES)
        # print("+++++++++++++")
        # print(data)
        string = str(100 * data) + "%"
        bridges[key]["per_change"] = string
        if data > 0:
            # print(string)
            # pprint(bridges[key])
            bridges[key]["category"] = "better"
            better += 1
        else:
            bridges[key]["category"] = "worse"
            worse += 1
    else:
        bridges[key]["category"] = "insufficient data"
        unknown += 1

with open("reports/final/bridge_loss_10-13v10-16.csv", "wb") as f:
        keys = ["name", "esn", "cam_count", "before_loss", "after_loss", "change", "per_change", "category"]
        writer = csv.DictWriter(f, fieldnames=keys, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for item in bridges:
            try:
                writer.writerow(bridges[item])
            except:
                print(bridges[item])
                raise



