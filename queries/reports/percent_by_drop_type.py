import sqlite3
conn = sqlite3.connect('dhl_data.db')
from pprint import pprint
from decimal import *
TWOPLACES = Decimal(10) ** -4
import csv


c = conn.cursor()

# Create table


drop_type = {"camera": {"incident_count": 0,
                        "duration_sum": 0},
             "bridge": {"incident_count": 0,
                        "duration_sum": 0},
             "switch": {"incident_count": 0,
                        "duration_sum": 0}}

bridges = dict()

days = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 23, 24, 25, 26, 27, 28, 29, 30, 31]

# days_2 = [1, 2, 3, 4, 5, 6, 7]

month = 11

for day in days:

    # gets canera_count
    c.execute('''SELECT bridges.name, cams.b_esn, count(c_esn)
                FROM cams
                INNER JOIN bridges
                ON cams.b_esn = bridges.b_esn
                WHERE cams.name NOT LIKE "Off %"
                GROUP BY cams.b_esn
                ORDER BY cams.b_esn;''')

    bridge_data = c.fetchall()
    for row in bridge_data:
        bridges[str(row[1])] = {"name": row[0],
                                "count": row[2],
                                "esn": row[1],
                                "total_secs": row[2] * 86400,
                                "after_loss": row[2] * 86400,
                                "bridge_loss": 0,
                                "bridge_loss_count": 0,
                                "bridge_loss_incidents": [],
                                "camera_loss": 0,
                                "camera_loss_count": 0,
                                "camera_loss_incidents": [],
                                "switch_loss_count": 0,
                                "switch_loss": 0,
                                "switch_loss_incidents": [],
                                "day": day,
                                "month": month,
                                "flagged": False}

    pprint(len(bridges.keys()))
    # Insert a row of data

    c.execute('''SELECT incident_guid, incidents.b_esn, bridges.name, duration_sum, drop_type
                FROM incidents
                INNER JOIN cams
                ON incidents.c_esn = cams.c_esn
                INNER JOIN bridges
                ON incidents.b_esn = bridges.b_esn
                WHERE CAST(strftime("%d",stop) as INTEGER) in (?) AND cams.name NOT LIKE "Off %" AND CAST(strftime("%m",stop) as INTEGER) = (?)
                GROUP BY  incident_guid, drop_type
                ORDER BY incident_guid;''', (day, month))

    with open("reports/dhl_incidents/bridge_loss_08-01_08-07.csv", "ab") as f:
            keys = ["esn", "name", "total_secs", "after_loss", "dif", "total_percent", "bridge_loss", "bridge_loss_count", "bridge_percent", "camera_loss", "camera_loss_count","camera_percent", "switch_loss", "switch_loss_count", "switch_percent", "month", "day"]
            writer = csv.DictWriter(f, fieldnames=keys, quotechar='"', quoting=csv.QUOTE_ALL, extrasaction="ignore")
            # writer.writeheader()
            all_rows = c.fetchall()
            print(len(all_rows))
            for row in all_rows:
                drop_type[row[-1]]["incident_count"] += 1
                drop_type[row[-1]]["duration_sum"] += row[-2]
            for row in all_rows:
                bridges[row[1]]["after_loss"] -= row[-2]
                if row[-1] == "camera":
                    bridges[row[1]]["camera_loss"] += row[-2]
                    bridges[row[1]]["camera_loss_count"] += 1
                    bridges[row[1]]["camera_loss_incidents"].append(row[0])
                elif row[-1] == "bridge":
                    bridges[row[1]]["bridge_loss"] += row[-2]
                    bridges[row[1]]["bridge_loss_count"] += 1
                    bridges[row[1]]["bridge_loss_incidents"].append(row[0])
                if row[-1] == "switch":
                    bridges[row[1]]["switch_loss"] += row[-2]
                    bridges[row[1]]["switch_loss_count"] += 1
                    bridges[row[1]]["switch_loss_incidents"].append(row[0])
            for bridge in bridges:

                bridges[bridge]["dif"] = bridges[bridge]["total_secs"] - bridges[bridge]["after_loss"]
                bridges[bridge]["total_percent"] = (str(100 * (Decimal(float((bridges[bridge]["after_loss"]) / float(bridges[bridge]["total_secs"]))).quantize(TWOPLACES))) + "%")
                bridges[bridge]["bridge_percent"] = (str(100 * (Decimal(float((bridges[bridge]["bridge_loss"]) / float(bridges[bridge]["dif"]))).quantize(TWOPLACES))) + "%") if bridges[bridge]["dif"] != 0 else "0%"
                bridges[bridge]["switch_percent"] = (str(100 * (Decimal(float((bridges[bridge]["switch_loss"]) / float(bridges[bridge]["dif"]))).quantize(TWOPLACES))) + "%") if bridges[bridge]["dif"] != 0 else "0%"
                bridges[bridge]["camera_percent"] = (str(100 * (Decimal(float((bridges[bridge]["camera_loss"]) / float(bridges[bridge]["dif"]))).quantize(TWOPLACES))) + "%") if bridges[bridge]["dif"] != 0 else "0%"
                writer.writerow(bridges[bridge])






        # for key in bridges.keys():
        #     if bridges[key]["flagged"] is not True:
        #         writer.writerow([bridges[key]["esn"], bridges[key]["name"], "0", "100.0000%", "03", "11"])

        # for row in all_rows:
        #     # print row
        #     data = Decimal(float((bridges[row[0]]["total_secs"] - row[2]) / float(bridges[row[0]]["total_secs"]))).quantize(TWOPLACES)
        #     string = str(100 * data) + "%"
        #     writer.writerow([row[0], row[1], row[2], string, row[3], row[4]])
        #     bridges[row[0]]["flagged"] = True

        # for key in bridges.keys():
        #     if bridges[key]["flagged"] is not True:
        #         writer.writerow([bridges[key]["esn"], bridges[key]["name"], "0", "100.0000%", "26", "10"])
