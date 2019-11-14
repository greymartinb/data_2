import sqlite3
conn = sqlite3.connect('dhl_data.db')
from pprint import pprint
from decimal import *
TWOPLACES = Decimal(10) ** -4
import csv
from datetime import datetime, timedelta

dt_format = "%Y-%m-%d %H:%M:%S"


c = conn.cursor()

# Create table

bridges = dict()


c.execute('''SELECT bridges.b_esn, bridges.name, count(c_esn)
             FROM cams
             INNER JOIN bridges
             ON cams.b_esn = bridges.b_esn
             WHERE cams.name NOT LIKE "Off %"
             GROUP BY bridges.b_esn;''')

# Insert a row of data
# c.execute("INSERT INTO )

all_rows = c.fetchall()
incidents = 0
for row in all_rows:
    bridges[row[0]] = {"name": row[1],
                       "esn": row[0],
                       "count": row[2],
                       "cams": []
                       }

# gets all_videogaps
c.execute('''SELECT bridges.b_esn, bridges.name, c_esn
             FROM cams
             INNER JOIN bridges
             ON cams.b_esn = bridges.b_esn
             WHERE cams.name NOT LIKE "Off %"
             GROUP BY c_esn;''')

# Insert a row of data
# c.execute("INSERT INTO )

all_rows = c.fetchall()
incidents = 0
print(len(all_rows))
for row in all_rows:
    bridges[row[0]]["cams"].append(row[2])


for key in bridges.keys():
    print(key)
    incident_time = None
    incident_array = []
    incident_array = [{"incident_cams": 1,
                       "rows": [],
                       "b_esn": key,
                       "incident_num": incidents,
                       "duration_sum": 0,
                       "start": "",
                       "stop": "",
                       "dif": 0,
                       "total_cams": bridges[key]["count"],
                       "guid": "",
                       "type": ""
                       }]

    c.execute('''SELECT *
                FROM video_gaps
                WHERE  b_esn = ? and CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER) > 10
                ORDER BY start ASC''', (key,))

    # Insert a row of data
    # c.execute("INSERT INTO )

    all_rows = c.fetchall()
    for idx, row in enumerate(all_rows):
        # print(row)
        t_id = row[0]
        b_esn = row[1]
        c_esn = row[2]
        start = datetime.strptime(row[3].split("+")[0], dt_format)
        stop = datetime.strptime(row[4].split("+")[0], dt_format)
        dif = stop - start
        # print("row dif {}".format(dif.total_seconds()))
        if incident_time is None:
            incident_time = start
            incident_array[-1]["start"] = start
            incident_array[-1]["guid"] = bridges[key]["esn"] + row[3].split("+")[0]

        inc_dif = start - incident_time
        # print("incident dif {}".format(inc_dif.total_seconds()))
        # print(c_esn)

        if inc_dif.total_seconds() > 60:
            incidents += 1
            incident_time = start
            incident_array.append({"incident_cams": 1,
                                   "rows": [],
                                   "b_esn": key,
                                   "incident_num": incidents,
                                   "duration_sum": dif.total_seconds(),
                                   "start": start,
                                   "stop": stop,
                                   "dif": 0,
                                   "total_cams": bridges[key]["count"],
                                   "guid": bridges[key]["esn"] + row[3].split("+")[0],
                                   "type": ""})
            incident_array[-1]["rows"].append(row)
        else:
            incident_array[-1]["incident_cams"] += 1
            incident_array[-1]["duration_sum"] += int(dif.total_seconds())
            incident_array[-1]["rows"].append(row)
            incident_array[-1]["stop"] = stop
    for i in incident_array:
        if i["total_cams"] - 5 <= i["incident_cams"]:
            i["type"] = "bridge"
        elif i["incident_cams"] > 1:
            i["type"] = "switch"
        else:
            i["type"] = "camera"
        dif = i["stop"] - i["start"]
        i["dif"] = dif.total_seconds()
        # print("++++++++++++++++++++")
        # pprint(i)
        # print("total_cams: {}".format(bridges[key]["count"]))
        # print("incident cams: {}".format(i["incident_cams"]))
        # print("incident_id: {}".format(i["incident_num"]))
        # print("incident_duration {}".format(i["total_duration"]))
        # print("incident type {}".format(i["type"]))
        # print("guid type {}".format(i["guid"]))
    # break

    # pprint(incident_array[-1])
    # incident_cams = [row[2] for row in incident_array[-1]["rows"]]
    # inc_besn = incident_array[-1]["b_esn"]
    # bridge_cams = bridges[inc_besn]["cams"]
    # missing = set(bridge_cams) - set(incident_cams)
    # print("incident cams {}".format(len(incident_cams)))
    # print("bridge cams {}".format(len(bridge_cams)))
    # print(missing)
    # pprint(incident_array[-1]["rows"])
    for entry in incident_array:
        for row in entry["rows"]:
                # print(row)
                try:
                    c.execute('''INSERT INTO incidents(b_esn, c_esn, video_gaps_id, start, stop,
                                             incident_cams, duration_sum, incident_start,
                                             incident_stop, incident_dif, total_bridge_cams,
                                             drop_type, incident_guid)
                                 VALUES (?, ?, ?, ?, ?,
                                        ?, ?, ?,
                                        ?, ?, ?,
                                        ?, ?)''', (str(entry["b_esn"]), str(row[2]), row[0], row[3], row[4],
                                                   entry["incident_cams"], entry["duration_sum"], entry["start"],
                                                   entry["stop"], entry["dif"], entry["total_cams"],
                                                   entry["type"], entry["guid"]))
                    conn.commit()
                except:
                    pprint(entry)
                    print(entry["rows"])
                    raise
