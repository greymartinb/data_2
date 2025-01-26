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

# pprint(bridges)

for key in bridges.keys():
    print("bridge esn : {}".format(key))
    # c.execute('''SELECT *
    #         FROM video_gaps
    #         WHERE  b_esn = ? and CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER) > 180
    #         AND CAST(strftime("%m",stop) as INTEGER) in (6) AND CAST(strftime("%Y",stop) as INTEGER) in (2022)
    #         ORDER BY start ASC''', (key,))
    c.execute('''SELECT *
            FROM video_gaps
            WHERE  b_esn = ?
            AND CAST(strftime("%m",stop) as INTEGER) in (12) AND CAST(strftime("%Y",stop) as INTEGER) in (2024)
            AND CAST(strftime("%Y",stop) as INTEGER) in (2024)
            ORDER BY start ASC''', (key,))


    ### SELECT * FROM video_gaps WHERE CAST(strftime("%m",stop) as INTEGER) in (1) AND CAST(strftime("%Y",stop) as INTEGER) in (2023)
    all_rows = c.fetchall()
    # if there are actually any gaps
    if len(all_rows) > 0:
        # setting incident time to none means that we have a fresh incident, first run on a new bridge
        # build the incident body
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
                           "unique_cams": 1,
                           "total_cams": bridges[key]["count"],
                           "guid": "",
                           "type": ""
                           }]

        print("gaps {}".format(len(all_rows)))
        # for every gap that occurs
        for idx, row in enumerate(all_rows):
            print("idx : {}".format(idx))
            # define the relevant values
            t_id = row[0]
            b_esn = row[1]
            c_esn = row[2]
            start = datetime.strptime(row[3].split("+")[0], dt_format)
            stop = datetime.strptime(row[4].split("+")[0], dt_format)
            dif = stop - start
            # print("row dif {}".format(dif.total_seconds()))
            print("camera esn {}".format(c_esn))

            # if its the first run
            if incident_time is None:
                # build the copy array that we will remove cameras from
                copy_b = bridges[key]["cams"].copy()
                # build other time keeper values
                incident_time = start
                incident_array[-1]["start"] = start
                # build the incident id
                incident_array[-1]["guid"] = bridges[key]["esn"] + row[3].split("+")[0]
                print("97 len : {}".format(len(copy_b)))
                print("bridge.cams {}".format(len(bridges[key]["cams"])))
                try:
                    copy_b.remove(c_esn)
                except:
                    continue
            inc_dif = start - incident_time
            print("incident dif {}".format(inc_dif.total_seconds()))
            # print("incident dif {}".format(inc_dif.total_seconds()))
            # print(c_esn)

            if inc_dif.total_seconds() > 500:
                print("new incident")
                copy_b = bridges[key]["cams"].copy()
                print("cam_len: {}".format(len(copy_b)))
                print("bridge.cams {}".format(len(bridges[key]["cams"])))
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
                                       "unique_cams": 1,
                                       "total_cams": bridges[key]["count"],
                                       "guid": bridges[key]["esn"] + row[3].split("+")[0],
                                       "type": ""})
                incident_array[-1]["rows"].append(row)
                # print(c_esn)
                # if inc_dif != 0:
                try:
                    print("122 len : {}".format(len(copy_b)))
                    copy_b.remove(c_esn)
                except Exception as e:
                    print(e)
            else:
                incident_array[-1]["incident_cams"] += 1
                incident_array[-1]["duration_sum"] += int(dif.total_seconds())
                incident_array[-1]["rows"].append(row)
                incident_array[-1]["stop"] = stop
                for cb in copy_b:
                    if cb == c_esn:
                        # incident_array[-1]["unique_cams"] += 1
                        print("matching {} on entry in list {}".format(c_esn, cb))
                        # print("matching {} on entry in list {}".format(type(c_esn), type(c)))
                        print("134 len : {}".format(len(copy_b)))
                        try:
                            copy_b.remove(c_esn)
                            incident_array[-1]["unique_cams"] += 1
                        except Exception as e:
                            print("remove cam exception {}".format(e))
        for idx, i in enumerate(incident_array):
            # print ("inc length {}".format(len(incident_array)))
            print(idx)
            try:
                print("total_cams - {} unique cams - {} incident_cams - {}".format(i["total_cams"], i["unique_cams"], i["incident_cams"] ))
                if i["total_cams"] == i["unique_cams"]:
                    i["type"] = "bridge"
                    print ("bridge")
                elif i["unique_cams"] > 1:
                    i["type"] = "switch"
                    print ("switch")
                else:
                    i["type"] = "camera"
                    print ("camera")
                dif = i["stop"] - i["start"]
                i["dif"] = dif.total_seconds()
                # if len(i["type"]) is not str:
                #     pprint(i)
                #     break
            except Exception as e:
                print (e)
                pprint(i)
            # print("++++++++++++++++++++")
            # pprint(i)
            # print("total_cams: {}".format(bridges[key]["count"]))
            # print("incident cams: {}".format(i["incident_cams"]))
            # print("incident_id: {}".format(i["incident_num"]))
            # print("incident_duration {}".format(i["total_duration"]))
            # print("incident type {}".format(i["type"]))
            # print("guid type {}".format(i["guid"]))
        # break


# search on 2019-12-15 13:36:43+00:00
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
                                                 incident_stop, incident_dif, total_bridge_cams, unique_cams,
                                                 drop_type, incident_guid)
                                     VALUES (?, ?, ?, ?, ?,
                                            ?, ?, ?,
                                            ?, ?, ?,
                                            ?, ?,?)''', (str(entry["b_esn"]), str(row[2]), row[0], row[3], row[4],
                                                         entry["incident_cams"], entry["duration_sum"], entry["start"],
                                                         entry["stop"], entry["dif"], entry["total_cams"], entry["unique_cams"],
                                                         entry["type"], entry["guid"]))
                        conn.commit()
                    except:
                        pprint(entry)
                        print(entry["rows"])
                        raise
    else:
        print("skipped {} gaps {}".format(key, len(all_rows)))
        print("error")
