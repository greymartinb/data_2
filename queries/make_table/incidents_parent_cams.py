import sqlite3
conn = sqlite3.connect('dhl_data.db')
from pprint import pprint
from decimal import *
TWOPLACES = Decimal(10) ** -4
import csv


parents = {}

c = conn.cursor()

c.execute('''SELECT parent_cams.p_esn, cams.c_esn
            FROM cams
            INNER JOIN parent_cams
            ON cams.p_esn = parent_cams.p_esn
            WHERE cams.name NOT LIKE "Off %"
            GROUP BY cams.c_esn;''')


p_cams = c.fetchall()

# print(len(p_cams))


for row in p_cams:
    p_esn = row[0]
    c_esn = row[1]
    try:
        parents[p_esn]["count"] += 1
        parents[p_esn]["rows"].append(c_esn)
    except Exception as e:
        parents[p_esn] = {"count": 1,
                          "rows": [c_esn],
                          }
        # print e

for entry in parents:
    # pprint(parents[entry])
    c.execute('''SELECT incident_guid
                FROM incidents
                WHERE incident_cams = ? and c_esn =  ?
                GROUP BY incident_guid;''', (parents[entry]["count"], parents[entry]["rows"][0]))

    guids = c.fetchall()
    print("*****************")
    print(len(guids))
    for thing in guids:
        counter = parents[entry]["count"]
        c.execute('''SELECT c_esn
            FROM incidents
            WHERE incident_guid = ?
            GROUP BY c_esn;''', (thing[0],))
        res = c.fetchall()
        if len(res) != 0:
            for x in res:
                esn = x[0]
                if esn in parents[entry]["rows"]:
                    counter -= 1
                    if counter == 1:
                        pprint(parents[entry])
                        print(thing)
                        pprint(res)
