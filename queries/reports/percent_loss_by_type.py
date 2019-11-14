import sqlite3
conn = sqlite3.connect('dhl_data.db')
from pprint import pprint
from decimal import *
TWOPLACES = Decimal(10) ** -4
import csv


c = conn.cursor()

cam = 0

bridge = 0

switch = 0


c.execute('''SELECT incidents.b_esn, bridges.name, drop_type, duration_sum
            FROM incidents
            INNER JOIN cams
            ON incidents.c_esn = cams.c_esn
            INNER JOIN bridges
            ON incidents.b_esn = bridges.b_esn
            WHERE CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER) > 10 AND CAST(strftime("%d",stop) as INTEGER) = 3 AND cams.name NOT LIKE "Off %" AND CAST(strftime("%m",stop) as INTEGER) = 08
            GROUP BY incidents.b_esn, drop_type
            ORDER BY incidents.b_esn;''')


all_rows = c.fetchall()
print(len(all_rows))
for row in all_rows:
    if row[2] == "camera":
        cam += row[3]
    elif row[2] == "bridge":
        bridge += row[3]
    elif row[2] == "switch":
        switch += row[3]

print("cams : {}".format(cam))
print("bridge : {}".format(bridge))
print("switch : {}".format(switch))
