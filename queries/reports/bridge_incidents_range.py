import sqlite3
conn = sqlite3.connect('dhl_data.db')
from pprint import pprint
from decimal import *
TWOPLACES = Decimal(10) ** -4
import csv


c = conn.cursor()



c.execute('''SELECT incidents.b_esn, bridges.name, start, stop, incidents.duration_sum/incident_cams
            FROM incidents
            INNER JOIN cams
            ON incidents.c_esn = cams.c_esn
            INNER JOIN bridges
            ON incidents.b_esn = bridges.b_esn
            WHERE CAST(strftime("%m",stop) as INTEGER) = 10 AND incidents.duration_sum/incident_cams < 25 AND drop_type = "bridge" AND incidents.b_esn != "100a54e9" AND incidents.b_esn = 10012346
            GROUP BY incidents.b_esn, incident_guid;''')

all_rows = c.fetchall()

for row in all_rows:
    print row