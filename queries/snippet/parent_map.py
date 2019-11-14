import sqlite3
conn = sqlite3.connect('dhl_data.db')
from pprint import pprint
from decimal import *
TWOPLACES = Decimal(10) ** -4
import csv


c = conn.cursor()

c.execute('''SELECT parent_cams.p_esn, parent_cams.name, parent_cams.b_esn, count(cams.c_esn)
            FROM cams
            INNER JOIN parent_cams
            ON cams.p_esn = parent_cams.p_esn
            WHERE cams.name NOT LIKE "Off %"
            GROUP BY parent_cams.p_esn;''')


p_cams = c.fetchall()

print(len(p_cams))


for row in p_cams:
    if row[0] != "":
        print row
        # p_esn = row[0].split("multiview_camera:", 1)[1].split(" ")[0]
        # c_esn = row[1]
        # c.execute(''' UPDATE cams
        #               SET p_esn = ?
        #               WHERE c_esn = ?''', (p_esn, c_esn))
        # conn.commit()