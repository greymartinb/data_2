import sqlite3
conn = sqlite3.connect('dhl_data.db')
from pprint import pprint
from decimal import *
TWOPLACES = Decimal(10) ** -4
import csv


parents = {}

c = conn.cursor()

# example c_esn of non-existent p_esn

c.execute('''SELECT p_esn
            FROM cams
            WHERE c_esn = "10031834";''')

non_p = c.fetchone()
print non_p


c.execute('''SELECT bridges.b_esn, bridges.name, c_esn, p_esn, cams.name,  cams.make, cams.model
            FROM cams
            INNER JOIN bridges
            ON cams.b_esn =  bridges.b_esn
            WHERE p_esn !=  ?
            GROUP BY c_esn''', non_p)


p_cams = c.fetchall()

with open("reports/parent_cams.csv", "ab") as f:
    writer = csv.writer(f, f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
    header = ["b_esn", "b_name", "c_esn","c_name", "make", "model"]
    writer.writerow(header)
    for row in p_cams:
        writer.writerow(row)
