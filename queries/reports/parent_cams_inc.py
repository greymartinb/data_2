import sqlite3
conn = sqlite3.connect('dhl_data.db')
from pprint import pprint
from decimal import *
TWOPLACES = Decimal(10) ** -4
import csv


parents = {}

c = conn.cursor()

month = 10

# example c_esn of non-existent p_esn

c.execute('''SELECT p_esn
            FROM cams
            WHERE c_esn = "10031834";''')

non_p = c.fetchone()
print non_p


c.execute('''SELECT c_esn
            FROM cams
            WHERE p_esn !=  ?
            GROUP BY c_esn''', non_p)


p_cams = c.fetchall()

print(len(p_cams))


# c.execute('''SELECT video_gaps_id
#             FROM incidents
#             INNER JOIN cams
#             ON incidents.c_esn  = cams.c_esn
#             WHERE cams.p_esn != (?) and CAST(strftime("%m",stop) as INTEGER) = 10 AND drop_type != "bridge"
#             GROUP BY video_gaps_id''', non_p)


# vid = c.fetchall()
# print(len(vid))
# with open("reports/parent_cams_loss_by_gap.csv", "ab") as f:
#     writer = csv.writer(f, f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
#     header = ["vg_id","b_esn", "c_esn", "c_name", "p_esn", "c_model", "c_make", "total_loss"]
#     writer.writerow(header)
#     for r in vid:
#         c.execute('''SELECT video_gaps_id, cams.b_esn, cams.c_esn, cams.name, cams.p_esn, cams.model, cams.make,CAST(strftime("%s",stop) as INTEGER) - CAST(strftime("%s",start) as INTEGER)
#                     FROM video_gaps
#                     INNER JOIN cams
#                     ON video_gaps.c_esn  = cams.c_esn
#                     WHERE video_gaps_id = (?) and CAST(strftime("%m",stop) as INTEGER) = 10
#                     GROUP BY video_gaps_id''', r)
#         i = c.fetchall()
#         writer.writerow(i)





# with open("reports/parent_cams_loss.csv", "ab") as f:
#     writer = csv.writer(f, f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
#     header = ["b_esn", "c_esn", "c_name", "p_esn", "c_model", "c_make", "total_loss"]
#     writer.writerow(header)
#     for row in p_sums:
#         writer.writerow(row)
