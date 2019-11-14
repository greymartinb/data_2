import sqlite3
conn = sqlite3.connect('dhl_data.db')
from pprint import pprint
import os
import glob
import csv
import xlwt
from datetime import datetime, timedelta

now = datetime.now()

title_format = "%Y-%m-%d"

now_string = datetime.strftime(now, title_format)

c = conn.cursor()




c.execute('''SELECT CAST(strftime("%d",stop) as INTEGER), CAST(strftime("%m",stop) as INTEGER)
             FROM Incidents
             GROUP BY CAST(strftime("%m",stop) as INTEGER), CAST(strftime("%d",stop) as INTEGER)
             ORDER BY CAST(strftime("%m",stop) as INTEGER), CAST(strftime("%d",stop) as INTEGER)''')

all_rows = c.fetchall()
for row in all_rows:
    print row
