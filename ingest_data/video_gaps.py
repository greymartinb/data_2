import sqlite3
import csv
from datetime import datetime, timedelta
import pytz
import re
from pprint import pprint


dt_format = "%Y%m%d%H%M%S"
utc = pytz.utc


if __name__ == "__main__":
    conn = sqlite3.connect('dhl_data.db')
    c = conn.cursor()
    # Insert a row of data
    # c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
    #        utc = pytz.utc
    #        at = utc.localize(datetime.now())


    # Save (commit) the changes
    csv_rows = []
    with open("data/DHL_ALL_11-07_11-13.csv", "rb") as f:
        reader = csv.reader(f, delimiter=',', quotechar='|')
        for row in reader:
            # print(" ************ new row ************")
            if row[0] != '"bridge_esn"':
                b_esn = row[0].replace('"', "")
                c_esn = row[2].replace('"', "")
                start_text = row[9].split(".")[0].replace('"', "")
                # print start_text
                end_text = row[14].split(".")[0].replace('"', "")
                # print end_text
                if int(start_text[-2:]) > 59:
                    # print start_text[-2:]
                    start_text = re.sub("60$", "59", start_text)
                    # print start_text[-2:]
                if int(end_text[-2:]) > 59:
                    # print end_text[-2:]
                    end_text = re.sub("60$", "59", end_text)
                    # print end_text[-2:]
                try:
                    start_time = utc.localize(datetime.strptime(start_text, dt_format))
                    end_time = utc.localize(datetime.strptime(end_text, dt_format))
                    entry = [b_esn, c_esn, start_time, end_time]
                    # pprint(entry)
                    csv_rows.append(entry)

                except Exception as e:
                    print e
                    pprint(entry)
                    raise
                    print("++++++++++++")
                    # print end_text
                    # print start_text

    # for idx, row in enumerate(csv_rows):
    #     print(row)
    #     if idx == 10:
    #         break

    for idx, row in enumerate(csv_rows):
        if idx != 0:
            # print(row)
            c.execute("INSERT INTO video_gaps(b_esn, c_esn, start, stop) VALUES (?, ?, ?, ?)", (row[0], row[1], row[2], row[3]))
            conn.commit()

    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()