import sqlite3
import csv




if __name__ == "__main__":
    conn = sqlite3.connect('dhl_data.db')
    c = conn.cursor()
    # Insert a row of data
    # c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")

    # Save (commit) the changes
    csv_rows = []
    with open("data/camera_to_switch.csv", "rb") as f:
        reader = csv.reader(f, delimiter=',', quotechar='|')
        for row in reader:
            csv_rows.append(row)

    for idx, row in enumerate(csv_rows):
        if idx != 0:
            print(row)
            c.execute("UPDATE cams set switch_guid = (?) WHERE c_esn = (?)", (row[0], row[1]))
            conn.commit()

    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()