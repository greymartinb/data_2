import sqlite3
import csv




if __name__ == "__main__":
    conn = sqlite3.connect('dhl_data.db')
    c = conn.cursor()
    # Insert a row of data
    # c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")

    # Save (commit) the changes
    csv_rows = []
    with open("DHL Global Forwarding_bridges_10_01_23_.csv", "r") as f:
        reader = csv.reader(f, delimiter=',', quotechar='|')
        for row in reader:
            csv_rows.append(row)

    for idx, row in enumerate(csv_rows):
        # if idx != 0:
        print(row)
        c.execute("INSERT INTO bridges(een, name, b_esn, firmware, at) VALUES (?, ?, ?, ?, ?)", (row[0].replace('"', ''), row[1].replace('"', ''), row[2].replace('"', ''), row[3].replace('"', ''), row[4].replace('"', '')))
        conn.commit()

    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()