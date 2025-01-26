import gevent
from gevent import monkey
from gevent.pool import Pool
monkey.patch_all()


import sqlite3
conn = sqlite3.connect('dhl_data.db')


import os
os.environ['MONKEY'] = "True"

from pprint import pprint
from datetime import datetime

import re
from pprint import pprint

dt_format = "%Y%m%d%H%M%S"
c = conn.cursor()

results = []
done = False
event = gevent.event.Event()

THREADS = 50


conn = sqlite3.connect('dhl_data.db')
c = conn.cursor()


def sorter_thing(e):
    return e[3]


def writer():
    event.wait()
    while not done and results:
        # print("writer")
        res = gevent.wait(results, timeout=100)
        for r in res:
            try:
                for thing in r.value:
                        print("writing")
                        # print(b_esn, thing[0], thing[1], thing[2])
                        c.execute("INSERT INTO video_gaps(b_esn, c_esn, start, stop) VALUES (?, ?, ?, ?)", (b_esn, thing[0], thing[1], thing[2]))
                        conn.commit()
                results.remove(r)
            except Exception as e:
                print(e)
                print(r.value)
                results.remove(r)


def producer(ts_data):
    ts_data.sort(key=sorter_thing)
    gaps = []
    print('producer')
    # print(ts_data[1][2])
    # if len(ts_data) == 72:
    #     return []
    for idx, entry in enumerate(ts_data):
        if idx != len(ts_data) - 1:
            # print("idx : {} of len {}".format(idx, len(data)))
            if ts_data[idx][4] and ts_data[idx + 1][4]:
                continue
            else:
                    now_text = str(ts_data[idx][3]).split(".")[0]
                    later_text = str(ts_data[idx + 1][3]).split(".")[0]
                    if int(now_text[-2:]) > 59:
                        now_text = re.sub("60$", "59", now_text)
                    if int(later_text[-2:]) > 59:
                        later_text = re.sub("60$", "59", later_text)
                    now = datetime.strptime(now_text, dt_format)
                    later = datetime.strptime(later_text, dt_format)
                    dif = later - now
                    # print(dif.total_seconds(), data[idx]["t"])
                    if dif.total_seconds() > 300:
                        gaps.append([ts_data[idx][2], now, later])
                        print([ts_data[idx][2], now, later])
    return gaps


def fetch_bridge_cams(b_esn):
    c.execute('''SELECT c_esn
            FROM cams
            WHERE b_esn = ?
            GROUP BY c_esn
            ORDER BY c_esn DESC;''', (b_esn,))
    cam_data = c.fetchall()
    return cam_data


# def fetch_cam_raw(b_esn, c_esn):
#     c.execute('''SELECT * FROM raw WHERE b_esn = ? AND c_esn = ? ''', (b_esn, c_esn[0],))
#     ts_data = c.fetchall()
#     return ts_data


def fetch_cam_raw(b_esn, c_esn):
    c.execute('''SELECT * FROM raw WHERE b_esn = ? AND c_esn = ? ''', (b_esn, c_esn[0],))
    ts_data = c.fetchall()
    return ts_data


def go_2(b_esn):
    print("go")
    gevent.spawn(writer)
    # monkey.patch_all()
    pool = Pool(THREADS)
    b_cams = fetch_bridge_cams(b_esn)
    for cam in b_cams:
        print(cam)
        ts_data = fetch_cam_raw(b_esn, cam)
        print(len(ts_data))
        ar = gevent.event.AsyncResult()
        pool.spawn(producer, ts_data).link(ar)
        results.append(ar)
        event.set()


if __name__ == "__main__":
   
    bridges = ['1000fb41',
                '1000fede'] # 

    for b_esn in bridges:
        time_start = datetime.now()
        go_2(b_esn)
        hub = gevent.get_hub()
        while not hub.join(timeout=50):
            pass
        time_end = datetime.now()
        time_dif = time_end - time_start
        pprint('it took {} mins'.format(time_dif.total_seconds()/60))
    conn.close()