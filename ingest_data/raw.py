import gevent
from gevent import monkey
from gevent.pool import Pool
monkey.patch_all()

import sqlite3
conn = sqlite3.connect('dhl_data.db')


import logging
import requests

from requests.adapters import HTTPAdapter, Retry

logging.basicConfig(level=logging.DEBUG)

s = requests.Session()
retries = Retry(total=3, backoff_factor=1)
s.mount('http://', HTTPAdapter(max_retries=retries))

import urllib3

http = urllib3.PoolManager(num_pools=90)


import os
import csv
os.environ['MONKEY'] = "True"

# # This is so Django knows where to find stuff.

import requests
import simplejson
from pprint import pprint
from datetime import datetime, timedelta
# import logging

d_USERNAME = "dhlvsaashd@tienational.com"
d_PASSWORD = "BT4LijZEr[PyXoYLHrxpgMyg22"
# pin 043446

t_user = "greymartinb@gmail.com"
t_password = "BT4LijZEr[PyXoYLHrxpgMyg12"


USERNAME = "lhoffmeister+PriorityWire@een.com"
PASSWORD = "brtBrzT4!bkp5K7"

KENDRA_U = "hegloff+test12@een.com"
KENDRA_P = "1234567890"

prodlist = []

results = []
done = False
event = gevent.event.Event()

THREADS = 80

RUN_FORMAT = '%Y%m%d%H0000.000'

EENFORMAT = '%Y%m%d%H%M%S.%f'

prop_format = "%Y_%m_%d_%H:%M:%S"


TITLE_FORMAT = "%m_%d"

excel_format = "%m/%d %H:%M:%S"

dt_month = "%m"

dt_day = "%d"

dt_hour = "%H"

dt_minute = "%M"


conn = sqlite3.connect('dhl_data.db')
c = conn.cursor()


def authenticate(username, password):
    url = "https://c012.eagleeyenetworks.com/g/aaa/authenticate"
    login = {"username": username, "password": password}
    resp = requests.post(url, data=login)
    print("authen response {}".format(resp.status_code))
    token = simplejson.loads(resp.content)
    return token


def authorize(token):
    url = "https://c012.eagleeyenetworks.com/g/aaa/authorize"
    resp = requests.post(url, data=token)
    print("authorize response {}".format(resp.status_code))
    cookie = resp.cookies
    return cookie


def list_video(camera, start, end):
    try:
        # print("hit ***********")
        # esn = str(camera.esn)
        url = "https://c012.eagleeyenetworks.com/asset/list/image"
        data = {"id": camera,
                "start_timestamp": start,
                "end_timestamp": end,
                "asset_class": "pre"}
        # pprint(data)
        # data = simplejson.dumps(data)
        # print("requested")
        resp = s.get(url, params=data, cookies=cookie)
        # print("returned", resp)
        list_video_data = simplejson.loads(resp.content)
        if resp.status_code == 200:
            # list_video_data = simplejson.loads(resp.content)
            # pprint(list_video_data)
            print("list_video success")
            return list_video_data, start, end
            # pprint(list_video_data)
        else:
            print("list_video fail")
            print(resp.status_code)
            # print(list_video_data)
            print(camera, start, end)
            # quit()
            write_skip(camera, start, end)
            return False, start, end
    except Exception as e:
        print("list video exception")
        print(e)
        print(resp)
        write_skip(camera, start, end)
        return False, start, end


def write_skip(camera, start, end):
    with open('skipped calls.csv', 'ab') as csvfile:
        csv_writer = csv.writer(csvfile, csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        # try:
        row = [camera, start, end]
        print("writing skip")
        print(row)
        csv_writer.writerow(row)
        # except Exception as e:
        #     print("write skip exeception")
        #     print(e)


# def parse_preview_list(camera, data):
#     gaps = []
#     for idx, entry in enumerate(data):
#         # print("idx : {} of len {}".format(idx, len(data)))
#         if  len(data) > 2100:
#             continue

#         else:
#             if idx != len(data) - 1:
#                 now = datetime.strptime(data[idx]["s"], EENFORMAT)
#                 later = datetime.strptime(data[idx + 1]["s"], EENFORMAT)
#                 dif = later - now
#                 # print(dif.total_seconds(), data[idx]["t"])
#                 if dif.total_seconds() > 180:
#                     gaps.append({"esn": camera["esn"],
#                                  "gap_start": now,
#                                  "gap_end": later,
#                                  'history_link': "https://login.eagleeyenetworks.com/#/camera_history/{}/{}".format(camera["esn"], data[idx]["s"]),
#                                  "gap_seconds": dif.total_seconds()})
#     return gaps


def date_range(start, end):
    ranges = []
    s_dt = datetime.strptime(start, EENFORMAT)
    e_dt = datetime.strptime(end, EENFORMAT)
    runner = 0
    inc = timedelta(hours=6)
    while s_dt < e_dt:
        runner = s_dt + inc
        s_txt = datetime.strftime(s_dt, RUN_FORMAT)
        r_text = datetime.strftime(runner, RUN_FORMAT)
        ranges.append([s_txt, r_text])
        s_dt = runner
    return ranges


def check_data_len(data, start, end):
    if len(data) > 21300:
        print("data is long enough")
        print(len(data))
        data = [{'s': start}, {'s': end}]
        return data
    else:
        print("data is to short")
        print(len(data))
        return data


def producer(bridge, camera, s, e):
    try:
        # print("producer")
        # print(bridge, camera, s, e)
        data, start, end = list_video(camera, s, e)
        data = check_data_len(data, start, end)
        if data is False:
            return False, False
        else:
            return bridge, camera, data
    except:
        return False, False


def writer():
    # print("here")
    event.wait()
    while not done and results:
        # print("writer")
        res = gevent.wait(results, timeout=10)
        for r in res:
            # print("r value")
            # print(type(r.value))
            # print(r.value)
            # print(r.value)
            if r.value[0]:
                # print(r.value)
                write_row(r.value)
                results.remove(r)
                print("written")
                print(r.value[0])
            else:
                results.remove(r)
                print("empty")


def write_row(r):
    bridge = r[0]
    cam = r[1]
    data = r[2]
    try:
        if len(data) > 10:
            restructure = []
            for entry in data:
                row = [bridge, cam, entry['s'], False]
                restructure.append(row)
            c.executemany("INSERT INTO raw( b_esn, c_esn, ts, implied) VALUES (?, ?, ?, ?)", (restructure))
            conn.commit()
        else:
            restructure = []
            for entry in data:
                row = [bridge, cam, entry['s'], True]
                restructure.append(row)
            c.executemany("INSERT INTO raw( b_esn, c_esn, ts, implied) VALUES (?, ?, ?, ?)", (restructure))
            conn.commit()

    except Exception as e:
        pprint("write_error")
        print(e)
        pprint(len(data))
        pprint(bridge)
        pprint(cam)


def go(bridge):
    print("go")
    gevent.spawn(writer)
    # pool = gevent.pool.Pool(THREADS)
    # pool = gevent.pool.Pool(THREADS)
    pool = Pool(THREADS)
    ranges = date_range(start, end)
    for camera in bridge["cameras"]:
        print(bridge["cameras"][camera]["esn"], bridge["cameras"][camera]["name"])
        c = bridge["cameras"][camera]
        for item in ranges:
            # pprint(bridge)
            ar = gevent.event.AsyncResult()
            pool.spawn(producer, bridge['esn'], c["esn"], item[0], item[1]).link(ar)
            results.append(ar)
            event.set()


def parse_cams(bridges, cams):
    for entry in cams:
        c_esn = entry[1]
        c_name = entry[2]
        try:
            b_esn = entry[4][0][0]
            bridges[b_esn]["cameras"][c_esn] = {"name": c_name,
                                                "esn": c_esn}
        except:
            print("unattached cams")
            print(entry[4])
            print(c_esn)
            print(c_name)
    return bridges


def parse_bridges(data):
    bridges = {}
    for b in data:
        # if b[1] in ["1006cded", "1006f448"]:
        bridges[b[1]] = {"name": b[2],
                         "esn": b[1],
                         "cameras": {}}
    return bridges


def get_bridges():
    url = "https://c012.eagleeyenetworks.com/g/device/list?t=bridge"
    resp = requests.get(url, cookies=cookie)
    if resp.status_code == 200:
        data = simplejson.loads(resp.content)
        return data
    else:
        print(resp.status_code)
        print(resp.content)


# def make_work_list(cam_map, ranges):
#     cam_list = []
#     for entry in cam_map:
#         for item in ranges:
#             cam_list.append([entry, item[0], item[1]])
#     return cam_list


def get_list_cameras():
    url = "https://c012.eagleeyenetworks.com/g/device/list?t=camera"
    resp = requests.get(url, cookies=cookie)
    if resp.status_code == 200:
        data = simplejson.loads(resp.content)
        return data
    else:
        print(resp.status_code)
        print(resp.content)


if __name__ == "__main__":

    start = "20241101000000.000"
    end =   "20251201000000.000"
    
    # start = "20231205000000.000"
    # end =   "20231215000000.000"

    # start = "20231215000000.000"
    # end =   "20231225000000.000"

    # start = "20231225000000.000"
    # end =   "20240101000000.000"

    # token = authenticate(d_USERNAME, d_PASSWORD)
    # cookie = authorize(token)
    cook = "c001~5c55f0b77ae2490ab44ee1db50916b7b"
    cookie = {"videobank_sessionid": cook,
              "auth_key": cook}
    # cookie = 
    
    bridges = get_bridges()
    b_data = parse_bridges(bridges)
    cams = get_list_cameras()
    cam_map = parse_cams(b_data, cams)

    time_start = datetime.now()


    
    # bridges =  ['10007c0c', '1000e712']

    # bridges = ['1000fb41',
    #             '1000fede'] # 

    bridges = ['100112ce',
                '10015d48'] # 
                
                # '1001eb37',
                # '100248df',
                # '1002ea86',
                # '100317d3',
                # '100354fd',
                # '1004bbe9',
                # '1004c0ff',
                # '10061de1',
                # '10083044',
                # '1009004c',
                # '100a86e8',
                # '100b317d',
                # '100c3985',
                # '100eed3d',
                # '10105ff7'] 

    
    

    for bridge in bridges:
        go(cam_map[bridge])
        hub = gevent.get_hub()
        while not hub.join(timeout=50):
            pass
        time_end = datetime.now()
        time_dif = time_end - time_start
        pprint('it took {} mins'.format(time_dif.total_seconds()/60))


