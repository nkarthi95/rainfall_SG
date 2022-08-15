import json
from os import stat
import numpy as np
import pandas as pd
from datetime import date, timedelta
import time
import urllib.request as ur
import os
# import urllib3

"""
TODO
1) Add in a way to check for whether the data being accessed is blank. 
"""


# http = urllib3.PoolManager()
url = "https://api.data.gov.sg/v1/environment/rainfall?date={0}"

# def parse_api(url, year):
#     thing =  url.format(str(year))
#     resp = http.request("GET", thing)
#     data_obj = json.loads(resp.data)
#     return data_obj

def parse_api(url, year):
    url =  url.format(str(year))
    fileobj = ur.urlopen(url)
    readobj = fileobj.read()
    data_obj = json.loads(readobj)
    return data_obj

def get_dates(sdate, edate):
    delta = edate - sdate       # as timedelta
    dates = []

    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        dates.append(day)

    dates = np.array(dates, dtype = object)
    return dates

def get_stations(data_obj):
    stations = data_obj["metadata"]["stations"]
    df = pd.DataFrame.from_dict(stations)
    return df 

def get_data_day(data_obj, stations):
    keys = np.array(stations['id'])
    precipitation_raw = data_obj["items"]
    n = len(precipitation_raw)
    t = np.zeros(n, dtype = object)
    df = pd.DataFrame(np.zeros((n, keys.size)), columns= keys)

    for i, d_obj in enumerate(precipitation_raw):
        t[i] = d_obj['timestamp'].split('+')[0].split('T')[-1]
        list_timestep = d_obj['readings']
        for j in range(len(list_timestep)):
            dict_key = list_timestep[j]['station_id']
            value = list_timestep[j]['value']
            df[dict_key].iloc[i] += value
    
    df['Time'] = t
    df = df.set_index('Time')
    return df

def concat_stations(station_ids):
    n = station_ids.size
    if n < 1:
        return None
    elif n == 1:
        print(True)
        df = station_ids[0]
    else:
        for i in range(0, n-1):
            df = pd.concat([station_ids[i], station_ids[i+1]], ignore_index=True)

    df.drop_duplicates(subset = 'id', ignore_index = True, inplace = True)
    return df

def collate_data(all_stations, sum_preci, dates):
    row = dates.size
    column_id = np.array(all_stations['id'])

    df = pd.DataFrame(np.zeros((row, column_id.size)), columns = column_id)

    for i, series in enumerate(sum_preci):
        keys = np.array(series.index)
        df[keys].iloc[i] = sum_preci[i]

    df['Date'] = dates
    df = df.set_index('Date')
    return df 

def main():
    sdate = date(2021, 1, 1)   # start date
    edate = date.today()   # end date

    dates = get_dates(sdate, edate)
    # dates = dates[0:dates.size:10]'
    # dates = dates[0:5]
    n = dates.size
    station_ids = np.zeros(n, dtype = object)
    sum_precipation_datas = np.zeros(n, dtype = object)

    # print(dates)

    for i, d in enumerate(dates):
        data_obj = parse_api(url, d)
        print(d)
        if len(data_obj['metadata']['stations']) == 0:
            dates[i] = 0
            continue
        else:
            stations = get_stations(data_obj)
            precipitation = get_data_day(data_obj, stations)
            station_ids[i] = stations
            sum_precipation_datas[i] = precipitation.sum()

    dates = dates[np.where(dates != 0)]
    station_ids = station_ids[np.where(station_ids != 0)]
    sum_precipation_datas = sum_precipation_datas[np.where(sum_precipation_datas != 0)]

    all_stations = concat_stations(station_ids)
    df = collate_data(all_stations, sum_precipation_datas, dates)
    print(df)

    pwd = os.getcwd()
    os.chdir(r'C:\Users\nikhi\Documents\GitHub\Rainfall_analysis')
    df.to_csv('Daily_rainfall.csv')
    all_stations.to_csv('station_stats.csv')
    os.chdir(pwd)

    # data_obj = parse_api(url, dates[0])
    # stations = get_stations(data_obj)
    # precipitation = get_data_day(data_obj, stations)
    # print(precipitation.sum())

main()

# ran = np.random.randn(20)
# print(ran)

# print(ran[0:ran.size:ran.size//5])

# b"User-agent: *\nDisallow: /deny\n"

"""
URL Reader
https://urllib3.readthedocs.io/en/latest/user-guide.html

Uses the urllib 3 library to open the json file

Getting the range of dates between a specified start and end date
https://www.codegrepper.com/code-examples/python/get+all+dates+between+two+dates+python
""" 