#!/usr/bin/python
import sys
import argparse
import logging
from datetime import datetime, timedelta
import json
import itertools
import numpy as np
import urllib.request, json
import requests
import codecs

from lib import mlfb

def read_data(filename, data_type=None, delimiter=';', skip_cols=0, skip_rows=1, remove=None):
    """ 
    Read data from csv file
    """
    X = []
    with open(filename, encoding='utf-8') as f:
        lines = f.read().splitlines()

    for line in lines[skip_rows:]:
        l = line.split(delimiter)[skip_cols:]
        if remove is not None:
            nl = []
            for e in l:
                nl.append(e.replace(remove, ''))
            l = nl
        if data_type is not None:
            l = list(map(data_type, l))
        X.append(l)

    return X

def get_stations(filename=None):
    """
    Get railway stations from digitrafic
    """
    if filename is None:
        url = "https://rata.digitraffic.fi/api/v1/metadata/stations"

        with urllib.request.urlopen(url) as u:
            data = json.loads(u.read().decode("utf-8"))
    else:
        with open(filename) as f:
            data = json.load(f)

    stations = dict()
       
    for s in data:
        latlon = {'lat': s['latitude'], 'lon': s['longitude']}
        stations[s['stationShortCode'].encode('utf-8').decode()] = latlon

    return stations

def find_id(locations, name):
    """
    Find id from (id, name) tuple list
    """
    for loc in locations:
        if name == loc[1]:
            return loc[0]

    return None
    
def main():
    """
    Put labels from csv file to db
    """

    a = mlfb.mlfb()
    X = read_data(options.filename, delimiter=',', remove='"')

    if options.num_of_rows > 0:
        X = X[0:options.num_of_rows]
            
    stations = get_stations(filename='data/stations.json')

    locations = []
    names = []
    for name, latlon in stations.items():
        locations.append([name, latlon['lat'], latlon['lon']])        
        names.append(name)

    if options.add_locations:
        a.add_point_locations(locations, check_for_duplicates=False)

    ids = a.get_locations_by_name(names)    

    header = ['late_minutes', 'total_late_minutes']
    data = []
    metadata = []
    
    for row in X:
        timestr = row[0]+'T'+row[1]
        t = datetime.strptime(timestr, "%Y-%m-%dT%H") + timedelta(hours=1)
        loc = row[3]
        late_minutes = int(row[6])
        total_late_minutes = int(row[4])
        
        try:
            metadata.append([t, find_id(ids, loc)])
            data.append([late_minutes, total_late_minutes])
        except:
            logging.error('No location data for {}'.format(loc))
            continue

    a.add_rows('label', header, np.array(data), metadata, dataset=options.dataset)
    
if __name__=='__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--filename', type=str, default=None, help='Dataset path and filename')
    parser.add_argument('--add_locations',
                        action='store_true',
                        help='If set, locations are inserted into db, default=False')
    parser.add_argument('--dataset',
                        type=str,
                        default='trains',
                        help='dataset name')
    parser.add_argument('--num_of_rows',
                        type=int,
                        default=-1,
                        help='dataset name')    
    parser.add_argument('--logging_level',
                        type=str,
                        default='INFO',
                        help='options: DEBUG,INFO,WARNING,ERROR,CRITICAL')

    options = parser.parse_args()
    
    debug=False

    logging_level = {'DEBUG':logging.DEBUG,
                     'INFO':logging.INFO,
                     'WARNING':logging.WARNING,
                     'ERROR':logging.ERROR,
                     'CRITICAL':logging.CRITICAL}
    logging.basicConfig(format=("[%(levelname)s] %(asctime)s %(filename)s:%(funcName)s:%(lineno)s %(message)s"), level=logging_level[options.logging_level])

    main()
