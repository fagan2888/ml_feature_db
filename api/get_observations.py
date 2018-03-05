#!/usr/bin/python
import sys
import argparse
import logging
from datetime import datetime, timedelta
import time
import json
import itertools
import numpy as np
import urllib.request, json
import requests
import codecs

from lib import mlfb

    
def main():
    """
    Get observations near locations from SmartMet Server
    
    Data start and end time and timestep is fetched from the
    data. Dataset is assumed coherent in means of time and
    locations. I.e. timestep is assumed to be constant between start
    and end time. 
    """
    a = mlfb.mlfb()

    params = ['time', 'place', 'temperature', 'windspeedms', 'winddirection', 'precipitation1h']
    
    # Get locations and create coordinate list for SmartMet
    locations = a.get_locations_by_dataset(options.dataset_name)
    latlons = []
    ids = dict()
    for loc in locations[0:2]:
        latlon = str(loc[3])+','+str(loc[2])
        latlons.append(latlon)
        ids[latlon] = loc[0]
            
    # Create url and get data
    url = 'http://smartmet.fmi.fi/timeseries?format=json&producer={producer}&timeformat=epoch&latlons={latlons}&timestep={timestep}&starttime={starttime}&endtime={endtime}&param={params}'.format(latlons=','.join(latlons), timestep=options.time_step, params=','.join(params), starttime=options.start_time, endtime=options.end_time, producer=options.producer)

    with urllib.request.urlopen(url) as u:
        data = json.loads(u.read().decode("utf-8"))
        
    # Parse data to numpy array
    result = []
    for el in data:
        row = []
        for param in params:
            if el[param] is None:
                row.append(-99)
            else:
                row.append(el[param])
        result.append(row)
    result = np.array(result)

    # Result by time
    result = result[result[:,0].argsort()]
    print(np.nan_to_num(result))

    # Coordinates back to location ids
    metadata = []
    for row in result[:,0:2]:
        metadata.append([int(row[0]), ids[row[1]]])
        
    # Save to database        
    data = result[:,2:]
    header = params[2:]

    a.add_rows('feature', header, data, metadata, 'tehanu-1-2')
    #print(header)
    #print(data)
    #print(metadata)
    
if __name__=='__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_name',
                        type=str,
                        default=None,
                        help='Name of dataset bind to locations')
    parser.add_argument('--distance',
                        type=str,
                        default=50,
                        help='How far from station locations are searched for')
    parser.add_argument('--start_time',
                        type=str,
                        default=None,
                        help='Start time')
    parser.add_argument('--end_time',
                        type=str,
                        default=None,
                        help='End time')
    parser.add_argument('--time_step',
                        type=str,
                        default=10,
                        help='Timestep of observations in minutes')
    parser.add_argument('--producer',
                        type=str,
                        default='opendata',
                        help='Data producer')
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
