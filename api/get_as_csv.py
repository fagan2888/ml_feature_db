import sys
import argparse
import logging
import datetime as dt
import json
import itertools
import numpy as np

from lib import mlfb

def main():
    """
    Get data from db and save it as csv
    """
    
    logging.info('Loading classification dataset from db')
    # start_time = dt.datetime.strptime(options.start_time, "%Y%m%d%H%M")
    # end_time = dt.datetime.strptime(options.end_time, "%Y%m%d%H%M")

    a = mlfb.mlfb(1, logging_level=options.logging_level)
    metadata, header, data = a.get_rows(options.dataset)

    logging.debug('Length of metadata: {}'.format(len(metadata)))
    logging.debug('Shape of data {}'.format(data.shape))
    logging.debug('Header is: {}'.format(','.join(header)))

    # TODO saving as csv
    
    # Serialize model to disc
    # logging.info('Serializing dataset to disc: {}'.format(options.save_path))
    
    # csv = dataset.as_csv()
    # with open(options.save_path, "w") as f: 
    #    f.write(csv)


if __name__=='__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--start_time', type=str, help='Start time of the classification data interval')
    parser.add_argument('--end_time', type=str, help='End time of the classification data interval')
    parser.add_argument('--save_path', type=str, default=None, help='Dataset save path and filename')
    parser.add_argument('--dataset', type=str, default=None, help='Dataset name')    
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
