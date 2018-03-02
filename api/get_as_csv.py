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
    Main python script for creating the classification model.
    """
    
    # Load the data and split it to training and testing datasets
    logging.debug('Loading classification dataset from db')
    # start_time = dt.datetime.strptime(options.start_time, "%Y%m%d%H%M")
    # end_time = dt.datetime.strptime(options.end_time, "%Y%m%d%H%M")

    a = mlfb.mlfb(1, logging_level=options.logging_level)
    a.get_rows()

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
