#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import argparse
import logging
import datetime as dt
import json
import itertools
import numpy as np

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 
from api.lib import mlfb


def main():
    """
    Main python script for creating the classification model.
    """
    config = '{parentdir}/api/cnf/database.ini'.format(parentdir=parentdir)

    logging.info('Using configuration file: {}'.format(config))
    a = mlfb.mlfb(config_filename=config)

    # Drop old tables
    if options.force:
        sql = "DROP TABLE IF EXISTS {schema}.data, {schema}.location".format(schema=options.schema)
        logging.debug(sql)
        if not options.simulate:
            a.execute(sql)
    
    # Create data table
    sql = """
    CREATE TABLE {schema}.data
    (
      id integer,
      type character varying(254) COLLATE pg_catalog."default",
      source character varying(254) COLLATE pg_catalog."default",
      "time" timestamp without time zone[],
      location_id bigint,
      parameter character varying(254) COLLATE pg_catalog."default",
      value double precision,
      "row" character varying(254) COLLATE pg_catalog."default"
    )
    WITH (
      OIDS = FALSE
    )
    TABLESPACE pg_default;
    """.format(schema=options.schema)

    logging.debug(sql)
    if not options.simulate:
        a.execute(sql)
    
    # Create location table
    sql = """
    CREATE TABLE {schema}.location
    (
      id integer,
      name character varying(254) COLLATE pg_catalog."default",
      lat numeric,
      lon numeric,
      geom geometry
    )
    WITH (
      OIDS = FALSE
    )
    TABLESPACE pg_default;
    """.format(schema=options.schema)
    logging.debug(sql)

    if not options.simulate:
        a.execute(sql)

    #sql = "ALTER TABLE traindata_test.location OWNER to weatherproof_rw;"
    
if __name__=='__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--force',
                        action='store_true',
                        help='If set, existing tables are dropped, default=False')
    parser.add_argument('--simulate',
                        action='store_true',
                        help='Simulate only, default=False')        
    parser.add_argument('--schema',
                        type=str,
                        default='traindata',
                        help='Schema, default traindata')
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
