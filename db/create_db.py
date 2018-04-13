#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import argparse
import logging
import datetime as dt
import json
import itertools
import numpy as np
from configparser import ConfigParser

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

    # Create schema
    parser = ConfigParser()
    parser.read(config)
    sql = "CREATE SCHEMA IF NOT EXISTS  {} AUTHORIZATION {}".format(options.schema, parser.items('postgresql')[2][1])
    logging.debug(sql)
    if not options.simulate:
        a.execute(sql)
    
    # Enable PostGIS
    if options.create_extension:
        sql = "CREATE EXTENSION postgis"
        logging.debug(sql)
        if not options.simulate:
            a.execute(sql)    

    # Enable table func
    if options.create_extension:
        sql = "CREATE EXTENSION tablefunc"
        logging.debug(sql)
        if not options.simulate:
            a.execute(sql)    
            
    sql = "SET SEARCH_PATH TO '{}, default'".format(options.schema)
    logging.debug(sql)
    if not options.simulate:
        a.execute(sql)
    
    # Drop old tables
    if options.force:
        sql = "DROP TABLE IF EXISTS {schema}.data, {schema}.location".format(schema=options.schema)
        logging.debug(sql)
        if not options.simulate:
            a.execute(sql)


    # Create partition functions
    sql = """
    CREATE OR REPLACE FUNCTION create_partition(IN base_name text,
    IN for_time timestamp)
    RETURNS VOID AS
    $BODY$
    DECLARE
      table_name text;
    BEGIN
      table_name := base_name || TO_CHAR(for_time, '_YY_MM');
    
      EXECUTE 'CREATE TABLE IF NOT EXISTS ' || table_name ||
          '(LIKE ' || base_name || ' INCLUDING ALL,
              CHECK(time >= DATE ''' || date_trunc('month', for_time) ||
                  ''' and time < DATE ''' ||
                  date_trunc('month', for_time + '1 months'::interval) || ''')
           ) INHERITS (' || base_name || ')';
    END
    $BODY$
      LANGUAGE PLpgSQL
      STRICT
      VOLATILE
      EXTERNAL SECURITY INVOKER;
    """

    logging.debug(sql)
    if not options.simulate:
        a.execute(sql)


    sql = """
    CREATE OR REPLACE FUNCTION trg_insert_data()
    RETURNS TRIGGER AS $$
    DECLARE
      old_time {schema}.data.time%TYPE := NULL;
    BEGIN
      -- Here we use time to insert into appropriate partition
      EXECUTE 'insert into {schema}.data_' || to_char(NEW.time, 'YY_MM') ||
          ' values ( $1.* )' USING NEW;
    
      -- Prevent insertion into master table
      RETURN NULL;
    EXCEPTION
    WHEN undefined_table THEN
      -- Use exclusive advisory lock to prevent two transactions
      -- trying to create new partition at the same time
      PERFORM pg_advisory_xact_lock('{schema}.data'::regclass::oid::integer);
 
      -- Create a new partition if another transaction didn't already do it
      PERFORM create_partition('{schema}.data', NEW.time);
 
      -- Try the insert again
      EXECUTE 'insert into {schema}.data_' || to_char(NEW.time, 'YY_MM') ||
          ' values ( $1.* )' USING NEW;
    
      -- Prevent insertion into master table
      RETURN NULL;
    END;
    $$
    LANGUAGE plpgsql;""".format(schema=options.schema)
    
    logging.debug(sql)
    if not options.simulate:
        a.execute(sql)

    sql = """
    CREATE TRIGGER data_insert BEFORE INSERT
      ON {schema}.data FOR EACH ROW
      EXECUTE PROCEDURE trg_insert_data(); 
    COMMIT; """.format(schema=options.schema)

    logging.debug(sql)
    if not options.simulate:
        a.execute(sql)
            
    # Create location table
    sql = """
    CREATE TABLE {schema}.location
    (
      id SERIAL PRIMARY KEY,
      name character varying(254),
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

    sql = "CREATE INDEX loc_idx ON {schema}.location USING GIST (geom)".format(schema=options.schema)
    logging.debug(sql)
    if not options.simulate:
        a.execute(sql)
            
    # Create data table
    sql = """
    CREATE TABLE {schema}.data
    (
      id SERIAL PRIMARY KEY,
      type character varying(254),
      dataset character varying(254),
      "time" TIMESTAMP,
      location_id bigint REFERENCES {schema}.location (id) ON DELETE NO ACTION,
      parameter character varying(254),
      value double precision,
      "row" character varying(254)
    )
    WITH (
      OIDS = FALSE
    )
    TABLESPACE pg_default;
    """.format(schema=options.schema)

    logging.debug(sql)
    if not options.simulate:
        a.execute(sql)

    # Indexes
    sql = "CREATE INDEX row_idx ON {schema}.data (row)".format(schema=options.schema)
    logging.debug(sql)
    if not options.simulate:
        a.execute(sql)    

    sql = "CREATE INDEX location_id_idx ON {schema}.data (location_id)".format(schema=options.schema)
    logging.debug(sql)
    if not options.simulate:
        a.execute(sql)    

    #sql = "CREATE INDEX parameter_idx ON {schema}.data (parameter)".format(schema=options.schema)
    #logging.debug(sql)
    #if not options.simulate:
    #    a.execute(sql)
        
    #sql = "ALTER TABLE traindata_test.location OWNER to weatherproof_rw;"
    
if __name__=='__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--force',
                        action='store_true',
                        help='If set, existing tables are dropped, default=False')
    parser.add_argument('--simulate',
                        action='store_true',
                        help='Simulate only, default=False')
    parser.add_argument('--create_extension',
                        action='store_true',
                        help='Create postgis extension, default=False')        
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
