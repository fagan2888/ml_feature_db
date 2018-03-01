#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
from configparser import ConfigParser
import logging

class mlfb(object):

    def __init__(self, id):
        self.id = 1


    def config(self, filename='cnf/database.ini', section='postgresql'):
        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(filename)

        # get section, default to postgresql
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))

        return db


    def connect(self):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params = self.config()

            # connect to the PostgreSQL server
            logging.info('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            logging.debug('PostgreSQL database version:')
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            logging.debug(db_version)

            # close the communication with the PostgreSQL
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.critical(error)
        finally:
            if conn is not None:
                conn.close()
                logging.debug('Database connection closed.')


    def get_rows_trains(self):
        """ query data from the trains table """
        conn = None
        try:
            params = self.config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute("""
                SELECT 
                aa1.id, aa1.type, aa1.source_opt,aa1.fk_id,aa1.parameter,aa1.value,  bb2.id, bb2.name,bb2.lat,bb2.lon

                FROM traindata.trains_fmi_trainingdata AA1
                INNER JOIN traindata.trains_fmi_location_wgs84 BB2 ON AA1.fk_id = BB2.id
                ORDER BY bb2.id;
            """)

            logging.info("The number of training.trains_fmi_trainingdata and locagtion: {}".format(cur.rowcount))
            row = cur.fetchone()

            while row is not None:
                # logging.debug(row)
                row = cur.fetchone()

            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.critical(error)
        finally:
            if conn is not None:
                conn.close()


