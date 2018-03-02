#!/usr/bin/python
# -*- coding: utf-8 -*-
import psycopg2
from configparser import ConfigParser
import logging
import numpy as np

class mlfb(object):

    conn = None
    config_filename = None
    id = 1
    
    def __init__(self, id=1, logging_level='INFO', config_filename='cnf/database.ini'):
        self.id = id
        self.config_filename = config_filename

    def config(self, section='postgresql'):
        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(self.config_filename)

        # get section, default to postgresql
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, self.config_filename))

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


    def get_rows(self):
        """ query data """

        # row id is not available, use time instead until it is                    
        sql = """
        SELECT b.id, EXTRACT(epoch from a.timestamp2) as time, st_x(b.geom) as lon, st_y(b.geom) as lat, a.parameter, a.value, a.row
        FROM traindata_test.data a, traindata_test.location b
        WHERE a.location_id = b.id AND a.type='feature' AND a.row is not null 
        ORDER BY time, a.location_id, a.parameter
        LIMIT 10000
        """
        
        logging.debug(sql)
        rows = self._query(sql)

        result = []
        header = ['time', 'lon', 'lat']
        prev_row_id = None
            
        while len(rows) > 0:
            row = rows.pop()
            # row_id = row[6]
            row_id = row[1]
            # print("Prev id: {} - id: {}".format(prev_row_id, row_id))
            if row_id != prev_row_id:
                try:
                    if len(resrow) == len(header):                        
                        result.append(resrow)
                    else:
                        print(resrow)
                except:
                    pass
                
                resrow = [row[1], row[2], row[3]]
                prev_row_id = row_id
            else:
                resrow.append(row[5])
                if row[4] not in header:
                    header.append(row[4])
                
        print(header)
        print(np.array(result))

    def execute(self, statement):
        """
        Execute single SQL statement in
        a proper manner
        """
        self._connect()
        with self.conn as conn:
            with conn.cursor() as curs:
                curs.execute(statement)
        
    def _connect(self):
        """ Create connection if needed """
        params = self.config()
        self.conn = psycopg2.connect(**params)
        return self.conn
        
    def _query(self, sql):
        """
        Execute query and return results
        
        sql str sql to execute
        
        return list of sets
        """
        self._connect()
        with self.conn as conn:
            with conn.cursor() as curs:
                curs.execute(sql)
                results = curs.fetchall()
                return results



