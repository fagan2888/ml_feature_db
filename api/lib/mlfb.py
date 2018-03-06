#!/usr/bin/python
# -*- coding: utf-8 -*-
import psycopg2
from configparser import ConfigParser
import logging
import numpy as np
import os
import datetime

class mlfb(object):

    conn = None
    config_filename = None
    schema = 'traindata'
    id = 1
    
    def __init__(self, id=1, logging_level='INFO', config_filename=None, schema='traindata'):
        self.id = id
        self.schema = schema

        if config_filename is None:
            config_filename = os.path.dirname(os.path.abspath(__file__))+'/../cnf/database.ini'
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
            # logging.debug('PostgreSQL database version:')
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            # logging.debug(db_version)

            # close the communication with the PostgreSQL
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.critical(error)
        finally:
            if conn is not None:
                conn.close()
                logging.debug('Database connection closed.')


    def get_rows(self, dataset_name, geom_type='point'):
        """ 
        Get all feature rows from given dataset
        
        dataset_name : str
                       dataset name
        geom_type : ('point'|'wkt')
                    How geometry is returned. If set to point, {'lat' x.xx, 'lon': x.xx} dict is returned. If set to WKT, WKT is returned. Default point
        """

        sql = """
             SELECT b.id, EXTRACT(epoch from a.time) as t
             """
        if geom_type == 'point':            
            sql += ", ST_x(geom) as lon, ST_y(geom) as lat,"
        else:
            sql += ", ST_AsText(geom) as wkt, 1,"

        sql += """ 
              a.parameter, a.value, a.row 
              FROM {schema}.data a, {schema}.location b 
              WHERE a.location_id = b.id AND a.type='feature' 
                AND a.row is not null AND a.dataset='{dataset}'
              ORDER BY a.row, t, a.location_id, a.parameter LIMIT 100
              """.format(schema=self.schema, dataset=dataset_name)
        
        # logging.debug(sql)
        rows = self._query(sql)

        result = []
        header = [] #'time', 'lon', 'lat']
        metadata = []
        prev_row_id = None
        resrow = []

        while len(rows) > 0:
            row = rows.pop(0)
            row_id = row[6]

            if row_id == prev_row_id or prev_row_id is None:
                resrow.append(row[5])
                if row[4] not in header:
                    header.append(row[4])
                prev_row_id = row_id
            else:                
                if len(resrow) == len(header):
                    result.append(resrow)
                    metadata.append([row[1], row[0], row[2], row[3]])
                else:
                    logging.error('Row with id {} has wrong length of {} while it should be {}'.format(row_id, len(resrow), len(header)))
                    print(resrow)

                resrow = [row[5]]
                if row[4] not in header:
                    header.append(row[4])
                prev_row_id = row_id
                
        return metadata, header, np.array(result)

    def add_point_locations(self, locations, check_for_duplicates=False):
        """
        Add locations to the db
        
        locations : list
                    location information in following format: ['name', 'lat', 'lon']
        """

        ids = []
        if check_for_duplicates:
            for loc in locations:
                id = self.get_location_by_name(loc[0])
            
                if id is not None:
                    logging.debug('Found id {} for location named {}'.format(id, loc[0]))
                    ids.append(id)
                else:
                    logging.debug('Location with name {} not found, creating...'.format(loc[0]))
                    sql = "INSERT INTO {schema}.location (name, geom) VALUES ('{name}', ST_GeomFromText('POINT({lon} {lat})'))".format(name=loc[0], lat=loc[1], lon=loc[2], schema=self.schema)
                    # logging.debug(sql)
                    self.execute(sql)
                    id = self.get_location_by_name(loc[0])
                    ids.append(id)
        else:
            sql = "INSERT INTO {schema}.location (name, geom) VALUES ".format(schema=self.schema)
            first = True
            for loc in locations:
                if not first:
                    sql = sql+', '
                else:
                    first = False
                sql = sql + "('{name}', ST_GeomFromText('POINT({lon} {lat})'))".format(name=loc[0], lat=loc[1], lon=loc[2])
            self.execute(sql)

    def add_rows(self, _type, header, data, metadata, dataset):
        """
        Add rows to the db
        
        type     : str
                   feature or label
        header   : list        
                   list containing data header (i.e. 'temperature';'windspeedms')
        data     : np.array      
                   numpy array or similar containing data in the same order with header
        metadata : list
                   list containing metadata in following order ['time', 'location_id']
        dataset   : str
                   optional dataset information
        """

        logging.info('Trying to insert {} {}s with dataset {}'.format(len(data), _type, dataset))
        
        sql = "INSERT INTO {schema}.data (type, dataset, time, location_id, parameter, value, row) VALUES ".format(schema=self.schema)
        i = 0
        first = True
        for row in data:            
            j = 0
            row = dataset+'-'+str(i)
            for param in header:
                if metadata[i][1] is None:
                    logging.error('No location for row {}'.format(i))
                    continue
                
                if not first: sql = sql+', '
                else: first = False

                if isinstance(metadata[i][0], int):
                    t =  datetime.datetime.fromtimestamp(int(metadata[i][0]))
                else:
                    t = metadata[i][0]
                
                sql = sql + "('{_type}', '{dataset}', '{time}', {location_id}, '{parameter}', {value}, '{row}')".format(_type=_type, dataset=dataset, time=t.strftime('%Y-%m-%d %H:%M:%S'), location_id=metadata[i][1], parameter=param, value=data[i][j], row=row)
                j += 1                        
            i +=1

        # logging.debug(sql)
        self.execute(sql)        

    def get_locations_by_name(self, names):
        """
        Find location ids by names
        
        names : list
                list of location names
        """
        sql = "SELECT id, name FROM {}.location WHERE name IN ({})".format(self.schema, '\''+'\',\''.join(names)+'\'')
        # logging.debug(sql)

        return self._query(sql)
    
    def get_location_by_name(self, name):
        """
        Find location id by name
        
        name : str
               location name
        
        return id (int) or None
        """
        sql = "SELECT id FROM {schema}.location WHERE name='{name}'".format(schema=self.schema, name=name)
        # logging.debug(sql)
        res = self._query(sql)
        if len(res) > 0:
            return int(res[0][0])

        return None

    def get_locations_by_dataset(self, dataset, geom_type='point'):
        """
        Get all locations attached to dataset.
        
        dataset   : str
                    dataset name
        geom_type : ('point'|'wkt')
                    How geometry is returned. If set to point, {'lat' x.xx, 'lon': x.xx} dict is returned. If set to WKT, WKT is returned. Default point
        """        

        sql = "SELECT id, name"
        if geom_type == 'point':            
            sql += ", ST_x(geom) as lon, ST_y(geom) as lat"
        else:
            sql += ", ST_AsText(geom) as wkt"
        sql += " FROM {schema}.location WHERE id IN (SELECT location_id FROM {schema}.data WHERE dataset='{dataset}')".format(schema=self.schema, dataset=dataset)
        # logging.debug(sql)
        return self._query(sql)        
        
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



