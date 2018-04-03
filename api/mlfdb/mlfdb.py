#!/usr/bin/python
# -*- coding: utf-8 -*-
import psycopg2
from configparser import ConfigParser
import logging
import numpy as np
import os
import re
import datetime
from os.path import expanduser
from google.cloud import storage

class mlfdb(object):

    conn = None
    config_filename = None
    schema = 'traindata'
    id = 1    
    
    def __init__(self, id=1, logging_level='INFO', config_filename=None, schema='traindata'):
        self.id = id
        self.schema = schema

        if config_filename is None:
            home = expanduser("~")
            config_filename = home+'/.mlfdbconfig'
            #config_filename = os.path.dirname(os.path.abspath(__file__))+'/../cnf/database.ini'
        elif config_filename[0:2] == 'gs':
            bucket_name = re.search('(?<=//).*?(?=/)', config_filename).group(0)
            blob_name = re.search('(?<=gs://'+bucket_name+'/).*$', config_filename).group(0)
            tmp_filename = '/tmp/creds'
            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            blob = storage.Blob(blob_name, bucket)
            blob.download_to_filename(tmp_filename)
            config_filename = tmp_filename
            
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
                
    def get_rows_from_postgre_to_numpy(self,parameter_in,value_in):
        """ Method: get data from the trains table Postgre table and return Numpy array with return sentence"""
        conn = None
        var1 = 'testa1'
        try:
            logging.info(parameter_in,value_in)
            # logging.info(location_id_in)
            # logging.info(type_in)
            params = self.config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute("select AA1.parameter,AA1.value,AA1.time,AA1.type,BB2.name,BB2.geom,AA1.id,AA1.location_id from traindata._data AA1 INNER JOIN traindata._location BB2 ON AA1.location_id=BB2.id and AA1.parameter = %s and AA1.value='%s'", (parameter_in,value_in,))
                            
            logging.info("The number of training.trains_fmi_trainingdata and location: ", cur.rowcount)
            row = cur.fetchone()

            
            # Parse data to numpy array
            logging.info('Parsing data to np array...')
            result = []
 
            while row is not None:
                #logging.info(row)
                row = cur.fetchone()
                #result = cur.fetchone()
                result.append(row)

            
            result = np.array(result)
            print(result)
            return result


            # close communication with the database
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()


    def get_rows(self, dataset_name,
                 starttime=None, endtime=None,
                 geom_type='point', rowtype='feature'):
        """ 
        Get all feature rows from given dataset
        
        dataset_name : str
                       dataset name
        starttime : DateTime
                    start time of rows (by default all rows are returned)
        endtime : DateTime
                    end time of rows (by default all rows are returned)
        geom_type : ('point'|'wkt')
                    How geometry is returned. If set to point, {'lat' x.xx, 'lon': x.xx} dict is returned. If set to WKT, WKT is returned. Default point
        rowtype : str
                  Type of rows to be returned (default feature)
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
              WHERE a.location_id = b.id AND a.type='{type}' 
                AND a.row is not null AND a.dataset='{dataset}'
        """.format(schema=self.schema, dataset=dataset_name, type=rowtype)

        if starttime is not None:
            sql += """
             AND a.time >= '{starttime}'
            """.format(starttime=starttime.strftime('%Y-%m-%d %H:%M:%S'))

        if endtime is not None:
            sql += """
             AND a.time >= '{endtime}'
            """.format(endtime=endtime.strftime('%Y-%m-%d %H:%M:%S'))
            
        sql += """
             ORDER BY a.row, t, a.location_id, a.parameter
              """
        
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

                resrow = [row[5]]
                if row[4] not in header:
                    header.append(row[4])
                prev_row_id = row_id

        data = np.array(result)
        
        logging.debug('{} \n'.format(rowtype))
        logging.debug('Header is: \n {} \n'.format(','.join(header)))
    
        logging.debug('Shape of metadata: {}'.format(np.array(metadata).shape))
        logging.debug('Sample of metadata: \n {} \n'.format(np.array(metadata)))
    
        logging.debug('Shape of data {}'.format(data.shape))
        logging.debug('Sample of data: \n {} \n '.format(data))
                
        return metadata, header, data

    def add_point_locations(self, locations, check_for_duplicates=False):
        """
        Add locations to the db
        
        locations : list
                    location information in following format: ['name', 'lat', 'lon']
        """
        self._connect()

        logging.info('Adding {} locations to db...'.format(len(locations)))
        
        ids = []
        if check_for_duplicates:
            for loc in locations:
                id = self.get_location_by_name(loc[0])
            
                if id is not None:
                    logging.info('Found id {} for location named {}'.format(id, loc[0]))
                    ids.append(id)
                else:
                    logging.info('Location with name {} not found, creating...'.format(loc[0]))
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

    def add_rows(self, _type, header, data, metadata, dataset, row_prefix='', row_offset=0):
        """
        Add rows to the db
        
        type       : str
                     feature or label
        header     : list        
                     list containing data header (i.e. 'temperature';'windspeedms')
        data       : np.array      
                     numpy array or similar containing data in the same order with header
        metadata   : list
                     list containing metadata in following order ['time', 'location_id']
        dataset    : str
                     optional dataset information
        row_prefix : str
                     row prefix to add (used if adding rows is done in paraller)
        row_offset : int
                     offset for row numbering (used if adding rows is split to batches)

        return int amount of added rows
        """
        logging.debug('Trying to insert {} {}s with dataset {}'.format(len(data), _type, dataset))
        self._connect()
        
        sql = "INSERT INTO {schema}.data (type, dataset, time, location_id, parameter, value, row) VALUES ".format(schema=self.schema)
        i = 0 # <-- for row indexing
        first = True
        row_num = -1 # <-- for finding correct row
        for row in data:            
            j = 0 # <-- for parameter
            row_num += 1
            for param in header:
                if metadata[row_num][1] is None:
                    logging.error('No location for row {} (row: {})'.format(row_num, metadata[row_num]))
                    continue
                
                if not first: sql = sql+', '
                else: first = False

                if isinstance(metadata[row_num][0], int):
                    t = datetime.datetime.fromtimestamp(int(metadata[row_num][0]))
                else:
                    t = metadata[row_num][0]

                loc_id = metadata[row_num][1]
                row = _type+'-'+dataset+'-'+str(t.timestamp())+'-'+str(loc_id)+'-'+str(i+row_offset)
                    
                sql = sql + "('{_type}', '{dataset}', '{time}', {location_id}, '{parameter}', {value}, '{row}')".format(_type=_type, dataset=dataset, time=t.strftime('%Y-%m-%d %H:%M:%S'), location_id=loc_id, parameter=param, value=data[row_num][j], row=row)
                j += 1
                
            i +=1

        #logging.debug(sql)
        self.execute(sql)
        return i

    def remove_dataset(self, dataset, type=None, clean_locations=False):
        """
        Remove dataset
        
        dataset : str
                  dataset name
        type : str
               If set, only given type is removed (by default all types are removed)
        clean_locations : boolean
                          If True, locations with no data rows are cleaned (default False)
        """

        # Remove dataset
        logging.debug('Removing dataset "{}"'.format(dataset))
        sql = "DELETE FROM {schema}.data WHERE dataset='{dataset}'".format(schema=self.schema, dataset=dataset)
        if type is not None:
            sql += " AND type='{type}'".format(type=type)
        logging.debug(sql)
        self.execute(sql)
        
        # Clean locations
        if clean_locations:
            logging.debug('Removing locations...')
            sql = "DELETE FROM {schema}.location WHERE id NOT IN (SELECT location_id FROM {schema}.data)".format(schema=self.schema)
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



