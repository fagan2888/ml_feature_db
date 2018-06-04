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
import pandas as pd

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

    def add_rows_from_df(self, _type, df, dataset, row_prefix='', row_offset=0,
                         time_column='time', loc_column='loc_id', columns=[],
                         update=True):
        """
        Add rows from pandas dataframe

        _type       : str
                      feature or label
        df          : pd.dataframe
                      Pandas DataFrame containing data. Column names works as header.
        dataset     : str
                      optional dataset information
        row_prefix  : str
                      row prefix to add (used if adding rows is done in paraller)
                      Default: empty
        row_offset  : int
                      offset for row numbering (used if adding rows is split to batches)
                      Default: 0
        time_column : str
                      Name of the column where time information is stored
                      Default: time
        loc_column  : str
                      Name of the column where location information is stored.
                      Default: loc_id
        columns     : list
                      Name of columns to be added into db. If empty,
                      first 4 colums are assumed to be metadata and all columns
                      from 5th column are added.
                      Default: empty
        update      : boolean
                      Do update in case of conflicts
                      Default: True

        return int amount of added rows
        """
        logging.debug('Trying to insert {} {}s with dataset {}'.format(len(df), _type, dataset))
        self._connect()

        # we skip metadata and start from first data value
        if len(columns) == 0:
            columns = list(df.columns.values)[4:]

        sql = "INSERT INTO {schema}.data (type, dataset, time, location_id, parameter, value, row) VALUES ".format(schema=self.schema)

        first = True
        row_num = -1 # <-- for finding correct row
        for i, data_row in df.iterrows():
            row_num += 1
            for param in columns:
                if not first: sql = sql+', '
                else: first = False

                t = datetime.datetime.fromtimestamp(int(data_row[time_column]))
                loc_id = data_row[loc_column]

                row = _type+'-'+dataset+'-'+str(t.timestamp())+'-'+str(loc_id)+'-'+str(int(row_num)+int(row_offset))
                #print(data_row)
                #print(data_row.iloc[j])
                sql = sql + "('{_type}', '{dataset}', '{time}', {location_id}, '{parameter}', {value}, '{row}')".format(_type=_type, dataset=dataset, time=t.strftime('%Y-%m-%d %H:%M:%S'), location_id=loc_id, parameter=param, value=data_row[param], row=row)

        first = True
        if update:
            sql += ' ON CONFLICT (type, dataset, time, location_id, parameter) DO UPDATE SET value = EXCLUDED.value'.format(col=param)

        logging.debug(sql)
        self.execute(sql)
        return row_num


    def add_rows(self, _type, header, data, metadata, dataset, row_prefix='', row_offset=0, check_uniq=False, time_column=0, loc_column=1):
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

                if isinstance(metadata[row_num][time_column], int) or isinstance(metadata[row_num][time_column], float):
                    t = datetime.datetime.fromtimestamp(int(metadata[row_num][time_column]))
                else:
                    t = metadata[row_num][time_column]

                loc_id = metadata[row_num][loc_column]
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

    def get_locations_by_dataset(self, dataset, starttime, endtime, rettype='tuple'):
        """
        Get all locations attached to dataset.

        dataset   : str
                    dataset name
        starttime : DateTime
                    starttime
        endtime : DateTime
                  endtime
        rettype : str
                  tuple|dict

        returns list of tuples [(id, name, lon, lat)]
        """

        sql = "SELECT id, name, ST_x(geom) as lon, ST_y(geom) as lat"
        sql += " FROM {schema}.location b WHERE id IN (SELECT location_id FROM {schema}.data a WHERE dataset='{dataset}' AND a.time > '{starttime}' AND a.time <= '{endtime}')".format(schema=self.schema, dataset=dataset, starttime=starttime.strftime('%Y-%m-%d %H:%M:%S'), endtime=endtime.strftime('%Y-%m-%d %H:%M:%S'))

        logging.debug(sql)

        res = self._query(sql)
        if rettype == 'dict':
            return self._locs_to_dict(res)

        return res

    def clean_duplicate_rows(self, dataset, rowtype, correct_length):
        """
        Clean duplicate rows. Note! only first 1000 rows are handled.

        dataset : str
                  name of dataset
        rowtype : str
                  feature | label
        correct_length : int
                         correct length of the row

        returns : int
                  count of cleaned rows
        """
        self._connect()
        sql = "SELECT row, count(1) c FROM {schema}.data WHERE type='{rowtype}' and dataset='{dataset}' GROUP BY row ORDER BY c DESC LIMIT 1000".format(schema=self.schema, rowtype=rowtype, dataset=dataset)

        logging.debug(sql)
        rows = self._query(sql)

        duplicates = []
        for row in rows:
            if row[1] > correct_length:
                duplicates.append(row[0])


        sql = "SELECT id,type,dataset,time,location_id,parameter,row FROM {schema}.data WHERE type='{rowtype}' and dataset='{dataset}' AND row IN ({duplicates})".format(schema=self.schema, rowtype=rowtype, dataset=dataset, duplicates='\''+'\',\''.join(duplicates)+'\'')
        logging.debug(sql)

        rows = self._query(sql)

        df = pd.DataFrame(rows)
        mask = df.duplicated([1,2,3,4,5,6]).as_matrix()
        ids = df.as_matrix()
        ids = ids[(mask)][:,0]
        logging.debug(ids.shape)
        logging.debug(ids)

        sql = "DELETE FROM {schema}.data WHERE id IN ({ids})".format(schema=self.schema, ids=','.join(list(map(str,ids))))
        logging.debug(sql)

        self.execute(sql)
        return len(ids)

    def execute(self, statement):

        """
        Execute single SQL statement in
        a proper manner
        """
        self._connect()
        with self.conn as conn:
            with conn.cursor() as curs:
                return curs.execute(statement)

    def _locs_to_dict(self, locs):
        """
        Convert locations to dict where id is key
        """
        ret = {}
        for loc in locs:
            ret[loc[0]] = {'name': loc[1],
                           'lon' : loc[2],
                           'lat' : loc[3]}

        return ret

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


    def create_index(self):
        """
        Helping method to add unique index afterwards.
        """

        sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='{}'
        AND table_type='BASE TABLE'
        AND table_name like 'data_%'
        """.format(self.schema)

        tables = self._query(sql)

        for table in tables:
            table = table[0]
            sql = "CREATE UNIQUE INDEX IF NOT EXISTS idx_values_uniq ON {schema}.{table} (type, dataset, time, location_id, parameter)".format(schema=self.schema, table=table)
            print(sql)
            #self.execute(sql)
        sys.exit()

    def get_rows(self, dataset_name,
                 starttime, endtime,
                 rowtype='feature',
                 return_type='np',
                 parameters=[],
                 chunk_size=1456):
        """
        Get all feature rows from given dataset

        dataset_name : str
                       dataset name
        starttime : DateTime
                    start time of rows ( data fetched from ]starttime, endtime] )
        endtime : DateTime
                    end time of rows ( data fetched from ]starttime, endtime] )
        rowtype : str
                  Type of rows to be returned (default feature)
        return_type : str
                      whether to return np arrays or pandas dataframe (np|pandas, default np)
        parameters : list
                     list of parameters to fetch. If omited all distinct parameters from the first 100 rows are fetched
        chunk_size : int
                     how large time chunks are used while reading the data from db (to save db memory)

        returns : np array, np array, np array or pandas DataFrame depending on return_type
        """

        start = starttime
        end = starttime
        data = []

        # Do long queries in chunks to save database memory
        while end < endtime:
            end = start + datetime.timedelta(days=chunk_size)
            if end > endtime: end = endtime

            startstr = start.strftime('%Y-%m-%d %H:%M:%S')
            endstr = end.strftime('%Y-%m-%d %H:%M:%S')

            if len(parameters) == 0:
                sql = "SELECT DISTINCT(parameter) FROM (SELECT parameter FROM {schema}.data a WHERE dataset='{dataset}' AND type='{type}' AND a.time >= '{starttime}' and a.time <= '{endtime}' LIMIT 100) AS parameter".format(schema=self.schema, dataset=dataset_name, type=rowtype, starttime=startstr, endtime=endstr)

                logging.debug(sql)
                rows = self._query(sql)
                for row in rows:
                    parameters.append(row[0])

            logging.debug('Fetching following parameters: {}'.format(parameters))
            if len(parameters) == 0:
                raise ValueError('Empty parameter set')

            sql = """
            SELECT
            row_info[1] as location_id, row_info[2] as t, ST_x(b.geom) as lon, ST_y(b.geom) as lat, ct.{params}
            FROM
            crosstab ($$
              SELECT
         	ARRAY[cast(a.location_id as integer), cast(extract(epoch from a.time) as integer)] as row_info,
                parameter,
                a.value
              FROM
                {schema}.data a
              JOIN (
                VALUES """.format(params=', ct.'.join(parameters), schema=self.schema)

            first = True
            i = 0
            for param in parameters:
                if not first: sql += ", "
                else: first = False
                sql += '(\'{}\', {})'.format(param, i)
                i += 1
            sql += ') AS x (id, ordering) ON parameter = x.id'

            sql += """
              WHERE
                a.type = '{type}'
                AND dataset = '{dataset}'
                AND a.time > '{starttime}'
                AND a.time <= '{endtime}'
                AND (""".format(type=rowtype, dataset=dataset_name, params=', ct.'.join(parameters), starttime=startstr, endtime=endstr, schema=self.schema)

            first = True
            for param in parameters:
                if not first: sql += " OR "
                else: first = False
                sql += 'parameter=\'{param}\''.format(param=param)
            sql += """)
               ORDER BY row_info, x.ordering
               $$) as ct(row_info int[]"""
            for param in parameters:
                sql += ', {param} float8'.format(param=param)
            sql += """)
            LEFT JOIN {schema}.location b ON ct.row_info[1] = b.id
            """.format(schema=self.schema)

            logging.debug(sql)
            newrows = self._query(sql)
            logging.debug('{} new rows loaded from db...'.format(len(newrows)))
            data += newrows

            start = end

        logging.debug('{} rows loaded from db'.format(len(data)))

        if len(data) == 0:
            if return_type == 'pandas':
                return pd.DataFrame()
            else:
                return [], [], []

        if return_type == 'pandas':
            return pd.DataFrame(data, columns=['loc_id', 'time', 'lon', 'lat'] + parameters)
            return pd.DataFrame(data)
        else:
            data = np.array(data)
            metadata = data[:,0:4]
            data = data[:,4:]
            logging.debug('{} \n'.format(rowtype))
            logging.debug('Header is: \n {} \n'.format(','.join(parameters)))

            logging.debug('Shape of metadata: {}'.format(np.array(metadata).shape))
            logging.debug('Sample of metadata: \n {} \n'.format(np.array(metadata[0:10])))

            logging.debug('Shape of data {}'.format(data.shape))
            logging.debug('Sample of data: \n {} \n '.format(data))

            return metadata, parameters, data
