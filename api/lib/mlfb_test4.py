#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
#import sys
from configparser import ConfigParser
import logging

# v. 0.2 21.2.2018 10.01
# v. 0.1 20.2.2018 10.37

class mlfb_test4(object):

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
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            print('PostgreSQL database version:')
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(db_version)

            # close the communication with the PostgreSQL
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')


    def get_rows_trains(self):
        """ query data from the trains table """
        conn = None
        try:
            params = self.config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute("""
                SELECT 
                --aa1.id, aa1.type, aa1.source_opt,aa1.fk_id,aa1.parameter,aa1.value,  bb2.id, bb2.name,bb2.lat,bb2.lon
                aa1.id, aa1.type, aa1.source,aa1.location_id,aa1.parameter,aa1.value,  bb2.id, bb2.name,bb2.lat,bb2.lon

                --FROM traindata.trains_fmi_trainingdata AA1
                FROM traindata.data AA1
                --INNER JOIN traindata.trains_fmi_location_wgs84 BB2 ON AA1.fk_id = BB2.id
                INNER JOIN traindata.location BB2 ON AA1.location_id = BB2.id
                ORDER BY bb2.id;
                
                
            """)


            print("The number of training.trains_fmi_trainingdata and locagtion: ", cur.rowcount)
            row = cur.fetchone()

            while row is not None:
                print(row)
                row = cur.fetchone()

            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    #def get_rows_trains_2(self,type_in,source_in,time_in,location_id_in,parameter_in,value_in):
    #def get_rows_trains_2(self):
    #def get_rows_trains_4(self,location_id_in):
    #def get_rows_trains_4(self,parameter_in,value_in):
    def get_rows_trains_4(self,parameter_in,value_in):
        """ get data from the trains table """
        conn = None
        var1 = 'testa1'
        try:
            print(parameter_in,value_in)
            #print(location_id_in)
            # print(type_in)
            params = self.config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            # cur.execute("select * from traindata_test.test5a where id = '%s'", (location_id_in,))
            # cur.execute("select * from traindata_test.data where id = '%s'", (location_id_in,))
            # cur.execute("select * from traindata_test.data where parameter = '%s' and value_in=%s'", (parameter_in,value_in,))
            # cur.execute("select * from traindata_test.data where parameter = %s and value_in=%s'", (parameter_in,value_in,))
            # cur.execute("select * from traindata_test.data where parameter = %s and value_in='%s'", (parameter_in,value_in,))
            #cur.execute("select * from traindata_test.data where parameter = %s and value='%s'", (parameter_in,value_in,))
            #cur.execute("select parameter,value,time,type,id,location_id from traindata_test.data where parameter = %s and value='%s'", (parameter_in,value_in,))
            #
            cur.execute("select AA1.parameter,AA1.value,AA1.time,AA1.type,BB2.name,BB2.geom,AA1.id,AA1.location_id from traindata_test.data AA1 INNER JOIN traindata_test.location BB2 ON AA1.location_id=BB2.id and AA1.parameter = %s and AA1.value='%s'", (parameter_in,value_in,))
                            
            print("The number of training.trains_fmi_trainingdata and location: ", cur.rowcount)
            row = cur.fetchone()

            while row is not None:
                print(row)
                row = cur.fetchone()

            
            # close communication with the database
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()


    # http://www.postgresqltutorial.com/postgresql-python/insert/
    def insert_row_trains_1(self,type_in,source_in,time_in,location_id_in,parameter_in,value_in):
        """ insert into data from the trains table """
        conn = None
        var1 = 'testa1'
        try:
            # print(type_in)
            params = self.config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO traindata_test.test5a(type,source,time,location_id,parameter,value) 
                VALUES(%s, %s, %s,%s, %s, %s);
                """,
                (type_in, source_in,time_in,location_id_in,parameter_in,value_in))

            #row = cur.fetchone()

            # commit the changes to the database
            conn.commit()
            
            # close communication with the database
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def insert_row_trains_2(self,type_in,source_opt_in,fk_id_in,parameter_in):
        """ insert into data from the trains table """
        conn = None
        try:
            params = self.config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO traindata_test.trains_fmi_trainingdata_t1(type,source_opt,fk_id,parameter,value) values(%s,%s,%s,%s,%s)
            """)

            #row = cur.fetchone()

            # commit the changes to the database
            conn.commit()
            
            # close communication with the database
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                
    def get_features(self):
        """ get_features: query rows containing type=feature data from the trains table """
        conn = None
        try:
            params = self.config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute("""
                SELECT 
                -- aa1.id, aa1.type, aa1.source_opt,aa1.fk_id,aa1.parameter,aa1.value,  bb2.id, bb2.name,bb2.lat,bb2.lon
                aa1.id, aa1.type, aa1.source,aa1.location_id,aa1.parameter,aa1.value,  bb2.id, bb2.name,bb2.lat,bb2.lon

                -- FROM traindata.trains_fmi_trainingdata AA1
                FROM traindata.data AA1
                -- INNER JOIN traindata.trains_fmi_location_wgs84 BB2 ON AA1.fk_id = BB2.id
                INNER JOIN traindata.location BB2 ON AA1.location_id = BB2.id
                WHERE aa1.type='feature' 
                ORDER BY bb2.id
                ;
            """)


            print("The number of type=feature training.trains_fmi_trainingdata and locagtion: ", cur.rowcount)
            row = cur.fetchone()

            while row is not None:
                print(row)
                row = cur.fetchone()

            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def get_labels(self):
        """ get_labels: query rows containing type=label data from the trains table """
        conn = None
        try:
            params = self.config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute("""
                SELECT 
                -- aa1.id, aa1.type, aa1.source_opt,aa1.fk_id,aa1.parameter,aa1.value,  bb2.id, bb2.name,bb2.lat,bb2.lon
                aa1.id, aa1.type, aa1.source,aa1.location_id,aa1.parameter,aa1.value,  bb2.id, bb2.name,bb2.lat,bb2.lon

                -- FROM traindata.trains_fmi_trainingdata AA1
                FROM traindata.data AA1
                -- INNER JOIN traindata.trains_fmi_location_wgs84 BB2 ON AA1.fk_id = BB2.id
                INNER JOIN traindata.location BB2 ON AA1.location_id = BB2.id
                WHERE aa1.type='label' 
                ORDER BY bb2.id
                ;
            """)


            print("The number of type=label training.trains_fmi_trainingdata and locagtion: ", cur.rowcount)
            row = cur.fetchone()

            while row is not None:
                print(row)
                row = cur.fetchone()

            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                          
    #add_rows(source|string, type|string, header|list, data|np.array)
    # Dummy
    #
    def add_rows(self,type_in,source_opt_in,fk_id_in,parameter_in):
        """ insert into data from the trains table """
        conn = None
        try:
            params = self.config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO traindata_test.trains_fmi_trainingdata_t1(type,source_opt,fk_id,parameter,value) values(%s,%s,%s,%s,%s)
            """)

            #row = cur.fetchone()

            # commit the changes to the database
            conn.commit()
            
            # close communication with the database
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    #get_rows (source, type, start_time=Null, end_time=Null, bbox=Null, location_id=Null, parameter=Null)
    # Dummy
    #
    def get_rows(self,type_in,start_time_in,end_time_in,parameter_in):
        """ get_rows: query rows containing data from the trains table """
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
                WHERE aa1.type=type_in and aa1.time between start_time_in and end_time_in and aa1.parameter=parameter_in
                ORDER BY bb2.id
                ;
            """)


            print("The number of type=label training.trains_fmi_trainingdata and locagtion: ", cur.rowcount)
            row = cur.fetchone()

            while row is not None:
                print(row)
                row = cur.fetchone()

            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                
    #add_columns (source, type, start_time, end_time, bbox, location_id, parameter, value)                                    
    # Dummy
    #
    def add_columns(self,type_in,start_time_in,end_time_in,parameter_in,value_in):
        """ insert into data from the trains table """
        conn = None
        try:
            params = self.config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO traindata_test.trains_fmi_trainingdata_t1(type,time,location_id,parameter,value) values(%s,%s,%s,%s,%s)
            """)

            #row = cur.fetchone()

            # commit the changes to the database
            conn.commit()
            
            # close communication with the database
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    # Optionaaliset metodit
    # http://www.postgresqltutorial.com/postgresql-python/insert/
    # Insert list
    #def insert_row_list_3(self,type_in,source_opt_in,fk_id_in,parameter_in):
    #sql = "INSERT INTO traindata_test.trains_fmi_trainingdata_t1(type,source_opt,fk_id,parameter,value) VALUES(%s,%s,%s,%s,%s)"

    # execute the INSERT statement
    #cur.executemany(sql,input_list)
    
    #if __name__ == '__main__':
    #get_rows_trains()
    
    