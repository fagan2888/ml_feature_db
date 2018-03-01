#!/usr/bin/python
# -*- coding: utf-8 -*-
from configparser import ConfigParser
#from lib import mlfb
from lib import mlfb_test4

# Example script to use mlfb class

a = mlfb_test4.mlfb_test4(1)

#a.get_rows_trains()

#a.get_labels()

#a.get_features()

#input1='test9988'
#input1='999'
#input1=999
#input1=99.88
#input1=99,66
#input1=99.55
#input1='atest9988'
#input1=97
#input1='atest1188'
#              (type_in, 'null', '20180226T165000',666,'testpara1',665))
#type_in,time_in,location_id_in,parameter_in,value_in
input_type='4test2288'
input_source='null'
input_time='20180226T165000'
input_location_id=455
input_parameter='test2para'
input_value=441

#a.insert_row_trains_1('test99')
#a.insert_row_trains_1(input_type,input_source,input_time,input_location_id,input_parameter,input_value)



#input_location_id=5
input_location_id=1
#input_parameter='temperature'
input_parameter='temperature'
input_value=-9

# get rows
# def get_rows_trains_2(self,type_in,source_in,time_in,location_id_in,parameter_in,value_in):
#a.get_rows_trains_2(type_in,source_in,time_in,location_id_in,parameter_in,value_in)
#a.get_rows_trains_2()
#a.get_rows_trains_4(input_location_id)
a.get_rows_trains_4(input_parameter,input_value)

