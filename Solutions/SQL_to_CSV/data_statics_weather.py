# -*- coding: utf-8 -*-
"""
Created on Fri May 27 22:53:42 2016

@author: tanfan.zjh
"""

class WeatherData:
    def __init__(self,date,district_id,time_slot_id,
                 weather_j,weather_j_1,weather_j_2,weather_j_3,
                 temperature_j,temperature_j_1,temperature_j_2,temperature_j_3,
                 pm_j,pm_j_1,pm_j_2,pm_j_3):
        self.date = date
        self.district_id = district_id
        self.time_slot_id = time_slot_id
        self.weather_j = weather_j
        self.weather_j_1 = weather_j_1
        self.weather_j_2 = weather_j_2
        self.weather_j_3 = weather_j_3
        self.temperature_j = temperature_j
        self.temperature_j_1 = temperature_j_1
        self.temperature_j_2 = temperature_j_2
        self.temperature_j_3 = temperature_j_3
        self.temperature_avg = (self.temperature_j_1 +
                                self.temperature_j_2 + self.temperature_j_3) / 3.
        self.pm_j = pm_j
        self.pm_j_1 = pm_j_1
        self.pm_j_2 = pm_j_2
        self.pm_j_3 = pm_j_3
        self.pm_avg = (self.pm_j_1 + self.pm_j_2 + self.pm_j_3) / 3.
        self._all = [self.date,self.district_id,self.time_slot_id,
                     self.weather_j,self.weather_j_1,self.weather_j_2,self.weather_j_3,
                     self.temperature_j,self.temperature_j_1,self.temperature_j_2,
                     self.temperature_j_3,self.temperature_avg,self.pm_j,
                     self.pm_j_1,self.pm_j_2,self.pm_j_3,self.pm_avg]
        self.all = [str(item) for item in self._all]
                    
    def to_csv(self):
        return ','.join(self.all)


import sqlite3

def get_database_connection(database_name):
    conn = sqlite3.connect(database_name)
    cur = conn.cursor()
    return conn,cur

def close_connection(*f):
    for ff in f:
        ff.close()

from tools import align_weather_district_data,align_weather_timeslot_data
if __name__ == '__main__':
    import time
    print time.asctime()
    conn,cur = get_database_connection('didi_data_training.sqlite3')
    data_file = open('data-weather.csv','wb')
    data_file.write('date,start_district_id,time_slot_id,weather_t(j),weather_t(j-1),weather_t(j-2),weather_t(j-3),temperature_t(j),temperature_t(j-1),temperature_t(j-2),temperature_t(j-3),temperature_avg,pm_t(j),pm_t(j-1),pm_t(j-2),pm_t(j-3),pm_avg\n')
    weather_statics_sql = "select date,time_slot_id,weather_type,temperature,pm25 \
                            from Weather_Training group by date,time_slot_id;"
    
    print 'query for weather data...'
    fetch = cur.execute(weather_statics_sql)
    result = fetch.fetchall()
    
    new_result2 = align_weather_timeslot_data(result)
    new_result = align_weather_district_data(new_result2) 
    
    result = new_result
    result.insert(0,['',0,0,0,0,0])
    result.insert(0,['',0,0,0,0,0])
    result.insert(0,['',0,0,0,0,0])
    
    print 'start iter results...'
    for item1,item2,item3,item4 in zip(result[3:],result[2:-1],
                                       result[1:-2],result[0:-3]):
        date,start_district_id,time_slot_id,weather_type,tempearture,pm = item1
        _,_,_,weather_type_j_1,tempearture_j_1,pm_j_1 = item2
        _,_,_,weather_type_j_2,tempearture_j_2,pm_j_2 = item3
        _,_,_,weather_type_j_3,tempearture_j_3,pm_j_3 = item4
        
        # special case
        if time_slot_id == 1:
            weather_type_j_1 = 0
            tempearture_j_1 = 0
            pm_j_1 = 0
            weather_type_j_2 = 0
            tempearture_j_2 = 0
            pm_j_2 = 0
            weather_type_j_3 = 0
            tempearture_j_3 = 0
            pm_j_3 = 0
        elif time_slot_id == 2:
            weather_type_j_1 = 0
            tempearture_j_1 = 0
            pm_j_1 = 0
            weather_type_j_2 = 0
            tempearture_j_2 = 0
            pm_j_2 = 0
        elif time_slot_id == 3:
            weather_type_j_3 = 0
            tempearture_j_3 = 0
            pm_j_3 = 0
            
        data = WeatherData(date,start_district_id,time_slot_id,weather_type,
                           weather_type_j_2,weather_type_j_2,weather_type_j_3,
                           tempearture,tempearture_j_1,tempearture_j_2,
                           tempearture_j_3,pm,pm_j_1,pm_j_2,pm_j_3)
        data_file.write(data.to_csv())
        data_file.write('\n')
            
    data_file.close()
    close_connection(cur,conn)
    print time.asctime()
#statics_sql = "select count(*) from Orders_Training where date='2016-01-01' and\
#                start_district_id=1 and time_slot_id=11111"


