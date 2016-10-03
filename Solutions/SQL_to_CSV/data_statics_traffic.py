# -*- coding: utf-8 -*-
"""
Created on Fri May 27 22:53:42 2016

@author: tanfan.zjh
"""


class TrafficData:
    def __init__(self,date,district_id,time_slot_id,
                 tj_1_j,tj_2_j,tj_3_j,tj_4_j,
                 tj_1_j_1,tj_2_j_1,tj_3_j_1,tj_4_j_1,
                 tj_1_j_2,tj_2_j_2,tj_3_j_2,tj_4_j_2,
                 tj_1_j_3,tj_2_j_3,tj_3_j_3,tj_4_j_3):
        self.date = date
        self.district_id = district_id
        self.time_slot_id = time_slot_id
        self.tj_1_j = tj_1_j
        self.tj_2_j = tj_2_j
        self.tj_3_j = tj_3_j
        self.tj_4_j = tj_4_j
        self.tj_1_j_1 = tj_1_j_1
        self.tj_2_j_1 = tj_2_j_1
        self.tj_3_j_1 = tj_3_j_1
        self.tj_4_j_1 = tj_4_j_1
        self.tj_1_j_2 = tj_1_j_2
        self.tj_2_j_2 = tj_2_j_2
        self.tj_3_j_2 = tj_3_j_2
        self.tj_4_j_2 = tj_4_j_2
        self.tj_1_j_3 = tj_1_j_3
        self.tj_2_j_3 = tj_2_j_3
        self.tj_3_j_3 = tj_3_j_3
        self.tj_4_j_3 = tj_4_j_3
        
        self._all = [self.date,self.district_id,self.time_slot_id,
                     self.tj_1_j,self.tj_2_j,self.tj_3_j,self.tj_4_j,
                     self.tj_1_j_1,self.tj_2_j_1,self.tj_3_j_1,self.tj_4_j_1,
                     self.tj_1_j_2,self.tj_2_j_2,self.tj_3_j_2,self.tj_4_j_2,
                     self.tj_1_j_3,self.tj_2_j_3,self.tj_3_j_3,self.tj_4_j_3]
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


from tools import align_timeslot_traffic_data,align_district_traffic_data
if __name__ == '__main__':
    import time
    print time.asctime()
    conn,cur = get_database_connection('didi_data_test_set_1.sqlite3')
    data_file = open('data-traffic.csv','wb')
    data_file.write('date,start_district_id,time_slot_id,tj_1_j,tj_2_j,tj_3_j,tj_4_j,tj_1_j_1,tj_2_j_1,tj_3_j_1,tj_4_j_1,tj_1_j_2,tj_2_j_2,tj_3_j_2,tj_4_j_2,tj_1_j_3,tj_2_j_3,tj_3_j_3,tj_4_j_3\n')
    traffic_statics_sql = "select date,district_id,time_slot_id,\
                        tj_level_1,tj_level_2,tj_level_3,tj_level_4 \
                        from Traffic_Training group by date,district_id,\
                        time_slot_id;"
    
    print 'query for traffic data...'
    fetch = cur.execute(traffic_statics_sql)
    result = fetch.fetchall()
    
    new_result = align_timeslot_traffic_data(result)
    new_result2 = align_district_traffic_data(new_result)

    result = new_result2 
    result.insert(0,['',0,0,0,0,0,0])
    result.insert(0,['',0,0,0,0,0,0])
    result.insert(0,['',0,0,0,0,0,0])
    
    print 'start iter results...'
    for item1,item2,item3,item4 in zip(result[3:],result[2:-1],
                                       result[1:-2],result[0:-3]):
        date,start_district_id,time_slot_id,tj_1,tj_2,tj_3,tj_4 = item1
        _,_,_,tj_1_j_1,tj_2_j_1,tj_3_j_1,tj_4_j_1 = item2
        _,_,_,tj_1_j_2,tj_2_j_2,tj_3_j_2,tj_4_j_2 = item3
        _,_,_,tj_1_j_3,tj_2_j_3,tj_3_j_3,tj_4_j_3 = item4
        
        # special case
        if time_slot_id == 1:
            tj_1_j_1 = 0
            tj_2_j_1 = 0
            tj_3_j_1 = 0
            tj_4_j_1 = 0
            
            tj_1_j_2 = 0
            tj_2_j_2 = 0
            tj_3_j_2 = 0
            tj_4_j_2 = 0

            tj_1_j_3 = 0
            tj_2_j_3 = 0
            tj_3_j_3 = 0
            tj_4_j_3 = 0
        elif time_slot_id == 2:
            tj_1_j_2 = 0
            tj_2_j_2 = 0
            tj_3_j_2 = 0
            tj_4_j_2 = 0

            tj_1_j_3 = 0
            tj_2_j_3 = 0
            tj_3_j_3 = 0
            tj_4_j_3 = 0
        elif time_slot_id == 3:
            tj_1_j_3 = 0
            tj_2_j_3 = 0
            tj_3_j_3 = 0
            tj_4_j_3 = 0
            
        data = TrafficData(date,start_district_id,time_slot_id,
                           tj_1,tj_2,tj_3,tj_4,
                           tj_1_j_1,tj_2_j_1,tj_3_j_1,tj_4_j_1,
                           tj_1_j_2,tj_2_j_2,tj_3_j_2,tj_4_j_2,
                           tj_1_j_3,tj_2_j_3,tj_3_j_3,tj_4_j_3)
        data_file.write(data.to_csv())
        data_file.write('\n')
            
    data_file.close()
    close_connection(cur,conn)
    print time.asctime()
#statics_sql = "select count(*) from Orders_Training where date='2016-01-01' and\
#                start_district_id=1 and time_slot_id=11111"


