# -*- coding: utf-8 -*-
"""
Created on Fri May 27 22:53:42 2016

@author: tanfan.zjh
"""

class Data:
    def __init__(self,date,district_id,time_slot_id,
                 order_j,order_j_1,order_j_2,order_j_3):
        self.date = date
        self.district_id = district_id
        self.time_slot_id = time_slot_id
        self.order_j = order_j
        self.order_j_1 = order_j_1
        self.order_j_2 = order_j_2
        self.order_j_3 = order_j_3
        self.order_averaged = (self.order_j_1 + \
                                self.order_j_2 + self.order_j_3) / 3.
        self.p1 = self.order_j / (self.order_j_1 + 0.0001)
        self.p2 = self.order_j / (self.order_averaged + 0.0001)
        
        self._all = [self.date,self.district_id,self.time_slot_id,
                    self.order_j,self.order_j_1,self.order_j_2,self.order_j_3,
                    self.order_averaged,self.p1,self.p2]
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

from tools import align_district_data,align_timeslot_data
if __name__ == '__main__':
    import time
    print time.asctime()
    conn,cur = get_database_connection('didi_data_training.sqlite3')
    data_file = open('data-order.csv','wb')
    data_file.write('date,start_district_id,time_slot_id,order_t(j),order_t(j-1),order_t(j-2),order_t(j-3),order_averaged,order_t(j)/order_t(j-1),order_t(j)/order_averaged\n')
    order_statics_sql = "select date,start_district_id,time_slot_id,count(*) \
                    from (select date,start_district_id,\
                    time_slot_id from Orders_Training group by date,\
                    start_district_id,time_slot_id,order_id_hash)\
                    group by date,start_district_id,time_slot_id;"
    gap_statics_sql = "select date,start_district_id,time_slot_id,count(*) as gap \
                        from (select date,start_district_id,time_slot_id \
                        from Orders_Training where driver_id_hash=='NULL' \
                        group by date,start_district_id,time_slot_id,order_id_hash)\
                        group by date,start_district_id,time_slot_id;"
    
    print 'query for order data...'
    fetch = cur.execute(order_statics_sql)
    order_result = fetch.fetchall()
    
    new_result = align_timeslot_data(order_result)
    new_result2 = align_district_data(new_result)    
    
    order_result = new_result2
    order_result.insert(0,['',0,0,0])
    order_result.insert(0,['',0,0,0])
    order_result.insert(0,['',0,0,0])
    
    print 'start iter results...'
    for item1,item2,item3,item4 in zip(order_result[3:],order_result[2:-1],
                                       order_result[1:-2],order_result[0:-3]):
        date,start_district_id,time_slot_id,order_count = item1
        _,_,_,order_count_j_1 = item2
        _,_,_,order_count_j_2 = item3
        _,_,_,order_count_j_3 = item4
        
        # special case
        if time_slot_id == 1:
            order_count_j_1 = 0
            order_count_j_2 = 0
            order_count_j_3 = 0
        elif time_slot_id == 2:
            order_count_j_2 = 0
            order_count_j_3 = 0
        elif time_slot_id == 3:
            order_count_j_3 = 0
            
        data = Data(date,start_district_id,time_slot_id,order_count,
                    order_count_j_1,order_count_j_2,order_count_j_3)
        data_file.write(data.to_csv())
        data_file.write('\n')
            
    data_file.close()
    close_connection(cur,conn)
    print time.asctime()
#statics_sql = "select count(*) from Orders_Training where date='2016-01-01' and\
#                start_district_id=1 and time_slot_id=11111"


