# -*- coding: utf-8 -*-
"""
Created on Fri May 27 22:53:42 2016

@author: tanfan.zjh
"""


class PriceData:
    def __init__(self,date,district_id,time_slot_id,
                 price_avg_j,price_avg_j_1,
                 price_avg_j_2,price_avg_j_3):
        self.date = date
        self.district_id = district_id
        self.time_slot_id = time_slot_id
        self.price_avg_j = price_avg_j
        self.price_avg_j_1 = price_avg_j_1
        self.price_avg_j_2 = price_avg_j_2
        self.price_avg_j_3 = price_avg_j_3
        
        self._all = [self.date,self.district_id,self.time_slot_id,
                     self.price_avg_j,self.price_avg_j_1,self.price_avg_j_2,
                     self.price_avg_j_3]
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
    data_file = open('data-price.csv','wb')
    data_file.write('date,start_district_id,time_slot_id,price_avg_t(j),price_avg_t(j-1),price_avg_t(j-2),price_avg_t(j-3)\n')
    price_statics_sql = "select date,start_district_id,time_slot_id,\
                        avg(price) as average_price from\
                        (select date,start_district_id,time_slot_id,\
                        price from Orders_Training group by date,\
                        start_district_id,time_slot_id,order_id_hash) \
                        group by date,start_district_id,time_slot_id;"
    
    print 'query for price data...'
    fetch = cur.execute(price_statics_sql)
    result = fetch.fetchall()
    
    new_result = align_timeslot_data(result)
    new_result2 = align_district_data(new_result)

    result = new_result2    
    result.insert(0,['',0,0,0])
    result.insert(0,['',0,0,0])
    result.insert(0,['',0,0,0])
    
    print 'start iter results...'
    for item1,item2,item3,item4 in zip(result[3:],result[2:-1],
                                       result[1:-2],result[0:-3]):
        date,start_district_id,time_slot_id,price_avg_j = item1
        _,_,_,price_avg_j_1 = item2
        _,_,_,price_avg_j_2 = item3
        _,_,_,price_avg_j_3 = item4
        
        # special case
        if time_slot_id == 1:
            price_avg_j_1 = 0
            price_avg_j_2 = 0
            price_avg_j_3 = 0
        elif time_slot_id == 2:
            price_avg_j_2 = 0
            price_avg_j_3 = 0
        elif time_slot_id == 3:
            price_avg_j_3 = 0
            
        data = PriceData(date,start_district_id,time_slot_id,
                         price_avg_j,price_avg_j_1,
                         price_avg_j_2,price_avg_j_3)
        data_file.write(data.to_csv())
        data_file.write('\n')
            
    data_file.close()
    close_connection(cur,conn)
    print time.asctime()
#statics_sql = "select count(*) from Orders_Training where date='2016-01-01' and\
#                start_district_id=1 and time_slot_id=11111"


