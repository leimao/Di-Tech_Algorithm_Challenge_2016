# -*- coding: utf-8 -*-
"""
Created on Mon May 30 08:27:38 2016

@author: tanfan.zjh
"""

## for gap/order/price data
def align_district_data(result):
    # align district data
    new_result = []
    last_start_district_id = 0
    last_date = ''
    for item in result:
        date,start_district_id,time_slot_id,gap_count = item
        district_interval = start_district_id - last_start_district_id
        if district_interval == 1 or district_interval == 0 or\
           district_interval == -65:
            new_result.append(item)
        elif district_interval > 1:
            for i in xrange(1,district_interval):
                for j in xrange(1,145):
                    item_null = (date,start_district_id + i,j,0)
                    new_result.append(item_null)
            new_result.append(item)
        elif district_interval < 0 and district_interval != -65:
            for i in xrange(last_start_district_id+1,67):
                for j in xrange(1,145):
                    item_null = (last_date, i, j, 0)
                    new_result.append(item_null)
            for i in xrange(1,start_district_id):
                for j in xrange(1,145):
                    item_null = (date, i, j, 0)
                    new_result.append(item_null)
            new_result.append(item)
        else:
            print 'error2'
            #print district_interval,start_district_id,last_start_district_id
        last_start_district_id = start_district_id
        last_date = date
    
    # process for last data item
    date,start_district_id,time_slot_id,gap_count = item
    for i in xrange(start_district_id + 1, 67):
        for j in xrange(1,145):
            item_null = (date, i, j, 0)
            new_result.append(item_null)
    return new_result

## for gap/order/price data
def align_timeslot_data(result):
    new_result = []
    last_time_slot_id = 0
    last_district_id = 0
    last_date = ''
    # align time slot data
    for item in result:
        date,start_district_id,time_slot_id,gap_count = item
        interval_timeslot = time_slot_id - last_time_slot_id
        if interval_timeslot == 1 or interval_timeslot == -143:
            new_result.append(item)
        elif interval_timeslot > 1:
            for i in xrange(last_time_slot_id+1,time_slot_id):
                item_null = (date,start_district_id,i,0)
                new_result.append(item_null)
            new_result.append(item)
        elif interval_timeslot < 0 and interval_timeslot != -143:
            for i in xrange(last_time_slot_id+1,145):
                item_null = (last_date,last_district_id,i,0)
                new_result.append(item_null)
            for i in xrange(1,time_slot_id):
                item_null = (date,start_district_id,i,0)
                new_result.append(item_null)
            new_result.append(item)
        else:
            print 'error'
        last_time_slot_id = time_slot_id    
        last_district_id = start_district_id
        last_date = date
    
    #process for last data item
    date,start_district_id,time_slot_id,gap_count = new_result[-1]
    for i in xrange(time_slot_id+1,145):
        item_null = (date,start_district_id,i,0)
        new_result.append(item_null)
    return new_result
    
## for weather data
def align_weather_district_data(result):
    # align district data
    new_result = []
    tmp_list = []
    for item in result:
        time_slot_id_ = item[1]
        date = item[0]
        tmp_list.append(item)
        if time_slot_id_ == 144:
            for district_id in xrange(1,67):
                for item2 in tmp_list:
                    date,time_slot_id,weather_type,temperature,pm = item2
                    item_new = date,district_id,time_slot_id,\
                                weather_type,temperature,pm
                    new_result.append(item_new)
            tmp_list = []
    return new_result

# for weather data
def align_weather_timeslot_data(result):
    new_result = []
    last_time_slot_id = 0
    last_date = ''
    for item in result:
        date,time_slot_id,weather_type,temperature,pm = item
        interval_timeslot = time_slot_id - last_time_slot_id
        if interval_timeslot == 1 or interval_timeslot == -143:
            new_result.append(item)
        elif interval_timeslot > 1:
            for i in xrange(last_time_slot_id+1,time_slot_id):
                item_null = (date,i,weather_type,temperature,pm)
                new_result.append(item_null)
            new_result.append(item)
        elif interval_timeslot < 0 and interval_timeslot != -143:
            for i in xrange(last_time_slot_id+1,145):
                item_null = (last_date,i,weather_type,temperature,pm)
                new_result.append(item_null)
            for i in xrange(1,time_slot_id):
                item_null = (date,i,weather_type,temperature,pm)
                new_result.append(item_null)
            new_result.append(item)
        else:
            print 'error'
        last_time_slot_id = time_slot_id  
        last_date = date
    date,time_slot_id,weather_type,temperature,pm = new_result[-1]
    for i in xrange(time_slot_id+1,145):
        item_null = (date,i,weather_type,temperature,pm)
        new_result.append(item_null)
    return new_result

# for traffic data
def align_timeslot_traffic_data(result):
    new_result = []
    last_time_slot_id = 0
    last_district_id = 0
    last_date = ''
    # align time slot data
    for item in result:
        date,start_district_id,time_slot_id,tj_1,tj_2,tj_3,tj_4 = item
        interval_timeslot = time_slot_id - last_time_slot_id
        if interval_timeslot == 1 or interval_timeslot == -143:
            new_result.append(item)
        elif interval_timeslot > 1:
            for i in xrange(last_time_slot_id+1,time_slot_id):
                item_null = (date,start_district_id,i,tj_1,tj_2,tj_3,tj_4)
                new_result.append(item_null)
            new_result.append(item)
        elif interval_timeslot < 0 and interval_timeslot != -143:
            for i in xrange(last_time_slot_id+1,145):
                item_null = (last_date,last_district_id,i,tj_1,tj_2,tj_3,tj_4)
                new_result.append(item_null)
            for i in xrange(1,time_slot_id):
                item_null = (date,start_district_id,i,tj_1,tj_2,tj_3,tj_4)
                new_result.append(item_null)
            new_result.append(item)
        else:
            print 'error'
        last_time_slot_id = time_slot_id  
        last_date = date
        last_district_id = start_district_id
    
    #process for last data item
    date,start_district_id,time_slot_id,tj_1,tj_2,tj_3,tj_4 = new_result[-1]
    for i in xrange(time_slot_id+1,145):
        item_null = (date,start_district_id,i,tj_1,tj_2,tj_3,tj_4)
        new_result.append(item_null)
    return new_result

# for traffic data
def align_district_traffic_data(result):
    # align district data
    new_result = []
    last_start_district_id = 0
    last_date = ''
    for item in result:
        date,start_district_id,time_slot_id,tj_1,tj_2,tj_3,tj_4 = item
        district_interval = start_district_id - last_start_district_id
        if district_interval == 1 or district_interval == 0 or\
           district_interval == -65:
            new_result.append(item)
        elif district_interval > 1:
            for i in xrange(1,district_interval):
                for j in xrange(1,145):
                    item_null = (date,start_district_id + i,j,tj_1,tj_2,tj_3,tj_4)
                    new_result.append(item_null)
            new_result.append(item)
        elif district_interval < 0 and district_interval != -65:
            for i in xrange(last_start_district_id+1,67):
                for j in xrange(1,145):
                    item_null = (last_date, i, j, tj_1,tj_2,tj_3,tj_4)
                    new_result.append(item_null)
            for i in xrange(1,start_district_id):
                for j in xrange(1,145):
                    item_null = (date, i, j, tj_1,tj_2,tj_3,tj_4)
                    new_result.append(item_null)
            new_result.append(item)
        else:
            print 'error2'
            #print district_interval,start_district_id,last_start_district_id
        last_start_district_id = start_district_id
        last_date = date
    
    # process for last data item
    date,start_district_id,time_slot_id,tj_1,tj_2,tj_3,tj_4 = item
    for i in xrange(start_district_id + 1, 67):
        for j in xrange(1,145):
            item_null = (date, i, j, tj_1,tj_2,tj_3,tj_4)
            new_result.append(item_null)
    return new_result
    
## not used tmp
def align_poi_data(result):
    result_new = []
    last_catagory = 0
    for item in result:
        district_id,catagory,number = item
        interval = catagory - last_catagory
        if interval == 1 or interval == -24:
            result_new.append(item)
        elif interval > 1:
            for i in xrange(last_catagory + 1, catagory):
                item_null = district_id,i,0
                result_new.append(item_null)
            result_new.append(item)
        elif interval < 0 and interval != -24:
            for i in xrange(last_catagory+1,26):
                item_null = district_id,i,0
                result_new.append(item_null)
            for i in xrange(1,catagory):
                item_null = district_id,i,0
                result_new.append(item_null)
            result_new.append(item)
        last_catagory = catagory
    district_id,catagory,number = result_new[-1]
    for i in xrange(catagory+1,26):
        item_null = district_id,catagory,0
        result_new.append(item)
    return result_new