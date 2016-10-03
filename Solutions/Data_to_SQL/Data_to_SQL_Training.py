# Di-Tech Challenge 2016 Data Import Script
# Coded by Lei Mao and Jianhai Zhang
# Last revised on 5/26/2016
# Description:
# Before running this script, make sure that all the osx files are removed in all the data folders.
# Please also change the data file paths below

# Set data file paths here

# Folder paths
order_data_path = "D:/Di-Tech_Challenge/Processed_Data/citydata/season_1/training_data/order_data/"
weather_data_path = "D:/Di-Tech_Challenge/Processed_Data/citydata/season_1/training_data/weather_data"
traffic_data_path = "D:/Di-Tech_Challenge/Processed_Data/citydata/season_1/training_data/traffic_data"

# File paths
poi_data_path = "D:/Di-Tech_Challenge/Processed_Data/citydata/season_1/training_data/poi_data/poi_data"
district_hash_path = "D:/Di-Tech_Challenge/Processed_Data/citydata/season_1/training_data/cluster_map/cluster_map"

import sqlite3
import os
import re

conn = sqlite3.connect("didi_data_training.sqlite3")

# Generate district hash dictionary
hash_dict = {}
fhand = open(district_hash_path)
for line in fhand:
    line_splitted = line.strip().split('\t')
    hash_dict[line_splitted[0]] = line_splitted[1]
fhand.close()


# Import order_data to SQLite3

print "Import order_data to SQLite3 ..."

file_paths = []
file_names = []

for root, dirs, files in os.walk(order_data_path):
    for name in files:
        file_paths.append(os.path.join(root,name))
        # print os.path.join(root,name)
        name = "order_" + ''.join(name.split('_')[2].split('-'))
        file_names.append(name)
        # print name

cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS Orders_Training")
cur.execute("CREATE TABLE Orders_Training (order_id_hash TEXT, driver_id_hash TEXT, passenger_id_hash TEXT, start_district_hash TEXT, start_district_id INTEGER, dest_district_hash TEXT, dest_district_id INTEGER, price REAL, time_raw TEXT, date TEXT, time_minutes INTEGER, time_slot_id INTEGER)")

for i in range(len(file_names)):
    print "Import file %s to database ..." % file_names[i]
    fhand = open(file_paths[i])
    for line in fhand:
        # Strip all the non-chars at the beginning and end of the string
        line_splitted = line.strip().split('\t')
        if hash_dict.has_key(line_splitted[3]):
            start_district_id = hash_dict[line_splitted[3]]
        else:
            start_district_id = -1
        if hash_dict.has_key(line_splitted[4]):
            dest_district_id = hash_dict[line_splitted[4]]
        else:
            dest_district_id = -1
        date = line_splitted[6].split(' ')[0]
        time_minutes = int(line_splitted[6].split(' ')[1].split(':')[0]) * 60 + int(line_splitted[6].split(' ')[1].split(':')[1])
        time_slot_id = int(time_minutes/10) + 1
        cur.execute("INSERT INTO Orders_Training (order_id_hash, driver_id_hash, passenger_id_hash, start_district_hash, dest_district_hash, price, time_raw, start_district_id, dest_district_id, date, time_minutes, time_slot_id) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )", (line_splitted[0], line_splitted[1], line_splitted[2], line_splitted[3], line_splitted[4], line_splitted[5], line_splitted[6], start_district_id, dest_district_id, date, time_minutes, time_slot_id))
    conn.commit()
    fhand.close()
    print "Done"

cur.close()
print "All order_data files imported."



# Import weather_data to SQLite3

print "Import weather_data to SQLite3 ..."

file_paths = []
file_names = []

for root, dirs, files in os.walk(weather_data_path):
    for name in files:
        file_paths.append(os.path.join(root,name))
        # print os.path.join(root,name)
        name = "weather_" + ''.join(name.split('_')[2].split('-'))
        file_names.append(name)
        # print name

cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS Weather_Training")
cur.execute("CREATE TABLE Weather_Training (time_raw TEXT, weather_type INTEGER, temperature REAL, pm25 REAL, date TEXT, time_minutes INTEGER, time_slot_id INTEGER)")  

for i in range(len(file_names)):
    print "Import file %s to database ..." % file_names[i]
    fhand = open(file_paths[i])
    for line in fhand:
        # Strip all the non-chars at the beginning and end of the string
        line_splitted = line.strip().split('\t')
        date = line_splitted[0].split(' ')[0]
        time_minutes = int(line_splitted[0].split(' ')[1].split(':')[0]) * 60 + int(line_splitted[0].split(' ')[1].split(':')[1])
        time_slot_id = int(time_minutes/10) + 1
        cur.execute("INSERT INTO Weather_Training (time_raw, weather_type, temperature, pm25, date, time_minutes, time_slot_id) VALUES ( ?, ?, ?, ?, ?, ?, ? )", (line_splitted[0], line_splitted[1], line_splitted[2], line_splitted[3], date, time_minutes, time_slot_id))
    conn.commit()
    fhand.close()
    print "Done"

cur.close()
print "All weather_data files imported."



# Import traffic_data to SQLite3

print "Import traffic_data to SQLite3 ..."

file_paths = []
file_names = []

for root, dirs, files in os.walk(traffic_data_path):
    for name in files:
        file_paths.append(os.path.join(root,name))
        # print os.path.join(root,name)
        name = "traffic_" + ''.join(name.split('_')[2].split('-'))
        file_names.append(name)
        # print name

cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS Traffic_Training")
cur.execute("CREATE TABLE Traffic_Training (district_hash TEXT, tj_level_1 INTEGER, tj_level_2 INTEGER, tj_level_3 INTEGER, tj_level_4 INTEGER, time_raw TEXT, date TEXT, time_minutes INTEGER, time_slot_id INTEGER, district_id INTEGER)")

for i in range(len(file_names)):
    print "Import file %s to database ..." % file_names[i]
    fhand = open(file_paths[i])
    for line in fhand:
        # Strip all the non-chars at the beginning and end of the string
        line_splitted = line.strip().split('\t')
        if hash_dict.has_key(line_splitted[0]):
            district_id = hash_dict[line_splitted[0]]
        else:
            district_id = -1
        date = line_splitted[5].split(' ')[0]
        time_minutes = int(line_splitted[5].split(' ')[1].split(':')[0]) * 60 + int(line_splitted[5].split(' ')[1].split(':')[1])
        time_slot_id = int(time_minutes/10) + 1
        cur.execute("INSERT INTO Traffic_Training (district_hash, tj_level_1, tj_level_2, tj_level_3, tj_level_4, time_raw, date, time_minutes, time_slot_id, district_id) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )", (line_splitted[0], line_splitted[1].split(':')[1], line_splitted[2].split(':')[1], line_splitted[3].split(':')[1], line_splitted[4].split(':')[1], line_splitted[5], date, time_minutes, time_slot_id, district_id))
    conn.commit()
    fhand.close()
    print "Done"

cur.close()
print "All traffic_data files imported"


# Import poi_data to SQLite3

print "Import poi_data to SQLite3 ..."

poi_data_file = open(poi_data_path)
poi_data_lines = poi_data_file.readlines()
poi_data_file.close()

column_set = []
for line in poi_data_lines:
    toks = re.split('\\s',line.strip())
    for tok in toks[1:]:
        typex = tok.split(':')[0]
        if len(typex.split('#')) == 1:
            typex = typex + '#0'
        column_name = '[' + typex + ']'
        if column_name not in column_set:
            column_set.append(column_name)

cur = conn.cursor()

cur.execute("drop table if exists Poi_Data")

column_set_typex = [name+' int default 0' for name in column_set]

# district_hash

create_table_sql = "CREATE TABLE Poi_Data \
                    (district_hash text primary key unique not null," + \
                    ','.join(column_set_typex) + ', district_id)'
cur.execute(create_table_sql)


for line in poi_data_lines:
    toks = re.split('\\s',line.strip())
    district_hash = toks[0]

    if hash_dict.has_key(district_hash):
        district_id = hash_dict[district_hash]
    else:
        district_id = -1
    
    column_names = []
    data = []
    for tok in toks[1:]:
        tts = tok.split(':')
        if len(tts[0].split('#')) == 1:
            typex = tts[0] +'#0' 
        else:
            typex = tts[0]
        column_name = '[' + typex + ']'
        count = int(tts[1])
        column_names.append(column_name)
        data.append(count)
        
    placeholder = ['?' for i in column_names]
    placeholder.append('?')
    placeholder.append('?')
    insert_sql = "insert into Poi_Data (district_hash,"+','.join(column_names) \
                 + ', district_id) values (' + ','.join(placeholder) +')'
    data.insert(0,district_hash)
    data.append(district_id)
    result = cur.execute(insert_sql,data)

rr = cur.execute('select * from Poi_Data')
print len(rr.fetchall())

conn.commit()
cur.close()
conn.close()

print "All poi_data files imported"













'''

# Input cluster_map to SQLite3

print "Input cluster_map to SQLite3 ..."


print "Input file cluster_map to database ..."

cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS district_hash_value")
cur.execute("CREATE TABLE district_hash_value " + "(district_hash TEXT, district_id INTEGER)")

fhand = open(r"D:/Di-Tech_Challenge/Processed_Data/citydata/season_1/training_data/cluster_map/cluster_map")

for line in fhand:
    # Strip all the non-chars at the beginning and end of the string
    cur.execute("INSERT INTO district_hash_value " + "(district_hash, district_id) VALUES ( ?, ? )", line.strip().split('\t'))
conn.commit()
cur.close()
fhand.close()
print "Done"

print "All cluster_map file inputs completed"
'''