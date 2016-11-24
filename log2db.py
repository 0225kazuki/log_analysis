#!/usr/bin/python
# coding: UTF-8

import collections
import pprint
import re
import sys
import numpy as np
import sqlite3
import ssdeep
import Levenshtein
import time


'''
python shiso3.py xxx.dat yyy.db offset
datをDBに格納
yyyは任意入力
'''

TIME_OFFSET = int(sys.argv[3])
MSG_OFFSET = TIME_OFFSET + 2

filename = sys.argv[1]
fd = open(filename,"r")

'''
Time Stampの取得(秒換算)
'''
def import_dat():
    logs = {}
    group_id = 0
    for i in fd.readlines():#1行ずつ読み込む
        line = i.split()
        if line[0] == 'group':#group が来るたびにグループ分け
            group_id = int(line[1])
            logs[group_id] = {}
        else:
            time_stamp = line[TIME_OFFSET].split(':')
            time_sec = int(time_stamp[0])*60*60+int(time_stamp[1])*60+int(time_stamp[2])
            if logs[group_id] != {} and logs[group_id].get(time_sec,-1) != -1:#秒単位でも重複がある場合は少数3桁でカウントしていく
                time_sec = round(max(logs[group_id].keys()) + 0.001,3)
            logs[group_id][time_sec] = line[MSG_OFFSET:]
    fd.close()
    return logs


'''
logs = { temp id : { time_sec : log}}
'''

def create_db(logs):
    dbname = sys.argv[2]
    con = sqlite3.connect(dbname)
    cur = con.cursor()
    for format_id,log in logs.items():
        cur.execute("""drop table if exists '{0}' """.format(format_id))
        cur.execute("""create table if not exists '{0}' (id integer primary key,time integer,log text)""".format(format_id))
        for time_stamp,msg in log.items():
            msg = " ".join(msg)
            if "'" in msg:#エスケープ処理
                msg = msg.replace("'", "''")
            print(msg)
            cur.execute("""insert into '{0}'(time,log) values ({1},'{2}');""".format(format_id,int(time_stamp),msg))
    con.commit()
    con.close()

if __name__ == '__main__':
    logs = import_dat()
    create_db(logs)
