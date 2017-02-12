#!/usr/bin/python
# coding: UTF-8

import sys
import sqlite3
import time
import re
import os.path
import tqdm

'''
python classification.py xxx.db yyy.log offset
'''

TIME_OFFSET = int(sys.argv[3])
MSG_OFFSET = TIME_OFFSET + 2

PARSE_CHAR = ['(',')','[',']','=']


#word split by space and parse char
def word_split(log):
    w = list(log)
    for (i,word) in enumerate(w):
        if word in PARSE_CHAR:
            w[i] = ' ' + w[i] + ' '
    w = ''.join(w)
    w = re.split(' +',w)
    if w[-1] == '':
        w = w[:-1]
    return w[MSG_OFFSET:]

#get format from db
#return: ft = [[group id, format]]
def get_ft():
    dbname = sys.argv[1]
    con = sqlite3.connect(dbname)
    cur = con.cursor()
    cur.execute("""select * from format""")
    data = cur.fetchall()
    data = [ [data[i][0],data[i][1].strip() ] for i in range(len(data)) ]
    con.commit()
    con.close()
    return data

#compare format and log
#return: 0 -> match, other -> not match
def compare_f(log,fmt):
    l = word_split(log)
    # print(l)
    f = fmt.split()
    if len(l) != len(f):#まず長さで評価
        return 1
    flag = 0
    for (lw,fw) in zip(l,f):
        if fw == '*':
            continue
        elif lw != fw:
            flag +=1
    return flag

#get time stamp(sec) from log
def get_time_sec(log):
    time_stamp = log.split()[TIME_OFFSET].split(':')
    time_sec = int(time_stamp[0])*60*60+int(time_stamp[1])*60+int(time_stamp[2])
    return time_sec

def sec2time(sec):
    return str(int(sec/3600)).zfill(2)+':'+str(int(sec%3600/60)).zfill(2)+':'+str(int(sec%3600%60)).zfill(2)

def insert_db(group_log_list,dbname):
    con = sqlite3.connect(dbname)
    cur = con.cursor()

    for group_id,logs in group_log_list.items():
        cur.execute("""drop table if exists '{0}' """.format(group_id))
        cur.execute("""create table if not exists '{0}' (id integer primary key,time integer,log text)""".format(group_id))

        for log in logs:
            msg = " ".join(log.split()[MSG_OFFSET:])
            time_stamp = get_time_sec(log)
            if "'" in msg:#エスケープ処理
                msg = msg.replace("'", "''")
            # print(msg)
            cur.execute("""insert into '{0}'(time,log) values ({1},'{2}');""".format(group_id,int(time_stamp),msg))
    con.commit()
    con.close()


if __name__ == '__main__':
    start_time = time.time()
    ft = get_ft()#ft = [[group id, format]]

    #log file open
    filename = sys.argv[2]
    fd = open(filename)
    log = fd.readline()

    filesize = os.path.getsize(filename)
    line_num = sum(1 for line in open(filename))

    match_cnt = 0


    #group_log_list: {group id : log}
    group_log_list = {k:[] for k in range(1,ft[-1][0]+1)}
    get_date_flag = 0
    # while log:
    for i in tqdm.tqdm(range(line_num)):
        log = log.strip()
        # print('log = ',log)

        #date 取得
        if get_date_flag == 0:
            month = log.split()[0]
            day = log.split()[1]
            # print(month,day)
            get_date_flag = 1

        #date 不一致のものは弾く
        if log.split()[0] != month or log.split()[1] != day:
            log = fd.readline()
            continue

        #テンプレートグループ比較
        for group_id,fmt in ft:
            d = compare_f(log,fmt)
            if d == 0:#合致したらgroup_log_list追加
                # print('match:',group_id,fmt,log)
                group_log_list[group_id].append(log)
                match_cnt += 1
                break

        log = fd.readline()

    fd.close()#raw log file close

    end_time = time.time()
    print('time:',end_time - start_time)
    print('match rate: {0}/{1} ({2} %)'.format(match_cnt,line_num,match_cnt/line_num*100))

    #insert classified logs to DB
    dbname = sys.argv[1]
    insert_db(group_log_list,dbname)


    #予備datファイル生成
    outputname = sys.argv[1].split('.')[0]+'.dat'
    fd = open(outputname,"w")

    for k in range(1,ft[-1][0]+1):
        fd.write('group {0}\n'.format(k))
        for log in group_log_list[k]:
            fd.write(log)
            fd.write('\n')

    fd.close()
