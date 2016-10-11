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


def get_ft():
    dbname = sys.argv[1]
    con = sqlite3.connect(dbname)
    cur = con.cursor()
    cur.execute("""select * from Node""")
    data = cur.fetchall()
    data = [ [data[i][0],data[i][1],data[i][2]] for i in range(len(data)) ]
    con.commit()
    con.close()
    return data


def create_db(super_ft_list,ft):
    dbname = sys.argv[1]
    con = sqlite3.connect(dbname)
    cur = con.cursor()
    cur.execute("""drop table if exists format""")
    cur.execute("""create table if not exists format (id integer,f text,cnt integer)""")
    format_id = 0
    for i in super_ft_list:
        format_id += 1
        for j in i:
            # print(format_id,ft[j-1])
            # for (i,word) in enumerate(format_list):#エスケープ処理
            if "'" in ft[j-1][1]:
                ft[j-1][1] = ft[j-1][1].replace("'", "''")
            cur.execute("""insert into format values ('{0}','{1}','{2}');""".format(format_id,ft[j-1][1],ft[j-1][2]+1))
    con.commit()
    con.close()


ft = get_ft()
tr = 0.5
ts = 0.5
r_max = 0
f_max = None
cnt_list = [0]*len(ft)


#全テンプレートのハッシュを計算
for f in ft:
    h = ssdeep.hash(f[1])
    ft[f[0]-1].append(h)

hash_dist_list = []

#ftは、[id,format,cnt,hash]に変換されている

#hash_dist_listは、各テンプレート間の距離を格納
for f in ft:
    hlist = []
    for g in ft:
        d = ssdeep.compare(f[3],g[3])
        hlist.append(d)
    hash_dist_list.append(hlist)

ft_d = {k:v for (k,v,x,y) in ft}

super_ft_list = []

#ここから類似なものをまとめていく
for i in range(len(ft_d)):
    i += 1#i はidと一致。
    if ft_d[i] == None:
        continue
    s_list = [i]#自分のナンバーを入れる
    for (j,d) in enumerate(hash_dist_list[i-1]):#自分と他の距離を持ってくる
        if d > 0 and i != (j+1):
            s_list.append(j+1)
            ft_d[j+1] = None
    ft_d[i] = None
    super_ft_list.append(s_list)

for i in range(len(super_ft_list)):
    print(i+1,super_ft_list[i])

#間接的に依存関係にあるものもまとめる
for i in range(len(super_ft_list)):
    a = set(super_ft_list[i])#検索元を集合化
    for j in range(i+1,len(super_ft_list)):
        b = set(super_ft_list[j])#自分より後ろのインデックスのものと集合比較
        if a & b:
            a = a | b
            super_ft_list[j] = []#共通部分があれば自分に取り込んで相手を消す
    super_ft_list[i] = list(a)#すべての検索を終えたら自分の値を戻す。
super_ft_list = [x for x in super_ft_list if x != []]#最後に空リスト部分を消す

for i in range(len(super_ft_list)):
    print(i+1,super_ft_list[i])

# for i in super_ft_list:
#     for j in i:
#         print(ft[j-1])
#     print('\n')

create_db(super_ft_list,ft)
