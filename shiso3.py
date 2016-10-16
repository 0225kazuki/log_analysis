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

'''
python shiso3.py dbname.db
shiso1段目のフォーマットをDBから取得して、ssdeepで類似度が出たものをまとめて、
同一dbのFormatスキーマに格納
'''

def get_ft():
    dbname = sys.argv[1]
    con = sqlite3.connect(dbname)
    cur = con.cursor()
    cur.execute("""select * from Node""")
    data = { row[0]:row[1] for row in cur.fetchall()}
    con.commit()
    con.close()
    return data


def create_db(super_ft_list,ft):
    dbname = sys.argv[1]
    con = sqlite3.connect(dbname)
    cur = con.cursor()
    cur.execute("""drop table if exists format""")
    cur.execute("""create table if not exists format (id integer,f text)""")
    format_id = 0
    for i in super_ft_list:
        format_id += 1
        for j in i:
            f = ft[j][0]
            if "'" in f:#エスケープ処理
                f = f.replace("'", "''")
            cur.execute("""insert into format values ('{0}','{1}');""".format(format_id,f))
    con.commit()
    con.close()


ft = get_ft()
cnt_list = [0]*len(ft)

#全テンプレートのハッシュを計算
for i,f in ft.items():
    h = ssdeep.hash(f)
    ft[i] = (f,h)

#ft = {id : (format,hash)}

#hash_dist_listは、各テンプレート間の距離を格納
hash_dist_list = {}
for i,f in ft.items():
    hlist = []
    for j,g in ft.items():
        if i == j :
            continue
        d = ssdeep.compare(f[1],g[1])
        if d > 0 :
            hlist.append(j)
    hash_dist_list[i] = tuple(hlist)

#間接的に依存関係にあるものもまとめる
for i,a in hash_dist_list.items():
    if a == ():#すでに削除されたものだった場合は飛ばす
        continue
    a = set(a)#検索元を集合化
    a.add(i)#自分自身が入ってないので追加する。
    for j in range(i+1,len(hash_dist_list)):
        b = set(hash_dist_list[j])#自分より後ろのインデックスのものと集合比較
        if a & b:
            a = a | b
            hash_dist_list[j] = ()#共通部分があれば自分に取り込んで相手を消す
    hash_dist_list[i] = tuple(a)#すべての検索を終えたら自分の値を戻す。
hash_dist_list = [x for x in hash_dist_list.values() if x != () ]#最後に空リスト部分を消す


create_db(hash_dist_list,ft)
