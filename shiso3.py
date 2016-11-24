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
python shiso3.py dbname.db
shiso1段目のフォーマットをDBから取得して、ssdeepで類似度が出たものをまとめて、
同一dbのFormatスキーマに格納
'''

def get_ft():
    dbname = sys.argv[1]
    con = sqlite3.connect(dbname)
    cur = con.cursor()
    cur.execute("""select distinct f from Node""")
    data = { i:row[0] for i,row in enumerate(cur.fetchall(),start = 1)}
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


#0で一致、0以外で不一致
def compare_f(format_group1,format_group2):
    '''
    パラメータ数が一番多い物を代表フォーマットとして比較する。
    アスタリスクが定数中に入ってくるものもあるので、単語パースしてカウント
    ex: sshd [ * ] : Invalid user ***** from 52.163.89.175
    '''
    parameter_count = {collections.Counter(ft[x][0].split())['*']:x for x in format_group1}
    fmt1 = ft[parameter_count[max(parameter_count)]][0].split()
    parameter_count = {collections.Counter(ft[x][0].split())['*']:x for x in format_group2}
    fmt2 = ft[parameter_count[max(parameter_count)]][0].split()
    if len(fmt1) != len(fmt2):
        return 1
    flag = 0
    for (w1,w2) in zip(fmt1,fmt2):
        if w1 == '*' or w2 == '*':
            continue
        elif w1 != w2:
            flag +=1
    return flag

if __name__ == '__main__':

    ft = get_ft()
    format_num = len(ft)
    cnt_list = [0]*len(ft)


    #全テンプレートのハッシュを計算
    start_time = time.time()
    for i,f in ft.items():
        h = ssdeep.hash(f)
        ft[i] = [f,h]
    end_time = time.time()
    print('Hash calc:',end_time - start_time)

    #ft = {id : (format,hash)}



    '''
    2016/11/01
    重複を削除するときの問題点

    id 5と96が関連ありで、5のdist listに96が入る。
    id 10と96が重複していた->96が削除される
    id 5のdist listの96を10に置き換えないといけない

    '''

    #hash_dist_listは、各テンプレート間の距離を格納
    start_time = time.time()
    hash_dist_list = {}
    for i,f in ft.items():
        hlist = []
        if f == []:
            continue
        for j,g in ft.items():
            if i == j or g == []:
                continue
            d = ssdeep.compare(f[1],g[1])
            '''重複はDB取得の時点で弾いた'''
            # if d == 100:#重複しているものはここで弾く
            #     print('dup hit')
            #     ft[j] = []
            #     continue
            if d > 0 :
                hlist.append(j)
        hash_dist_list[i] = tuple(hlist)
    end_time = time.time()
    print('Hash dist calc:',end_time - start_time)

    #間接的に依存関係にあるものもまとめる
    start_time = time.time()
    for i in range(1,format_num+1):
        if hash_dist_list.get(i,0) == 0:
            continue
        a = hash_dist_list[i]
        if a == ():#依存関係なしのものは自分を入れて終わり
            hash_dist_list[i] = (i,)
            continue
        a = set(a)#検索元を集合化
        a.add(i)#自分自身が入ってないので追加する。
        for j in range(i+1,format_num+1):
            if hash_dist_list.get(j,0) == 0:
                continue
            b = set(hash_dist_list[j])#自分より後ろのインデックスのものと集合比較
            if a & b:
                a = a | b
                hash_dist_list.pop(j)#共通部分があれば自分に取り込んで相手を消す
        hash_dist_list[i] = tuple(a)#すべての検索を終えたら自分の値を戻す。
    hash_dist_list = {i:x for i,x in enumerate(hash_dist_list.values(),start = 1) if x != () }#最後に空リスト部分を消す
    end_time = time.time()
    print('Hash dup calc:',end_time - start_time)


    hash_dist_list_len = len(hash_dist_list)
    start_time = time.time()
    for i in range(1,hash_dist_list_len + 1):
        if hash_dist_list.get(i,0) == 0 :
            continue
        format_group1 = hash_dist_list[i]
        for j in range(i+1,hash_dist_list_len + 1):
            if hash_dist_list.get(j,0) == 0 :
                continue
            format_group2 = hash_dist_list[j]
            if not compare_f(format_group1,format_group2):
                hash_dist_list[i] = format_group1 + format_group2
                format_group1 = hash_dist_list[i]
                hash_dist_list.pop(j)
    end_time = time.time()
    print('refine format:',end_time - start_time)

    hash_dist_list = [x for x in hash_dist_list.values() if x != () ]
    create_db(hash_dist_list,ft)
