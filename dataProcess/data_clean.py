# -*- coding: utf-8 -*-
"""
Created on Sat Jun 24 11:22:24 2017

@author: elon
"""
import numpy as np
import pandas as pd

user_action_train_file = "../data/user_action_train.txt"
goods_train_file = "../data/goods_train.txt"
part_user_action_train_file = "part_user_action_train.txt"
part_goods_train_file = "part_goods_train.txt"
user_table_file = "user_table.csv"
goods_table_file = "goods_table.csv"
test_file = '../data/user_action_test_items.txt'

fileList = [user_action_train_file, goods_train_file,
            part_user_action_train_file, part_goods_train_file]

def extra_part_data():    
    n_samples = 10000
    with open(fileList[0],"rb") as fi:
        with open(fileList[2],"wb") as fo:
            for i in xrange(n_samples):
                fo.write( fi.readline())
    with open(fileList[1],"rb") as fi:
        with open(fileList[3],"wb") as fo:
            for i in xrange(n_samples):
                fo.write( fi.readline())

def read_file(file_name, chunk_size=100000):
    chunks = []
    reader = pd.read_csv(file_name, sep='\t', header=None,iterator=True)
    loop = True
    while loop:
        try:
            chunk = reader.get_chunk(chunk_size)
            chunks.append(chunk)
        except:
            loop = False
            print "iterator stoped"
    df = pd.concat(chunks, ignore_index=True)
    return df;
    

#　对user_action数据分解成用户和商品的统计数据
# 根据自己的需求调节chunk_size大小
def get_from_action_data(fname, chunk_size=100000):
    chunks = []
    names = ['uid', 'spu_id', 'action_type', 'date']
    reader = pd.read_csv(fname, sep='\t', header=None,names=names,iterator=True)
    loop = True
    while loop:
        try:
            chunk = reader.get_chunk(chunk_size)
            chunks.append(chunk)
        except:
            loop = False
            print "iterator stoped"
    
    df_ca = pd.concat(chunks, ignore_index=True)
    
    groupbytemp = df_ca.groupby('uid')
    user_data = groupbytemp['action_type'].agg({'click_num':'count','buy_num':'sum'}).reset_index()
    user_data['buy_click_ratio'] = user_data['buy_num']/user_data['click_num']

    groupbytemp = df_ca.groupby('spu_id')
    goods_data = groupbytemp['action_type'].agg({'click_num':'count','buy_num':'sum'}).reset_index()
    goods_data['buy_click_ratio'] = goods_data['buy_num']/goods_data['click_num']
    return [user_data, goods_data]
 
def get_from_goods_data(file_name, chunk_size=100000):    
    df_ca = read_file(file_name, chunk_size)
    columns = ['spu_id', 'brand_id', 'cat_id']
    df_ca.columns = columns
    return df_ca;
def merge_data_from_userAction_goods(userFile, goodsFile):
    user_behavior, goods_behavior= get_from_action_data(userFile)
    goods_base = get_from_goods_data(goodsFile)
    goods_all = pd.merge(goods_base, goods_behavior, on=['spu_id'], how='left')
    goods_all.to_csv(goods_table_file, index=False)
    user_behavior.to_csv(user_table_file, index=False)  
    
    
if __name__ == "__main__":
#    extra_part_data()
#    merge_data_from_userAction_goods(fileList[0], fileList[1])
    pass