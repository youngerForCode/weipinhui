# -*- coding: utf-8 -*-
"""
特征提取：包括用户、商品、用户-商品对、品牌、用户-品牌的特征
"""
import pandas as pd


file_list = ['data/user_action_train.txt','data/goods_train.txt','data/user_action_test_items.txt']

#分块读取文件，返回一个DataFrame
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
            print 'iterator stoped, finish read file'
            
    df = pd.concat(chunks, ignore_index=True)
    return df
#fibonacci list 生成器
#开始想用他来做时间窗口的时间间隔，但是效果并不好
def fibs(iter_num):
    n, a , b = 0, 1, 2
    while n<iter_num:
        yield a
        a,b = b, a+b
        n += 1
    
#对start_day到end_day的用户-商品对，取data_start_day到start_day的所有用户的特征，
#用start_day到end_day的用户-商品对是否购买作为标签    
#day_list 是时间滑动窗口
def get_user_feat(start_day,end_day,data_start_day,day_list):
    actions = read_file(file_list[0])
    actions.columns = ['uid','spu_id','type','date']
    #根据data_start_day到start_day历史数据actions，统计得出start_day到end_day区域的特征feat
    user_feat = actions[(actions['date']>= start_day) & (actions['date']<=end_day)]
     
    actions = actions[(actions['date']>= data_start_day) & (actions['date']<start_day)]
#    df = pd.get_dummies(actions['type'])
#    actions = pd.concat([actions,df],axis =1)
#    actions.columns = ['uid','sup_id','type','date','click','buy']
    #时间滑动窗口 提取特此
#    user = actions[['uid', 'click', 'buy', 'date']]
    user = actions[['uid', 'type', 'date']]
    
    fib_gen = fibs(len(day_list))
    data_end_day = start_day
    for i ,moving_start_day in enumerate(day_list):
        day = user[(user['date'] <data_end_day) & (user['date'] >= moving_start_day)]
        day = day.drop('date', axis=1).groupby('uid',as_index=False)['type'].agg({'uclick':'count','ubuy':'sum'})
        day['u_ratio'] = day['buy']/day['click']
        dist_day ='_%d' %fib_gen.next()
        user_feat = pd.merge(user_feat,day,on='uid',how='left',suffixes=('',dist_day))
        
    return user_feat

def get_sub_user_feat(data_start_day,data_end_day,day_list):
    user_feat = read_file(file_list[2])
    user_feat = user_feat.drop(2, axis =1)
    user_feat.columns = ['uid', 'spu_id']
    
    actions = read_file(file_list[0])
    actions.columns = ['uid','spu_id','type','date']
     
    actions = actions[(actions['date']>= data_start_day) & (actions['date']<=data_end_day)]

    #时间滑动窗口 提取特此
    user = actions[['uid', 'type', 'date']]

    fib_gen = fibs(len(day_list))
    for i ,moving_start_day in enumerate(day_list):
        day = user[(user['date'] <= data_end_day) & (user['date'] >= moving_start_day)]
        day = day.drop('date', axis=1).groupby('uid',as_index=False)['type'].agg({'click':'count','buy':'sum'})
        day['u_ratio'] = day['buy']/day['click']
        dist_day ='_%d' %fib_gen.next()
        user_feat = pd.merge(user_feat,day,on='uid',how='left',suffixes=('',dist_day))
    
    return user_feat

#对start_day到end_day的用户-商品对，取data_start_day到start_day的所有商品的特征，
#用start_day到end_day的用户-商品对是否购买作为标签    
#day_list 是时间滑动窗口
def get_item_feat(start_day,end_day,data_start_day,day_list):
    actions = read_file(file_list[0])
    actions.columns = ['uid','spu_id','type','date']
    item_feat = actions[(actions['date']>= start_day) & (actions['date']<=end_day)]
     
    actions = actions[(actions['date']>= data_start_day) & (actions['date']<start_day)]

    item = actions[['spu_id', 'type', 'date']]
    
    fib_gen = fibs(len(day_list))
    data_end_day = start_day
    for i ,moving_start_day in enumerate(day_list):
        day = item[(item['date'] <data_end_day) & (item['date'] >= moving_start_day)]
        day = day.drop('date', axis=1).groupby('spu_id',as_index=False)['type'].agg({'iclick':'count','ibuy':'sum'})
        day['i_ratio'] = day['ibuy']/day['iclick']
        dist_day ='_%d' %fib_gen.next()
        item_feat = pd.merge(item_feat,day,on='spu_id',how='left',suffixes=('',dist_day))
        
    return item_feat

def get_sub_item_feat(data_start_day,data_end_day,day_list):
    item_feat = read_file(file_list[2])
    item_feat = item_feat.drop(2, axis =1)
    item_feat.columns = ['uid', 'spu_id']
    
    actions = read_file(file_list[0])
    actions.columns = ['uid','spu_id','type','date']
     
    actions = actions[(actions['date']>= data_start_day) & (actions['date']<=data_end_day)]

    #时间滑动窗口 提取特此
    item = actions[['spu_id', 'type', 'date']]

    fib_gen = fibs(len(day_list))
    for i ,moving_start_day in enumerate(day_list):
        day = item[(item['date'] <= data_end_day) & (item['date'] >= moving_start_day)]
        day = day.drop('date', axis=1).groupby('spu_id',as_index=False)['type'].agg({'iclick':'count','ibuy':'sum'})
        day['i_ratio'] = day['ibuy']/day['iclick']
        dist_day ='_%d' %fib_gen.next()
        item_feat = pd.merge(item_feat,day,on='spu_id',how='left',suffixes=('',dist_day))
    
    return item_feat

#对start_day到end_day的用户-商品对，取data_start_day到start_day的所有用户-商品的特征，
#用start_day到end_day的用户-商品对是否购买作为标签    
#day_list 是时间滑动窗口
def get_userItem_feat(start_day,end_day,data_start_day,day_list):
    actions = read_file(file_list[0])
    actions.columns = ['uid','spu_id','type','date']
    userItem_feat = actions[(actions['date']>= start_day) & (actions['date']<=end_day)]
     
    actions = actions[(actions['date']>= data_start_day) & (actions['date']<start_day)]

    userItem = actions[['uid','spu_id', 'type', 'date']]
    
    fib_gen = fibs(len(day_list))
    data_end_day = start_day
    for i ,moving_start_day in enumerate(day_list):
        day = userItem[(userItem['date'] <data_end_day) & (userItem['date'] >= moving_start_day)]
        day = day.drop('date', axis=1).groupby(['uid','spu_id'],as_index=False)['type'].agg({'uiclick':'count','uibuy':'sum'})
        day['ui_ratio'] = day['uibuy']/day['uiclick']
        dist_day ='_%d' %fib_gen.next()
        userItem_feat = pd.merge(userItem_feat,day,on=['uid','spu_id'],how='left',suffixes=('',dist_day))
        
    return userItem_feat

def get_sub_userItem_feat(data_start_day,data_end_day,day_list):
    userItem_feat = read_file(file_list[2])
    userItem_feat = userItem_feat.drop(2, axis =1)
    userItem_feat.columns = ['uid', 'spu_id']
    
    actions = read_file(file_list[0])
    actions.columns = ['uid','spu_id','type','date']
     
    actions = actions[(actions['date']>= data_start_day) & (actions['date']<=data_end_day)]

    #时间滑动窗口 提取特此
    userItem = actions[['uid','spu_id', 'type', 'date']]

    fib_gen = fibs(len(day_list))
    for i ,moving_start_day in enumerate(day_list):
        day = userItem[(userItem['date'] <= data_end_day) & (userItem['date'] >= moving_start_day)]
        day = day.drop('date', axis=1).groupby(['uid','spu_id'],as_index=False)['type'].agg({'uiclick':'count','uibuy':'sum'})
        day['ui_ratio'] = day['uibuy']/day['uiclick']
        dist_day ='_%d' %fib_gen.next()
        userItem_feat = pd.merge(userItem_feat,day,on=['uid','spu_id'],how='left',suffixes=('',dist_day))
    
    return userItem_feat
 
#对start_day到end_day的用户-商品对，取data_start_day到start_day的所有用户-品牌的特征，
#用start_day到end_day的用户-商品对是否购买作为标签    
#day_list 是时间滑动窗口
def get_userBrand_feat(start_day,end_day,data_start_day,day_list):
    actions = read_file(file_list[0])
    actions.columns = ['uid','spu_id','type','date']
    goods = read_file(file_list[1])
    goods.columns = ['spu_id','brand_id','cat_id']
    actions = pd.merge(actions,goods,on='spu_id',how='left')
    del goods
    #根据data_start_day到start_day历史数据actions，统计得出start_day到end_day区域的特征feat
    feat = actions[(actions['date']>= start_day) & (actions['date']<=end_day)]
    actions = actions[(actions['date']>= data_start_day) & (actions['date']<start_day)]    
     
    userBrand = actions[['uid','brand_id', 'type', 'date']]
    del actions
    
    fib_gen = fibs(len(day_list))
    data_end_day = start_day
    for i ,moving_start_day in enumerate(day_list):
        day = userBrand[(userBrand['date'] <data_end_day) & (userBrand['date'] >= moving_start_day)]
        day = day.drop('date', axis=1).groupby(['uid','brand_id'],as_index=False)['type'].agg({'ubclick':'count','ubbuy':'sum'})
        day['ub_ratio'] = day['ubbuy']/day['ubclick']
        dist_day ='_%d' %fib_gen.next()
        feat = pd.merge(feat,day,on=['uid','brand_id'],how='left',suffixes=('',dist_day))
        
    return feat

def get_sub_userBrand_feat(data_start_day,data_end_day,day_list):
    feat = read_file(file_list[2])
    feat = feat.drop(2, axis =1)
    feat.columns = ['uid', 'spu_id']
    
    actions = read_file(file_list[0])
    actions.columns = ['uid','spu_id','type','date']
    
    goods = read_file(file_list[1])
    goods.columns = ['spu_id','brand_id','cat_id']
   
    feat = pd.merge(feat,goods, on='spu_id',how='left')
    actions = pd.merge(actions,goods,on='spu_id',how='left')
    del goods 
    
    actions = actions[(actions['date']>= data_start_day) & (actions['date']<=data_end_day)]

    #时间滑动窗口 提取特此
    userBrand = actions[['uid','brand_id', 'type', 'date']]
    del actions 

    fib_gen = fibs(len(day_list))
    for i ,moving_start_day in enumerate(day_list):
        day = userBrand[(userBrand['date'] <= data_end_day) & (userBrand['date'] >= moving_start_day)]
        day = day.drop('date', axis=1).groupby(['uid','brand_id'],as_index=False)['type'].agg({'ubclick':'count','ubbuy':'sum'})
        day['ub_ratio'] = day['ubbuy']/day['ubclick']
        dist_day ='_%d' %fib_gen.next()
        feat = pd.merge(feat,day,on=['uid','brand_id'],how='left',suffixes=('',dist_day))
    
    return feat
#对start_day到end_day的用户-商品对，取data_start_day到start_day的所有品牌的特征，
#用start_day到end_day的用户-商品对是否购买作为标签    
#day_list 是时间滑动窗口
def get_brand_feat(start_day,end_day,data_start_day,day_list):
    actions = read_file(file_list[0])
    actions.columns = ['uid','spu_id','type','date']
    goods = read_file(file_list[1])
    goods.columns = ['spu_id','brand_id','cat_id']
    actions = pd.merge(actions,goods,on='spu_id',how='left')
    del goods
    #根据data_start_day到start_day历史数据actions，统计得出start_day到end_day区域的特征feat
    feat = actions[(actions['date']>= start_day) & (actions['date']<=end_day)]
    actions = actions[(actions['date']>= data_start_day) & (actions['date']<start_day)]    
     
    Brand = actions[['brand_id', 'type', 'date']]
    del actions
    
    fib_gen = fibs(len(day_list))
    data_end_day = start_day
    for i ,moving_start_day in enumerate(day_list):
        day = Brand[(Brand['date'] <data_end_day) & (Brand['date'] >= moving_start_day)]
        day = day.drop('date', axis=1).groupby(['brand_id'],as_index=False)['type'].agg({'bclick':'count','bbuy':'sum'})
        day['b_ratio'] = day['bbuy']/day['bclick']
        dist_day ='_%d' %fib_gen.next()
        feat = pd.merge(feat,day,on=['brand_id'],how='left',suffixes=('',dist_day))
        
    return feat

def get_sub_brand_feat(data_start_day,data_end_day,day_list):
    feat = read_file(file_list[2])
    feat = feat.drop(2, axis =1)
    feat.columns = ['uid', 'spu_id']
    
    actions = read_file(file_list[0])
    actions.columns = ['uid','spu_id','type','date']
    
    goods = read_file(file_list[1])
    goods.columns = ['spu_id','brand_id','cat_id']
   
    feat = pd.merge(feat,goods, on='spu_id',how='left')
    actions = pd.merge(actions,goods,on='spu_id',how='left')
    del goods 
    
    actions = actions[(actions['date']>= data_start_day) & (actions['date']<=data_end_day)]

    #时间滑动窗口 提取特此
    brand = actions[['brand_id', 'type', 'date']]
    del actions 

    fib_gen = fibs(len(day_list))
    for i ,moving_start_day in enumerate(day_list):
        day = brand[(brand['date'] <= data_end_day) & (brand['date'] >= moving_start_day)]
        day = day.drop('date', axis=1).groupby(['brand_id'],as_index=False)['type'].agg({'bclick':'count','bbuy':'sum'})
        day['b_ratio'] = day['bbuy']/day['bclick']
        dist_day ='_%d' %fib_gen.next()
        feat = pd.merge(feat,day,on=['brand_id'],how='left',suffixes=('',dist_day))
    
    return feat       
    
if __name__ == '__main__':
#    利用一下时间调用相关函数即可输出相应的特征文件
#    online_train_start_day = '03-25'
#    online_train_end_day = '03-31'
#    online_train_data_start_day = '01-10'
#    online_train_day_list = ['03-22','03-17', '03-01','02-24','01-10']

#    online_sub_data_start_day = '01-18'
#    online_sub_data_end_day = '03-31'
#    online_sub_day_list = ['03-29','03-26', '03-13','02-26','01-18']
#    
#    offline_train_start_day = '03-18'
#    offline_train_end_day = '03-24'
#    offline_train_data_start_day = '01-01'
#    offline_train_day_list = ['03-15','03-10', '02-23','02-10','01-01']
#    get_sub_brand_feat()
    pass
 
