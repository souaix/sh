import sys
import datetime
import pymssql
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import pyodbc
import requests
import time
import logging

import warnings
warnings.filterwarnings('ignore')

# sys.path.append(r'C:\Users\User\Desktop\python')
sys.path.append('/home/cim')

#logging
import global_fun.logging_fun as logfun

# connector
import connect.connect as cc

eng_mes = cc.connect('MES', 'MES_Production')
eng_ris = cc.connect('RIS', 'RIS_PRD')

con_mes = eng_mes.connect()
cur = eng_ris.cursor()

DB = "MES_Production"
# DB = "MES_Test"

  
def sql_str(table, cols):
    sql = "INSERT INTO "+table + "("

    cols_str = ""
    for i, v in enumerate(cols):
        sql = sql + v + ","
    sql = sql[0:-1]+") VALUES("

    for i, v in enumerate(cols):
        if "DATE" in str(v) and "DATECODE" not in str(v):
            sql = sql + "to_date(:" + v + ",'YYYY-MM-DD HH24:mi:ss'),"
        else:
            sql = sql + ":" + v + ","
    sql = sql[0:-1]+")"

    return sql


def sql_val(sql, df, cur, con_sap):

    for j in range(0, len(df["LOTNO"])):
        bindVar = {}
        for i, v in enumerate(df):
            if(str(df[v][j])=="NaT" or str(df[v][j])=="nan" or str(df[v][j])=="None"):
                b = {str(v): ''}
            else:
                b = {str(v): str(df[v][j])}
            bindVar.update(b)
#         print(sql)
#         print(bindVar)
        cur.execute(sql, bindVar)
        con_sap.commit()
        
def str_combine(arr):
    arr_str=''
    for i,v in enumerate(arr):
        arr_str = arr_str + "'"+str(v)+"',"
    
    arr_str = arr_str[:-1]
    return arr_str


#開啟log
logfun.set_logging('/home/cim/sh/RIS_PRODUCTLABELDATA')

logging.debug('----------------------------------------------------------')
logging.info('Start at - ' + datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))

# 撈出TRIGGER LOG
sql = "SELECT * FROM  "+DB+".dbo.TH_PRODUCTLABELDATA_RECORD"
df = pd.read_sql(sql, eng_mes)

if(len(df)>0):
    df_del = df[df["TYPE_DESC"]=="UPDATE-DEL"]
    df_ins = df[df["TYPE_DESC"]=="UPDATE-INS"]
    df_insert = df[df["TYPE_DESC"]=="INSERT"]

    #刪除RIS資料用
    del_lot = df_del["LOTNO"].tolist()
    del_lot_str = str_combine(del_lot)
    #刪除MES資料用
    del_sn = df_del["SN"].tolist()
    ins_sn = df_ins["SN"].tolist()
    insert_sn = df_insert["SN"].tolist()
    del_sn1_str = str_combine(del_sn)
    del_sn2_str = str_combine(ins_sn)
    del_sn3_str = str_combine(insert_sn)

    #避免重複資料，及insert筆數合併
    df_del.drop_duplicates(subset=['LOTNO'],keep='last',inplace=True)
    df_ins_ = pd.concat([df_ins,df_insert])
    df_ins_.drop_duplicates(subset=['LOTNO'],keep='last',inplace=True)

    #去除多餘欄位
    del df_ins_["MODIFY_TIME"]
    del df_ins_["TYPE_DESC"]
    del df_ins_["SN"]
    del df_ins_["RONO"]
    del df_ins_["COMPONENTNO"]
    df_ins_.rename(columns={"QTY":"LOT_QTY"}, inplace=True)


    df_ins_.reset_index(drop=True,inplace=True)
    df_insert.reset_index(drop=True,inplace=True)


    try:
        if(del_lot_str!=''):
            #刪RIS
            logging.info('DEL UPDATE-DEL RIS LOT : '+ datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")) 
            sql = "DELETE FROM MES.MES_PRODUCTLABELDATA_NEW WHERE LOTNO in("+del_lot_str+")"
            cur.execute(sql)
            eng_ris.commit()
        else:
            logging.info('NO UPDATE-DEL DATA') 
            
        if(len(df_ins_)>0):
            #新增RIS
            logging.info('INS UPDATE-INS RIS LOT : '+ datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")) 
            cols = df_ins_.columns.tolist()
            sql = sql_str('MES.MES_PRODUCTLABELDATA_NEW', cols)
            sql_val(sql, df_ins_, cur, eng_ris)
        else:
            logging.info('NO UPDATE-INS DATA')             

        #刪MES
        if(del_sn1_str!=''):
            logging.info('DEL UPDATE-DEL MES SN : '+ datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
            sql = "DELETE FROM "+DB+".dbo.TH_PRODUCTLABELDATA_RECORD WHERE SN IN("+del_sn1_str+")"
            con_mes.execute(text(sql))
            con_mes.commit()
        else:
            logging.info('NO UPDATE-DEL DATA')            

        #    eng_mes.execute(sql)
        if(del_sn2_str!=''):        
            logging.info('DEL UPDATE-INS MES SN : '+ datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
            sql = "DELETE FROM "+DB+".dbo.TH_PRODUCTLABELDATA_RECORD WHERE SN IN("+del_sn2_str+")"
            con_mes.execute(text(sql))
            con_mes.commit()
        else:
            logging.info('NO UPDATE-INS DATA')            


        if(del_sn3_str!=''):        
            logging.info('DEL INSERT MES SN : '+ datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
            sql = "DELETE FROM "+DB+".dbo.TH_PRODUCTLABELDATA_RECORD WHERE SN IN("+del_sn3_str+")"

            con_mes.execute(text(sql))
            con_mes.commit()            
        else:
            logging.info('NO INSERT DATA')            
            
        logging.info('END at - '+ datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
        logging.debug('----------------------------------------------------')
    except Exception as E:
        logging.debug("UPDATE FAIL : "+str(E))
else:
    logging.info('Theres no record to update')    
    logging.info('End at - ' + datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
    logging.debug('----------------------------------------------------')
