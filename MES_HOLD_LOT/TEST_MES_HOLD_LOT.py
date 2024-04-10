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
#email
import global_fun.thmail_fun as thmail

# connector
import connect.connect as cc

eng_mes = cc.connect('MES', 'MES_Test')
eng_ris = cc.connect('RIS', 'RIS_TEST')

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
        if "TIME" in str(v) :
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
        #print('----')
        #print(bindVar)
        
        cur.execute(sql, bindVar)
        con_sap.commit()
        
def str_combine(arr):
    arr_str=''
    for i,v in enumerate(arr):
        arr_str = arr_str + "'"+str(v)+"',"
    
    arr_str = arr_str[:-1]
    return arr_str


#開啟log
logfun.set_logging('/home/cim/log/MES_HOLD_LOT')

logging.debug('----------------------------------------------------------')
logging.info('Start at - ' + datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))

# 撈出TRIGGER LOG
sql = "SELECT * FROM  "+DB+".dbo.TH_MES_HOLD_LOT_RECORD"
df = pd.read_sql(sql, eng_mes)

if(len(df)>0):
    df_del = df[df["TYPE_DESC"]=="UPDATE-DEL"]
    df_ins = df[df["TYPE_DESC"]=="UPDATE-INS"]
    df_insert = df[df["TYPE_DESC"]=="INSERT"]
    df_delete = df[df["TYPE_DESC"]=="DELETE"]
    
    #刪除MES資料用 by SN
    del_sn = df_del["SN"].tolist()
    ins_sn = df_ins["SN"].tolist()
    insert_sn = df_insert["SN"].tolist()
    delete_sn = df_delete["SN"].tolist()
    
    del_sn1_str = str_combine(del_sn)
    del_sn2_str = str_combine(ins_sn)
    del_sn3_str = str_combine(insert_sn)
    del_sn4_str = str_combine(delete_sn)
    
    #避免del重複資料
    df_del.drop_duplicates(subset=['LOTNO','HOLD_NO','HOLD_CODE','HOLD_EQ_NAME'],keep='last',inplace=True)
    df_delete.drop_duplicates(subset=['LOTNO','HOLD_NO','HOLD_CODE','HOLD_EQ_NAME'],keep='last',inplace=True)
    
    #刪除RIS資料用 by LOT
    del_lot = df_del["HOLD_NO"].tolist()

    #in list > oracle不接受超過1000筆，須拆分
    if(len(del_lot)/1000>1):
        
        import math
        parti = math.ceil(len(del_lot)/1000)
        splitpart=[]
        cc = 0
        for i in range(0,parti):
            splitpart.append(str_combine(del_lot[cc:cc+1000]))
            cc = cc+1000
            
        del_lot_str= '' 
        
    else:
        del_lot_str = str_combine(del_lot)

    
    delete_lot = df_delete["LOTNO"].tolist()
    delete_lot_str = str_combine(delete_lot)
    

    
    
    #避免ins重複資料，及insert筆數合併    
    df_ins_ = pd.concat([df_ins,df_insert])
    df_ins_.drop_duplicates(subset=['LOTNO','HOLD_NO','HOLD_CODE','HOLD_EQ_NAME'],keep='last',inplace=True)

    #去除多餘欄位
    del df_ins_["MODIFY_TIME"]
    del df_ins_["TYPE_DESC"]
    del df_ins_["SN"]


    df_ins_.reset_index(drop=True,inplace=True)
    #df_insert.reset_index(drop=True,inplace=True)


    try:
        if(del_lot_str!='' or len(del_lot)/1000>1):
            #刪RIS            
            logging.info('DEL UPDATE-DEL RIS LOT : '+ datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")) 
            
            if(len(del_lot)/1000>1):            
                logging.info('DEL UPDATE-DEL LOT >1000')
                sql = "DELETE FROM MES.MES_HOLD_LOT_NEW WHERE "
                sql_del_lot=''
                for s in splitpart:    
                    sql_del_lot = sql_del_lot+"HOLD_NO in("+s+") or "        
                sql = sql+sql_del_lot[:-4]
            else:
                logging.info('DEL UPDATE-DEL LOT <=1000')
                sql = "DELETE FROM MES.MES_HOLD_LOT_NEW WHERE HOLD_NO in("+del_lot_str+")"
            print(sql)                
            cur.execute(sql)
            eng_ris.commit()
        else:
            logging.info('NO UPDATE-DEL DATA') 
            
        if(len(df_ins_)>0):
#>0
            #新增RIS
            logging.info('INS UPDATE-INS RIS LOT : '+ datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")) 
            cols = df_ins_.columns.tolist()
            sql = sql_str('MES.MES_HOLD_LOT_NEW', cols)
            sql_val(sql, df_ins_, cur, eng_ris)
        else:
            logging.info('NO UPDATE-INS DATA')             

        if(delete_lot_str!=''):
#!=''
            #刪除RIS >> 放置最後順序，避免刪掉後又INS回去
            logging.info('DEL DELETE RIS LOT : '+ datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")) 
            sql = "DELETE FROM MES.MES_HOLD_LOT_NEW WHERE HOLD_NO in("+delete_lot_str+")"
            cur.execute(sql)
            eng_ris.commit()
        else:
            logging.info('NO DELETE DATA') 
            
        #刪MES
        if(del_sn1_str!=''):
#!=''
            logging.info('DEL UPDATE-DEL MES SN : '+ datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
            sql = "DELETE FROM "+DB+".dbo.TH_MES_HOLD_LOT_RECORD WHERE SN IN("+del_sn1_str+")"
            con_mes.execute(text(sql))
            #con_mes.commit()
        else:
            logging.info('NO UPDATE-DEL DATA')            

        #    eng_mes.execute(sql)
        if(del_sn2_str!=''):        
#!=''
            logging.info('DEL UPDATE-INS MES SN : '+ datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
            sql = "DELETE FROM "+DB+".dbo.TH_MES_HOLD_LOT_RECORD WHERE SN IN("+del_sn2_str+")"
            con_mes.execute(text(sql))
            #con_mes.commit()
        else:
            logging.info('NO UPDATE-INS DATA')            


        if(del_sn3_str!=''):        
#!=''
            logging.info('DEL INSERT MES SN : '+ datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
            sql = "DELETE FROM "+DB+".dbo.TH_MES_HOLD_LOT_RECORD WHERE SN IN("+del_sn3_str+")"

            con_mes.execute(text(sql))
            #con_mes.commit()            
        else:
            logging.info('NO INSERT DATA')            
            
        if(del_sn4_str!=''):        
#!=''
            logging.info('DEL INSERT MES SN : '+ datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
            sql = "DELETE FROM "+DB+".dbo.TH_MES_HOLD_LOT_RECORD WHERE SN IN("+del_sn4_str+")"

            con_mes.execute(text(sql))
            #con_mes.commit()            
        else:
            logging.info('NO INSERT DATA')
            
        logging.info('END at - '+ datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
        logging.debug('----------------------------------------------------')

        con_mes.close()
        eng_mes.dispose()

    except Exception as E:
        logging.info("UPDATE FAIL : "+str(E))
        #email alarm
        from datetime import date
        thmail.thmail('MES_HOLD_LOT','Error:'+str(E),'/home/cim/log/MES_HOLD_LOT/'+format(str(date.today()))+'.log','[WARNING] - MES_HOLD_LOT同步錯誤')

        con_mes.close()
        eng_mes.dispose()

else:
    logging.info('Theres no record to update')    
    logging.info('End at - ' + datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"))
    logging.debug('---------------------------------------------------------')
    con_mes.close()
    eng_mes.dispose()
