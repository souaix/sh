
import sys
import datetime
import pymssql
import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import requests
import time

import warnings
warnings.filterwarnings('ignore')

sys.path.append('/home/cim')

# connector
import connect.connect as cc

eng_mes = cc.connect('MES', 'MES_Production')
# con_cim = cc.connect('CIM', 'SAP_WKTIME')
eng_sap = cc.connect('SAP', 'SAP_PRD')
cur = eng_sap.cursor()

DB = "MES_Production"
# DB = "MES_Test"

def sql_str(table, cols):
    sql = "INSERT INTO "+table + "("

    cols_str = ""
    for i, v in enumerate(cols):
        sql = sql + v + ","
    sql = sql[0:-1]+") VALUES("

    for i, v in enumerate(cols):
        if str(v) == "GETDAT":
            sql = sql + "to_date(:" + v + ",'YYYY-MM-DD HH24:mi:ss'),"
        else:
            sql = sql + ":" + v + ","
    sql = sql[0:-1]+")"

    return sql


def sql_val(sql, df, cur, con_sap):
    for j in range(0, len(df["IDBSNO"])):
        bindVar = {}
        for i, v in enumerate(df):
            b = {str(v): str(df[v][j])}
            bindVar.update(b)
#         print(sql)
#         print(bindVar)
        cur.execute(sql, bindVar)
        con_sap.commit()
        
# 工單關結FUNCTION
def CLOSEMO(IDBSNO, AUFNR, MANDT):
    # 撈取指定MO過帳紀錄
    sql = "EXECUTE "+DB+".dbo.SAP_28A_AUFNR @IDBSNO='" + \
        IDBSNO+"',@AUFNR='"+AUFNR+"',@MANDT='"+MANDT+"'"

    df_28A = pd.read_sql(sql, eng_mes)

    sql = "EXECUTE "+DB+".dbo.SAP_28B1_AUFNR @IDBSNO='" + \
        IDBSNO+"',@AUFNR='"+AUFNR+"',@MANDT='"+MANDT+"'"
    df_28B1 = pd.read_sql(sql, eng_mes)

    return df_28A, df_28B1        


now = datetime.datetime.now()
#now = datetime.datetime(2024,1,4,0,0,0)
BEGIN = now + datetime.timedelta(days=-1)
BEGIN = BEGIN.strftime('%Y-%m-%d')+' 00:00:00'
END = now.strftime('%Y-%m-%d')+' 00:00:00'

sql ='''
select AUFNR from
(select distinct left(LOTNO,12) AS MONO from TBLINVWIPINVENTORY_SEMI_DETAIL where STATUS='02' and FGDINDATE >=\''''+BEGIN+'''' and FGDINDATE < \''''+END+'''') as a
left join
(select aufnr,mono from TBLOEMOBASIS) as b
on a.MONO=b.MONO
'''

df = pd.read_sql(sql, eng_mes)
aufnr_list = df['AUFNR'].tolist()

sql = "SELECT PARAMETERVALUE FROM TBLSYSPARAMETER WHERE PARAMETERNO = 'SAP_MANDT'"
MANDT = pd.read_sql(sql, eng_mes)["PARAMETERVALUE"][0]

df_fail = pd.DataFrame(
    columns=['AUFNR', 'ZZTB_NO', 'ZZCUST_LOT', 'GETDAT', 'STATUS', 'DIRECT', 'NODE'])

for i,v in enumerate(aufnr_list):
    idbsno = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")

    df_28A, df_28B1 = CLOSEMO(idbsno, v, MANDT)

    cols = df_28A.columns.tolist()
    sql = sql_str('thsap.ZPPT0028A', cols)

    try:
        sql_val(sql, df_28A, cur, eng_sap)
    except:
        df_fail["AUFNR"]=[v]
        df_fail['GETDAT']=[now]
        df_fail['STATUS']=['FAIL']
        df_fail['DIRECT']=['DLV']
        df_fail['NODE']=['28A']
        df_fail.to_sql('TH_SAPSTOCK_LOG', con=eng_mes,
                             if_exists='append', index=False)                        

    cols = df_28B1.columns.tolist()
    sql = sql_str('thsap.ZPPT0028B1', cols)

    try:
        sql_val(sql, df_28B1, cur, eng_sap)
    except:
        df_fail["AUFNR"]=[v]
        df_fail['GETDAT']=[now]
        df_fail['STATUS']=['FAIL']
        df_fail['DIRECT']=['DLV']
        df_fail['NODE']=['28B']
        df_fail.to_sql('TH_SAPSTOCK_LOG', con=eng_mes,
                             if_exists='append', index=False)                
        
        
    
