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
import os
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

eng_mes = cc.connect('MES', 'MES_Production')

con_mes = eng_mes.connect()

DB = "MES_Production"

now = datetime.datetime.now().strftime("%Y%m%d")

sql = "select * from MES_Production.dbo.V_Check_DSRW_Instock"

df = pd.read_sql(sql, eng_mes)

if(len(df)>0):
    df.to_excel(now+"_CheckDSRWInstock.xlsx",index=False)
    thmail.thmail('checkdsrwinstock','FYI','/home/cim/'+now+'_CheckDSRWInstock.xlsx','[MES] - 切割入庫數不等於RW工單數!')
    os.remove('/home/cim/'+now+'_CheckDSRWInstock.xlsx')

else:
    print("no data")

con_mes.close()

eng_mes.dispose()


