import sys
import datetime
import pymssql
import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import requests
import time
import numpy as np

import warnings
warnings.filterwarnings('ignore')

sys.path.append('/home/cim')


# connector
import connect.connect as cc
DB = "MES_Test"
eng_mes = cc.connect('MES', DB)



sql = "SELECT * FROM VIEW_PRDSTDTIMESETTING WHERE KEEPIT ='1'"
df = pd.read_sql(sql, eng_mes)



df.drop_duplicates(subset=['PRODUCTNO', 'PRODUCTVERSION','PROCESSNO','OPGROUP','UNITNO','VALUE'], keep='first',inplace=True)
df.reset_index(drop=True,inplace=True)
PRDLIST = list(set(df["PRODUCTNO"].tolist()))

df_std = pd.DataFrame(columns=['PRODUCTNO','PRODUCTVERSION','AREANO','OPNO','EQUIPMENTNO','EQUIPMENTTYPE','STDUNITEMPTIME','STDUNITEQPTIME','COUNTEQPUNITQTY'
,'COUNTOPUNITQTY','STDQUEUETIME','CREATOR','CREATEDATE','FIXEMPTIMEA','VAREMPTIME','FIXEQPTIME','VAREQPTIME','WORKPRICETYPE','WORKPRICE'])


for i in range(0,len(df)):    
    df_ =df[i:i+1]
    df_.reset_index(drop=True,inplace=True)
    GROSSDIE = df_["GROSSDIE"][0]
    FRAMEDIE = df_["FRAMEDIE"][0]
    ProductSize = df_["ProductSize"][0]    
    STDUNITEQPTIME = eval(df_["FUNC"][0])
    #STDUNITEQPTIME = df_["FUNC"][0]
    STDUNITEMPTIME = STDUNITEQPTIME / df_["MMR"][0]
    df_["STDUNITEQPTIME"] = STDUNITEQPTIME
    df_["STDUNITEMPTIME"] = STDUNITEMPTIME
    df_["STDRUNTUNE"] = 0
    df_["STDQUEUETIME"] = 0
    df_["CREATOR"] = "AUTO"
    df_["CREATEDATE"] = datetime.datetime.now()
    df_["FIXEMPTIMEA"] = 0
    df_["VAREMPTIME"] = 0
    df_["FIXEQPTIME"] = 0
    df_["VAREQPTIME"] = 0
    df_["WORKPRICETYPE"] = 1
    df_["WORKPRICE"] = 0
    df_["COUNTEQPUNITQTY"]=1
    
    ITKEEP2 = 1
    
    #S059確認
    if('S059' in df_["ATTR"][0]):
        if('1' in df_["VALUE"][0]) :
            prsno = df_["PROCESSNO"][0]
            if('S059' in df[df["PROCESSNO"]==prsno]["OPGROUP"].tolist()):
                ITKEEP2 =1
            else:
                ITKEEP2 =0
        elif('0' in df_["VALUE"][0]) :
            prsno = df_["PROCESSNO"][0]
            if('S059' not in df[df["PROCESSNO"]==prsno]["OPGROUP"].tolist()):
                ITKEEP2 =1
            else:
                ITKEEP2 =0            
        else:
            ITKEEP2 = 1
        
    #S081確認
    if('S081' in df_["ATTR"][0]):
        if('1' in df_["VALUE"][0]) :
            prsno = df_["PROCESSNO"][0]
            if('S081' in df[df["PROCESSNO"]==prsno]["OPGROUP"].tolist()):
                ITKEEP2 =1
            else:
                ITKEEP2 =0
        elif('0' in df_["VALUE"][0]) :
            prsno = df_["PROCESSNO"][0]
            if('S081' not in df[df["PROCESSNO"]==prsno]["OPGROUP"].tolist()):
                ITKEEP2 =1
            else:
                ITKEEP2 =0
        else:
            ITKEEP2 = 1
        
    if ITKEEP2 == 1 :
        df_= df_[['PRODUCTNO','PRODUCTVERSION','AREANO','OPNO','EQUIPMENTNO','EQUIPMENTTYPE','STDUNITEMPTIME','STDUNITEQPTIME','COUNTEQPUNITQTY'
    ,'COUNTOPUNITQTY','STDQUEUETIME','CREATOR','CREATEDATE','FIXEMPTIMEA','VAREMPTIME','FIXEQPTIME','VAREQPTIME','WORKPRICETYPE','WORKPRICE']]
        df_std = pd.concat([df_std,df_])    
        
df_std = df_std.fillna(0)        
df_std.replace([np.inf, -np.inf], 0, inplace=True)

df_std.to_excel("final.xlsx",index=False)
df_std.to_sql("TBLPRDRUNTIMESETUP",con=eng_mes,if_exists='append',index=False)
