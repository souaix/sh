
import sys
import datetime
# import pymssql
import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import requests
import time
import os

import warnings
warnings.filterwarnings('ignore')

sys.path.append('/home/cim')

import global_fun.sftp_fun as ss

# connector
import connect.connect as cc

#email
import global_fun.thmail_fun as thmail

DB = "MES_Production"
eng_mes = cc.connect('MES',DB)

now = datetime.datetime.now()
#now = datetime.datetime(2024,4,7,0,0,0)

BEGIN = now + datetime.timedelta(days=-1)


BEGIN = BEGIN.strftime('%Y-%m-%d')+' 00:00:00'
END = now.strftime('%Y-%m-%d')+' 00:00:00'

DATESTR = now.strftime('%Y%m%d')
FILESTR = "996ZYield"+DATESTR+".CSV"

sql = "EXECUTE "+DB+".dbo.[ST_S093B] @D1='" + BEGIN+"',@D2='"+END+"'"
df = pd.read_sql(sql, eng_mes)

df.to_csv(FILESTR,index=False)

hh = datetime.datetime.now().hour

for root, dir_list, file_list in os.walk('./'):
    for csv in file_list:
        if(".CSV" in csv or ".csv" in csv) :                  
            from datetime import date
            #8點先發給工程
            if(hh==8):
                thmail.thmail('f83yieldtest','FYI','/home/cim/sh/ST/WIPDATA/F83UPLOAD/'+csv,'[ST]-F83YIELD:'+format(str(date.today())))
                os.remove('/home/cim/sh/ST/WIPDATA/F83UPLOAD/'+csv)

            else:
                try:
                    sftp = ss.sftp_upload('mft-ap.st.com','Share/gobmsftp_tonghsing_996Z/Yield',csv,'/home/cim/global_fun/PublicKeyForST/STSFTP_DSAKEY')
                    os.remove('/home/cim/sh/ST/WIPDATA/F83UPLOAD/'+csv)
                except:
                    thmail.thmail('f83yieldtest','F83 Yield上拋失敗，請聯繫IT處理','/home/cim/sh/ST/WIPDATA/F83UPLOAD/'+csv,'[ST]-F83YIELD FAIL:'+format(str(date.today())))

eng_mes.dispose()
