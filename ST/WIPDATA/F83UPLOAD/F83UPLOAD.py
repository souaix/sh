
import sys
import datetime
# import pymssql
import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import requests
import time
import os
import logging

import warnings
warnings.filterwarnings('ignore')

sys.path.append('/home/cim')

import global_fun.sftp_fun as ss

# connector
import connect.connect as cc

#email
import global_fun.thmail_fun as thmail

# logging
import global_fun.logging_fun as logfun
#開啟log
logfun.set_logging('/home/cim/log/F83UPLOAD')

now = datetime.datetime.now()
#now = datetime.datetime(2024,7,3,17,0,0)
hh = now.hour

localPATH = '/home/cim/'
toPATH = '/home/cim/S/F83UPLOAD/'
proPATH = '/home/cim/sh/ST/WIPDATA/F83UPLOAD/'

#8點產生檔案丟入S:
if(hh==8):

    DB = "MES_Production"
    eng_mes = cc.connect('MES',DB)
        
    BEGIN = now + datetime.timedelta(days=-1)        
    BEGIN = BEGIN.strftime('%Y-%m-%d')+' 00:00:00'
    END = now.strftime('%Y-%m-%d')+' 00:00:00'
    
    DATESTR = now.strftime('%Y%m%d')
    FILESTR = "996ZYield"+DATESTR+".csv"
    
    sql = "EXECUTE "+DB+".dbo.[ST_S093B_0716] @D1='" + BEGIN+"',@D2='"+END+"'"
    #print(sql)
    df = pd.read_sql(sql, eng_mes)
    
    df["PnP_input"] =  df["PnP_input"].astype(str)
    df["PnP_output"] =  df["PnP_output"].astype(str)
    df["Total_reject_Qty"] =  df["Total_reject_Qty"].astype(str)
    df["PnP_Yield"] =  df["PnP_Yield"].astype(str)
    df["BSC_QTY"] =  df["BSC_QTY"].astype(str)
    df["BSC_Rate"] =  df["BSC_Rate"].astype(str)
    
    logging.info("CHECK EXISTS:"+toPATH)   
    sudoPassword = 'theil4893701'
    #建立DIR
    if not os.path.isdir(toPATH):
        logging.info("folder NOT EXIST!")
    #    os.makedirs(PATH)
        try:
            command = 'mkdir '+toPATH
            os.system('echo %s|sudo -S %s' % (sudoPassword, command))
    
            #os.system("sudo mkdir "+toPATH)
    
            logging.info("NOT EXISTS > MKDIR SUCCESS")
        except Exception as e:
            
            logging.info("NOT EXISTS > MKDIR FAIL")
            logging.info(str(e))
    else:
        logging.info("folder EXIST!")
        
    #匯出CSV
    try:
    
        import csv
        df[""]=""
        df.to_csv(FILESTR,index=False,quoting=csv.QUOTE_NONNUMERIC)
    
        logging.info("TO_CSV SUCCESS :"+FILESTR)
    
    except Exception as e:
    
        logging.info("TO_CSV FAIL :"+FILESTR)
        logging.info(str(e))

    
    #寄信
    try:
        from datetime import date
        thmail.thmail('f83yield','S:\F83UPLOAD',FILESTR,'[ST]-F83YIELD:'+format(str(date.today())))
    except Exception as e:
        logging.info("MAIL FAIL:"+str(e))

    
    #搬移至S:
    try:
    
        command = "mv "+localPATH+FILESTR+" "+proPATH+FILESTR
        os.system('echo %s|sudo -S %s' % (sudoPassword, command))
    
        #os.system("sudo mv /home/cim/sh/ST/WIPDATA/F83UPLOAD/"+FILESTR+" "+PATH+FILESTR)
        #os.system("sudo mv "+localPATH+FILESTR+" "+proPATH+FILESTR)
        logging.info("MV SUCCESS:"+localPATH+" to "+proPATH)
    
        command = " mv "+proPATH+FILESTR+" "+toPATH+FILESTR
        #os.system("sudo mv "+proPATH+FILESTR+" "+toPATH+FILESTR)
        os.system('echo %s|sudo -S %s' % (sudoPassword, command))
    
        logging.info("MV SUCCESS:"+proPATH+" to "+toPATH)
    
        print(FILESTR+" success")
    
    except Exception as e:    
        logging.info("MV FAIL:"+str(e))                
        
#拋給客戶端        
elif(hh==17):    
    for root, dir_list, file_list in os.walk(toPATH):
        for csvs in file_list:
            if(".CSV" in csvs or ".csv" in csvs) :                  
                from datetime import date
                try:
                    sftp = ss.sftp_upload('mft-ap.st.com','Share/gobmsftp_tonghsing_996Z/Yield',toPATH+csvs,'/home/cim/global_fun/PublicKeyForST/STSFTP_DSAKEY')
                    #os.remove(localPATH+csvs)
                    logging.info("上傳成功")
                except Exception as e:
                    thmail.thmail('f83yield','F83 Yield上拋失敗，請聯繫IT處理:'+str(e),toPATH+csvs,'[ST]-F83YIELD FAIL:'+format(str(date.today())))
                    logging.info(str(e))
                    

eng_mes.dispose()
