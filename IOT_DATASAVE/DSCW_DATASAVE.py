import pandas as pd
import sys
import datetime
from sqlalchemy import text
import os


sys.path.append('/home/cim')
#sys.path.append('C:\\Users\\User\\Desktop\\python')
import connect.connect as cc
import global_fun.sftp_fun as ss


eng_cim = cc.connect('CIM_ubuntu', 'iot')
con = eng_cim.connect()	

try:
    sql = "SELECT no,value,time from dscw"    
    df = pd.read_sql(sql, eng_cim)

    sql = "TRUNCATE TABLE dscw"
    con.execute(text(sql))
    #con.commit()
      
    nolist = list(set(df['no'].tolist()))
    df["date"] = df["time"].map(lambda x : x.replace('-',''))  
    df["mm"] = df["date"].map(lambda x : x[0:6])  
    df["dd"] = df["date"].map(lambda x : x[0:8])  

    #設備編號
    for n in nolist:
        df_ = df[df['no'] ==n]
        mmlist = list(set(df_['mm'].tolist()))
        
        #月份
        for mm in mmlist:        
            df_mm = df_[df['mm']==mm]
            ddlist = list(set(df_mm['dd'].tolist()))
            
            #日期
            for dd in ddlist:        
                df_dd = df_[df['dd']==dd]
                del df_dd['date']
                del df_dd['mm']
                del df_dd['dd']
                df_dd.to_csv(dd+".csv",index=False)
                PATHS = 'G1-DataBase/DS-CLEAN/'+str(n)+'/'+str(mm)
                sftp = ss.sftp_upload('ltg1database.theil.com',PATHS,dd+'.csv')
                os.remove(dd+'.csv')

except Exception as e:
    print("ERROR:"+str(e))

eng_cim.dispose()
con.close()
