
import datetime
import pymssql
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

import pyodbc
import sys
import time

import warnings
warnings.filterwarnings('ignore')

import sys
sys.path.append('/home/cim')
# connector
import connect.connect as cc

eng_mes = cc.connect('MES', 'MES_Test')
eng_cim = cc.connect('CIM', 'SAP_WKTIME')
eng_sap = cc.connect('SAP', 'SAP_TEST')

cur = eng_sap.cursor()

DB = "MES_Production"
DB = "MES_Test"


def sql_str(table, cols):
    sql = "INSERT INTO "+table + "("

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


def sql_val(sql, df, cur, eng_sap):
	for j in range(0, len(df["IDBSNO"])):
		bindVar = {}
		for i, v in enumerate(df):
			b = {str(v): str(df[v][j])}
			bindVar.update(b)
			#         print(sql)
			#         print(bindVar)

		cur.execute(sql, bindVar)
		eng_sap.commit()


def UPDATESTATUS(MANDT):
    #CIM等待交易的BUDAT區間
    sql = "SELECT MIN(`BUDAT`) AS `STARTDAT`,MAX(`BUDAT`) AS `ENDDAT` FROM `sap_wktime`.`sap_25a` WHERE `MANDT` = '"+MANDT+"' AND `STATUS` = '001'"
    check_date = pd.read_sql_query(sql, eng_cim)
    
    if(len(check_date)>0):

        startdat = str(check_date['STARTDAT'][0])
        enddat = str(check_date['ENDDAT'][0])

	    
        #篩選CIM等待交易的AUFNR
        sql = "SELECT * FROM `sap_wktime`.`sap_25a` WHERE `MANDT` = '"+MANDT+"' AND `STATUS` = '001'"
        cim_log = pd.read_sql_query(sql, eng_cim)


        #篩選SAP BUDAT區間的交易結果

        sql = '''
        SELECT IDBSNO,AUFNR,RMZHL,MANDT,BUDAT,EXETYP,CASE WHEN SUM(SAP_RESULT)> 0 THEN 'E' ELSE 'F' END AS SAP_RESULT
        FROM (
            SELECT a.IDBSNO,a.AUFNR,a.RMZHL,a.MANDT,c.BUDAT,a.EXETYP,CASE WHEN a.STSTYP = 'F' THEN 0 ELSE 1 END AS SAP_RESULT 
            FROM SAPS4.ZPPT0025C2 a
        LEFT JOIN (
            SELECT * FROM THSAP.ZPPT0025A) c 
        ON a.IDBSNO = c.IDBSNO AND a.AUFNR = c.AUFNR AND a.WERKS = c.WERKS AND a.RMZHL = c.RMZHL AND a.VORNR = c.VORNR
        WHERE a.WERKS = '1031' AND a.MANDT = \'''' + MANDT + '''\'
        AND (c.BUDAT >= \'''' + startdat + '''\' AND c.BUDAT <= \'''' + enddat + '''\')
        ) ff 
        GROUP BY IDBSNO,AUFNR,RMZHL,MANDT,BUDAT,EXETYP
        '''


		
        sap_result = pd.read_sql(sql, eng_sap)

        if(len(sap_result)>0):
	   
            #更改RMZHL格式為INT64(配合CIM)
            sap_result['RMZHL'] = sap_result['RMZHL'].astype('int64')


            #合併兩表
            cim_result = cim_log.merge(sap_result, left_on=['IDBSNO','AUFNR','RMZHL','BUDAT','MANDT','EXETYP'], right_on=['IDBSNO','AUFNR','RMZHL','BUDAT','MANDT','EXETYP'], how='left')
            #根據SAP交易結果判斷CIM STATUS欄位
            cim_result['STATUS'] = cim_result['SAP_RESULT'].map(lambda x:'003' if x=='F' else('002' if x=='E' else '001'))
            #刪除SAP_RESULT欄位
            cim_result = cim_result.drop(['SAP_RESULT'], axis=1)


            #存入CIM暫存表
            cim_result.to_sql('sap_25a_temp', con=eng_cim, if_exists='replace', index=False)

            #更新 sap_25a.STATUS

            sql = "REPLACE into sap_25a (SELECT * FROM sap_25a_temp)"

            con_cim = eng_cim.connect()		   
            con_cim.execute(text(sql))
            con_cim.commit()


def READ_MES(MANDT):
    # 撈取指定MO過帳紀錄
    sql = "EXECUTE "+DB+".dbo.SAP_25A_ALL @BEGIN='"+BEGIN+"',@END='"+END+"'"
    df_25A = pd.read_sql(sql, eng_mes)

    sql = "EXECUTE "+DB+".dbo.SAP_25B_ALL @BEGIN='"+BEGIN+"',@END='"+END+"'"
    df_25B = pd.read_sql(sql, eng_mes)

    AUFNR_LIST =df_25A["AUFNR"].tolist()
    AUFNR_LIST = list(set(AUFNR_LIST))
    
    df_25A_ = pd.DataFrame(columns=df_25A.columns)
    df_25B_ = pd.DataFrame(columns=df_25B.columns)
    df_25A_.insert(0,"IDBSNO","")
    df_25B_.insert(0,"IDBSNO","")
    
    for i,v in enumerate(AUFNR_LIST):
        dfa_ = df_25A[df_25A['AUFNR']==v]
        dfb_ = df_25B[df_25B['AUFNR']==v]
        time.sleep(1)
        IDBSNO = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
               
        dfa_ = dfa_.copy()
        dfb_ = dfb_.copy()
        
        dfa_["IDBSNO"] = IDBSNO
        dfb_["IDBSNO"] = IDBSNO
        df_25A_ = pd.concat([df_25A_,dfa_])
        df_25B_ = pd.concat([df_25B_,dfb_])


    df_25C1 = df_25A_.copy()
    # 最後拋給SAP
    df_25A = df_25A_[['IDBSNO',
                     'WERKS',
                     'AUFNR',
                     'VORNR',
                     'RMZHL',
                     'RWKIND',
                     'LMNGA',
                     'XMNGA',
                     'BUDAT'
                     ]]

    # 最後拋給SAP
    df_25B = df_25B_[['IDBSNO',
                     'WERKS',
                     'AUFNR',
                     'VORNR',
                     'RMZHL',
                     'MATNR',
                     'CHARG'
                     ]]

    df_25C1 = df_25C1[['IDBSNO',
                       'WERKS',
                       'AUFNR',
                       'VORNR',
                       'RMZHL',
                       'GETDAT'
                       ]]

    df_25C1["EXETYP"] = 'A'

    df_25A["MANDT"] = MANDT
    df_25B =df_25B.copy()
    df_25B["MANDT"] = MANDT
    df_25C1["MANDT"] = MANDT

    return df_25A, df_25B, df_25C1,AUFNR_LIST




#取出時間區間
now = datetime.datetime.now()
#now = datetime.datetime(2023,8,30,1,10,0)
BEGIN = now + datetime.timedelta(days=-1)
BEGIN = BEGIN.strftime('%Y-%m-%d')+' 00:00:00'
END = now.strftime('%Y-%m-%d')+' 00:00:00'

#
sql = "SELECT PARAMETERVALUE FROM TBLSYSPARAMETER WHERE PARAMETERNO = 'SAP_MANDT'"
MANDT = str(pd.read_sql(sql,eng_mes)["PARAMETERVALUE"][0])

#更新狀態

# try:
#     print('更新CIM LOG狀態')
#     UPDATESTATUS(MANDT)
# except Exception as e:
#     print('更新CIM LOG狀態失敗')
#     print(e)


UPDATESTATUS(MANDT)

print(BEGIN+'~~~'+END)
print("MANDT:"+MANDT)
# In[4]:


df_25A, df_25B, df_25C1,AUFNR_LIST = READ_MES(MANDT)



# In[6]:

time.sleep(1)
# df_25A_ = pd.DataFrame(columns=df_25A.columns)
# df_25B_ = pd.DataFrame(columns=df_25B.columns)
# df_25C1_ = pd.DataFrame(columns=df_25C1.columns)
print(AUFNR_LIST)

for i,v in enumerate(AUFNR_LIST):
    dfa_ = df_25A[df_25A['AUFNR']==v]
    dfb_ = df_25B[df_25B['AUFNR']==v]    
    dfc_ = df_25C1[df_25C1['AUFNR']==v]
    # 把select的key值抓出來
    cim_25A = dfa_.copy()
    cim_25A = cim_25A[['IDBSNO', 'AUFNR', 'RMZHL', 'BUDAT', 'MANDT']]
    cim_25A.drop_duplicates(inplace=True)
    
    cim_25A.reset_index(drop=True,inplace=True)
    
    AUFNR_ = cim_25A['AUFNR'][0]
    BUDAT_ = cim_25A['BUDAT'][0]

    # 確認有同一天記錄=>STATUS=001=>不可再傳中介表  / 2023.0711 +003也不可再傳中介
    # sql = "SELECT count(*) as cc FROM SAP_25A WHERE AUFNR ='"+AUFNR_ + "' AND MANDT = '"+MANDT+"' AND BUDAT = '"+BUDAT_+"' AND STATUS in ('001','003')"

    #撈出當天最後一筆IDBSNO
    sql ="SELECT * FROM sap_25a WHERE AUFNR ='"+AUFNR_ + "' AND MANDT = '"+MANDT+"' AND BUDAT = '"+BUDAT_+"' ORDER BY IDBSNO DESC ,EXETYP DESC LIMIT 1"
    checklog = pd.read_sql(sql, eng_cim)

    if(len(checklog) > 0) :
        EXETYP_ = checklog["EXETYP"][0]
        STATUS_ = checklog["STATUS"][0]
    else:
        EXETYP_=''
        STATUS_=''
        
    
    #2023.8.24 - 若該筆EXETYP=C & STATUS in ('003') > 可再拋中介  or EXETYP=A & STATUS in ('002') > 可再拋中介
    #print(sql)
    time.sleep(1)
    if( (EXETYP_=='C' and STATUS_=='003') or (EXETYP_=='A' and (STATUS_=='002' or STATUS_=='004') ) or (EXETYP_=='' and STATUS_=='') ):
        time.sleep(1)
        # 取RMZHL歷史最大紀錄
        sql = "SELECT RMZHL FROM SAP_25A WHERE AUFNR ='"+AUFNR_+"' AND MANDT = '" +MANDT+"' AND STATUS <> '004' ORDER BY IDBSNO DESC LIMIT 1"
        #print(sql)
        df_rmzhl = pd.read_sql(sql, eng_cim)

        if(len(df_rmzhl) == 0):

            RMZHL_ = 0
        else:
            RMZHL_ = df_rmzhl["RMZHL"][0:1]
            RMZHL_ = int(RMZHL_)

        # 加回25A
        dfa_=dfa_.copy()
        dfa_["RMZHL"] = dfa_["RMZHL"].astype(int)
        dfa_["RMZHL"] = dfa_["RMZHL"] + RMZHL_

        # 加回25B
        dfb_=dfb_.copy()
        dfb_["RMZHL"] = dfb_["RMZHL"].astype(int)
        dfb_["RMZHL"] = dfb_["RMZHL"] + RMZHL_

        # 加回25C1
        dfc_=dfc_.copy()
        dfc_["RMZHL"] = dfc_["RMZHL"].astype(int)
        dfc_["RMZHL"] = dfc_["RMZHL"] + RMZHL_

        # cim_25a 加回
        cim_25A = cim_25A.copy()
        cim_25A["RMZHL"] = cim_25A["RMZHL"].astype(int)
        cim_25A["RMZHL"] = cim_25A["RMZHL"] + RMZHL_
        
       
        try:
            dfa_.reset_index(drop=True,inplace=True)
            dfb_.reset_index(drop=True,inplace=True)
            dfc_.reset_index(drop=True,inplace=True)
            # 拋給SAP
            cols = dfa_.columns.tolist()            
            sql = sql_str('thsap.ZPPT0025A', cols)
            sql_val(sql, dfa_, cur, eng_sap)

            cols = dfb_.columns.tolist()
            sql = sql_str('thsap.ZPPT0025B', cols)
            sql_val(sql, dfb_, cur, eng_sap)

            cols = dfc_.columns.tolist()
            sql = sql_str('thsap.ZPPT0025C1', cols)
#             因材料有重工造成資料重複~SAP不能丟重複值~故程式報錯~2023.5.9增加去重複步驟
            dfc_.drop_duplicates(subset=[
                                    'IDBSNO', 'WERKS', 'AUFNR', 'VORNR', 'RMZHL', 'GETDAT', 'EXETYP', 'MANDT'], inplace=True)
            dfc_.reset_index(drop=True,inplace=True)
            sql_val(sql, dfc_, cur, eng_sap)

            # 拋給cim=>等待交易結果
            cim_25A["EXETYP"] = "A"
            cim_25A["STATUS"] = '001'
            cim_25A.to_sql('sap_25a', con=eng_cim, if_exists='append', index=False)
            
        except Exception as e:
            print(e)
            # 拋給cim=>拋送中介表失敗
            cim_25A["EXETYP"] = "A"
            cim_25A["STATUS"] = '004'
            
            cim_25A.to_sql('sap_25a', con=eng_cim, if_exists='append', index=False)


eng_cim.dispose()
eng_mes.dispose()

