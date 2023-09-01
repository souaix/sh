import datetime
import pymssql
import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import requests
import time

import sys
sys.path.append('/home/cim')

# connector
import connect.connect as cc

con_mes = cc.connect('MES', 'MES_Test')
# con_cim = cc.connect('CIM', 'SAP_WKTIME')
con_sap = cc.connect('SAP', 'SAP_TEST')
cur=con_sap.cursor()


#ORACLE FUNCTION
def sql_str(table,cols):
    sql = "INSERT INTO "+table +"("
    
    cols_str = ""
    for i,v in enumerate(cols):                    
        sql = sql + v +","
    sql = sql[0:-1]+") VALUES(" 

    for i,v in enumerate(cols):
        if str(v)=="GETDAT":        
            sql = sql + "to_date(:" + v + ",'YYYY-MM-DD HH24:mi:ss'),"
        else:
            sql = sql + ":"+ v +","
    sql = sql[0:-1]+")" 
    
    return sql
    
def sql_val(sql,df,cur,con_sap):
    for j in range(0,len(df["IDBSNO"])):
        bindVar={}
        for i,v in enumerate(df):
            b={str(v):str(df[v][j])}
            bindVar.update(b)

#             print(sql)
#         print(bindVar)            
        cur.execute(sql,bindVar)
        con_sap.commit()    
        print('commited')




# In[3]:


#工單關結FUNCTION
def CLOSEMO(IDBSNO,AUFNR,MANDT):
    #撈取指定MO過帳紀錄
    sql ="EXECUTE MES_Test.dbo.SAP_28A_AUFNR @IDBSNO='"+IDBSNO+"',@AUFNR='"+AUFNR+"',@MANDT='"+MANDT+"'"
    
    df_28A = pd.read_sql(sql,con_mes)

    sql ="EXECUTE MES_Test.dbo.SAP_28B1_AUFNR @IDBSNO='"+IDBSNO+"',@AUFNR='"+AUFNR+"',@MANDT='"+MANDT+"'"
    df_28B1 = pd.read_sql(sql,con_mes)   
    
    return df_28A,df_28B1    
    


# In[4]:


#入庫FUNCTION
def finishgoodsin_xml(boxno,fgdinno):
    now_str =datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
    now_date =datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    now_str = now_str[0:17]    

    xml_data = '''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:wse="http://www.imestech.com/wsEAI">
       <soapenv:Header/>
       <soapenv:Body>
          <wse:invokeSrv>
             <wse:InXml>
    <![CDATA[
        <request key="fc4789febbba9a3a5cc29f9455a9ef90" type="sync">
          <host lang="zh_TW" acct="tiptop" timestamp="20230619090702706" timezone=" 8" id="toptst" ip="127.0.0.1" ver="1.0" prod="ERP"/>
          <service id="THEIL" ip="192.168.1.129" prod="ERP" srvver="1.0" name="FinishGoodsIn"/>
          <identity>
            <transactionid>'''+now_str+'''</transactionid>
            <moduleid>ERP</moduleid>
            <functionid>FinishGoodsIn</functionid>
            <computername>ERPAP01</computername>
            <curuserno>ERPAUTO</curuserno>
            <sendtime>'''+now_date+'''</sendtime>
          </identity>
          <parameter>
            <finishgoodsin>
              <name>finishgoodsin</name>
              <type>String</type>
              <value>
                <fgdinno>'''+fgdinno+'''</fgdinno>
                <fgdinitemno>1</fgdinitemno>
                <boxno>'''+boxno+'''</boxno>
              </value>
            </finishgoodsin>
          </parameter>
        </request>
    ]]>
             </wse:InXml>
          </wse:invokeSrv>
       </soapenv:Body>
    </soapenv:Envelope>
    '''

    return xml_data


#退庫FUNCTION
def finishgoodsout_xml(fgdinno):
    now_str =datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
    now_date =datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    now_str = now_str[0:17]    

    xml_data = '''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:wse="http://www.imestech.com/wsEAI">
       <soapenv:Header/>
       <soapenv:Body>
          <wse:invokeSrv>
             <wse:InXml>
    <![CDATA[
        <request key="fc4789febbba9a3a5cc29f9455a9ef90" type="sync">
          <host lang="zh_TW" acct="tiptop" timestamp="20230613135900198" timezone=" 8" id="toptst" ip="127.0.0.1" ver="1.0" prod="ERP"/>
          <service id="THEIL" ip="192.168.1.129" prod="ERP" srvver="1.0" name="FinishGoodsOut"/>
          <identity>
            <transactionid>'''+now_str+'''</transactionid>
            <moduleid>ERP</moduleid>
            <functionid>FinishGoodsOut</functionid>
            <computername>ERPAP01</computername>
            <curuserno>ERPAUTO</curuserno>
            <sendtime>'''+now_date+'''</sendtime>
          </identity>
          <parameter>
            <fgdinno>
              <name>fgdinno</name>
              <type>String</type>
              <value>'''+fgdinno+'''</value>
              <desc></desc>
            </fgdinno>
          </parameter>
        </request>
    ]]>
             </wse:InXml>
          </wse:invokeSrv>
       </soapenv:Body>
    </soapenv:Envelope>
    '''

    return xml_data


def send_xml(xml_data):
    url = 'http://10.21.150.109/MES_Test_MESws_EAI/wsEAI.asmx?wsdl'  # 修改为正确的URL

    headers = {'Content-Type': 'application/soap+xml; charset=utf-8'}  # 设置请求头
    headers = {
        'content-type': 'text/xml',

        'User-Agent': 'AutodiscoverClient',
        'Accept-Encoding':'utf-8',
    }

    response = requests.post(url, data=xml_data, headers=headers)
    code = response.status_code
    return code


# In[5]:


#取最後MES拋轉入庫時間
sql = "SELECT top(1) GETDAT FROM MES_Test.dbo.TH_SAPSTOCK_LOG WHERE STATUS ='SUCCESS' AND DIRECT ='IN' ORDER BY GETDAT DESC"
df_log = pd.read_sql(sql,con_mes)
if len(df_log)>0:
    LAST_PDADAT = df_log['GETDAT'][0]
    LAST_PDADAT = LAST_PDADAT.strftime("%Y-%m-%d %H:%M:%S")
else:
    LAST_PDADAT=''
    print("NO LAST PDADAT")
#取最後SAP拋轉退庫時間
sql = "SELECT top(1) GETDAT FROM MES_Test.dbo.TH_SAPSTOCK_LOG WHERE STATUS ='SUCCESS' AND DIRECT ='OUT' ORDER BY GETDAT DESC"
df_log = pd.read_sql(sql,con_mes)
if len(df_log)>0:
    LAST_OUTDAT = df_log['GETDAT'][0]
    LAST_OUTDAT = LAST_OUTDAT.strftime("%Y-%m-%d %H:%M:%S")
else:
    LAST_OUTDAT = LAST_PDADAT



sql = "SELECT PARAMETERVALUE FROM TBLSYSPARAMETER WHERE PARAMETERNO = 'SAP_MANDT'"
MANDT = pd.read_sql(sql,con_mes)["PARAMETERVALUE"][0]




#取PDA入庫 + 退庫

sql = '''
SELECT * FROM (
	SELECT AUFNR,ZZTB_NO,PDADAT AS GETDAT,DIRECT FROM 
	(
		SELECT B2.*,A.ZZTB_NO,A.ZZCUST_LOT,DISPO,STSTYP,OUT_IDBSNO,(CASE WHEN OUT_IDBSNO IS NOT NULL THEN 'IN_OUT' ELSE 'IN' END) AS DIRECT  FROM
		(SELECT IDBSNO,WERKS,AUFNR,MATNR,GRSNO,PDADAT FROM SAPS4.ZPPT0026B2 WHERE  PDASTSTYP ='F' AND PDADAT > to_date(\''''+LAST_PDADAT+'''','YYYY-MM-DD HH24:mi:ss') AND MANDT =\''''+MANDT+'''' AND WERKS='1031') B2
		LEFT JOIN 
		(SELECT * FROM THSAP.ZPPT0026A) A
		ON B2.IDBSNO=A.IDBSNO AND B2.AUFNR=A.AUFNR AND B2.MATNR=A.MATNR AND B2.GRSNO=A.GRSNO
		LEFT JOIN
		(SELECT AUFNR,DISPO FROM SAPS4.ZPPT0024A WHERE DISPO ='3AA' AND WERKS='1031') A24
		ON B2.AUFNR=A24.AUFNR
		LEFT JOIN 
		(SELECT AUFNR,STSTYP FROM SAPS4.ZPPT0024D1 WHERE WERKS='1031') D124
		ON B2.AUFNR=D124.AUFNR
		LEFT JOIN
		(SELECT IDBSNO AS OUT_IDBSNO FROM SAPS4.ZPPT0027A WHERE WERKS='1031') A27
		ON B2.IDBSNO=A27.OUT_IDBSNO
		WHERE ZZTB_NO IS NOT NULL AND DISPO IS NOT NULL AND (STSTYP <> 'D' OR STSTYP IS NULL) 
	) PACK GROUP BY ZZTB_NO,ZZCUST_LOT,AUFNR,PDADAT,DIRECT
	UNION ALL
	SELECT *　FROM 
	(
		SELECT B127.AUFNR,'' AS ZZTB_NO,B127.GETDAT,'OUT' AS DIRECT FROM
			(SELECT IDBSNO,AUFNR,MAX(GETDAT) AS GETDAT FROM SAPS4.ZPPT0027B1 WHERE WERKS='1031' AND MANDT=\''''+MANDT+'''' AND  GETDAT> to_date(\''''+LAST_OUTDAT+'''','YYYY-MM-DD HH24:mi:ss') GROUP BY IDBSNO,AUFNR) B127			
            LEFT JOIN
            (SELECT AUFNR,DISPO FROM SAPS4.ZPPT0024A WHERE DISPO ='3AA' AND WERKS='1031') A24
            ON B127.AUFNR=A24.AUFNR            
            WHERE DISPO IS NOT NULL
	) PACK GROUP BY AUFNR,ZZTB_NO,GETDAT,DIRECT
) WHERE GETDAT IS NOT NULL ORDER BY GETDAT 
'''
df_pda = pd.read_sql(sql,con_sap)

# 非PDA入庫
sql ='''
SELECT AUFNR,ZZSUBLOT,ZZTB_NO,ZZCUST_LOT FROM 
(
	SELECT B2.*,A.ZZTB_NO,A.ZZCUST_LOT,ZZSUBLOT,DISPO,STSTYP FROM
	(SELECT IDBSNO,WERKS,AUFNR,MATNR,GRSNO FROM SAPS4.ZPPT0026B2 WHERE  PDASTSTYP ='F' AND PDADAT > to_date(\''''+LAST_PDADAT+'''','YYYY-MM-DD HH24:mi:ss') AND MANDT =\''''+MANDT+'''' AND WERKS='1031') B2
	LEFT JOIN 
	(SELECT * FROM THSAP.ZPPT0026A) A
	ON B2.IDBSNO=A.IDBSNO AND B2.AUFNR=A.AUFNR AND B2.MATNR=A.MATNR AND B2.GRSNO=A.GRSNO
	LEFT JOIN
	(SELECT AUFNR,DISPO FROM SAPS4.ZPPT0024A WHERE DISPO ='3AA') A24
	ON B2.AUFNR=A24.AUFNR
	LEFT JOIN 
	(SELECT AUFNR,STSTYP FROM SAPS4.ZPPT0024D1) D124
	ON B2.AUFNR=D124.AUFNR	
	LEFT JOIN	
	(SELECT IDBSNO AS OUT_IDBSNO FROM SAPS4.ZPPT0027A) A27
	ON B2.IDBSNO=A27.OUT_IDBSNO
	WHERE ZZTB_NO IS NOT NULL AND DISPO IS NOT NULL AND (STSTYP <> 'D' OR STSTYP IS NULL) AND OUT_IDBSNO IS NULL	
) PACK	
GROUP BY ZZSUBLOT,ZZTB_NO,ZZCUST_LOT,AUFNR
'''
df_nonpda = pd.read_sql(sql,con_sap)

if(len(df_pda)>0):

    #要刪除重複退庫資料
    #先拆解
    IN =df_pda[df_pda['DIRECT']=='IN']
    OUT = df_pda[df_pda['DIRECT']=='OUT']
    INOUT = df_pda[df_pda['DIRECT']=='IN_OUT']
    INOUT_MERGE = INOUT.merge(OUT,on=["AUFNR"],how='left')

    #刪除的主程式

    df_pda_del_out=pd.DataFrame(columns=['AUFNR','ZZTB_NO','GETDAT','DIRECT'])
    for i in range(1,len(INOUT_MERGE)+1):
        OUT_ = INOUT_MERGE.iloc[i-1:i]
        if(OUT_["DIRECT_y"].iloc[0] == "OUT"):        
            if(OUT_["GETDAT_y"].iloc[0] > OUT_["GETDAT_x"].iloc[0]):
                OUT_ = OUT_[['AUFNR','ZZTB_NO_y','GETDAT_y','DIRECT_y']]
                OUT_ = OUT_.rename(columns={'ZZTB_NO_y':'ZZTB_NO','GETDAT_y':'GETDAT','DIRECT_y':'DIRECT'})
                df_pda_del_out = pd.concat([df_pda_del_out,OUT_])

    #重構
    OUT = pd.concat([OUT,df_pda_del_out,df_pda_del_out]).drop_duplicates(keep=False)
    df_pda = pd.concat([IN,OUT,INOUT])
    df_pda = df_pda.sort_values(by=['GETDAT'], ascending=True)
    df_pda.reset_index(inplace=True)

###############


    # In[8]:


    #MES入庫XML + SAP工單關結
    df_success_in = pd.DataFrame(columns=['AUFNR','ZZTB_NO','ZZCUST_LOT','GETDAT','STATUS','DIRECT','NODE'])
    df_fail_in = pd.DataFrame(columns=['AUFNR','ZZTB_NO','ZZCUST_LOT','GETDAT','STATUS','DIRECT','NODE'])
    df_success_out = pd.DataFrame(columns=['AUFNR','ZZTB_NO','ZZCUST_LOT','GETDAT','STATUS','DIRECT','NODE'])
    df_fail_out = pd.DataFrame(columns=['AUFNR','ZZTB_NO','ZZCUST_LOT','GETDAT','STATUS','DIRECT','NODE'])

    ARDY_IN=[]
    #MES包裝入庫+退庫拋送
    for i in range(0,len(df_pda)):
        aufnr = df_pda["AUFNR"][i]
        boxno = df_pda["ZZTB_NO"][i]
        direct = df_pda["DIRECT"][i]
    #     fgdinno = df_pda["ZZCUST_LOT"][i]
        fgdinno = aufnr
        
        print(aufnr)
        #入庫又退庫
        if direct == 'IN_OUT':
            #IN
            xml_data = finishgoodsin_xml(boxno,fgdinno)
            code = send_xml(xml_data)

            #紀錄最後成功筆數>>下次撈取的時間區間
            if code == 200:            
                print(aufnr+":"+boxno+"--XML入庫SUCCESS")            
                
                df_success_in_ = df_pda[df_pda["ZZTB_NO"]==boxno]
                df_success_in_ = df_success_in_.copy()
                df_success_in_["NODE"]="XML"
                df_success_in = pd.concat([df_success_in,df_success_in_])
                
            #紀錄失敗資料>>下次撈取的時間區間        
            else:
                print(aufnr+":"+boxno+"--XML入庫FAIL")
                df_fail_in_ = df_pda[df_pda["ZZTB_NO"]==boxno]
                df_fail_in_ = df_fail_in_.copy()
                df_fail_in_["NODE"]="XML"
                df_fail_in = pd.concat([df_fail_in,df_fail_in_])            
                
            #休息一下    
            time.sleep(1)
                
            #OUT    
            xml_data = finishgoodsout_xml(fgdinno)
            code = send_xml(xml_data)

            #紀錄最後成功筆數>>下次撈取的時間區間
            if code == 200:
                
                print(aufnr+":"+str(boxno)+"--XML退庫SUCCESS")
                df_success_out_ = df_pda[df_pda["AUFNR"]==fgdinno]
                df_success_out_ = df_success_out_.copy()
                df_success_out_["NODE"]="XML"
                df_success_out = pd.concat([df_success_out,df_success_out_])
                
            #紀錄失敗資料>>下次撈取的時間區間        
            else:
                print(aufnr+":"+boxno+"--XML退庫FAIL")
                df_fail_out_ = df_pda[df_pda["ZZTB_NO"]==boxno]
                df_fail_out_ = df_fail_out_.copy()
                df_fail_out_["NODE"]="XML"
                df_fail_out = pd.concat([df_fail_out,df_fail_out_])            
                
                
        
        #入庫
        elif direct == 'IN':
            
            xml_data = finishgoodsin_xml(boxno,fgdinno)
            code = send_xml(xml_data)

            #紀錄最後成功筆數>>下次撈取的時間區間
            if code == 200:
                
                print(aufnr+":"+boxno+"--XML入庫SUCCESS")
                
                df_success_in_ = df_pda[df_pda["ZZTB_NO"]==boxno]
                df_success_in_=df_success_in_.copy()
                df_success_in_["NODE"]="XML"
                df_success_in = pd.concat([df_success_in,df_success_in_])

                #成功後確認MES工單狀態是否變99，若是才做工單關結 > MES和SAP狀態要同步
                time.sleep(1)
                
                sql = "SELECT MOSTATE FROM MES_Test.dbo.TBLOEMOBASIS WHERE AUFNR = '"+aufnr+"'"
                mostate = pd.read_sql(sql,con_mes)
                mostate = mostate["MOSTATE"][0]
                
                if mostate ==99 and aufnr not in ARDY_IN:
                    idbsno = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")

                    df_28A,df_28B1 = CLOSEMO(idbsno,aufnr,MANDT)
              
                    cols =df_28A.columns.tolist()
                    sql = sql_str('thsap.ZPPT0028A',cols)
                    
                    try:
                        sql_val(sql,df_28A,cur,con_sap)        
                    except:
                        df_fail_in_["NODE"]="28A"
                        df_fail_in_ = pd.concat([df_fail_in,df_fail_in_])                        
                        

                    cols =df_28B1.columns.tolist()
                    sql = sql_str('thsap.ZPPT0028B1',cols)
                    
                    try:
                        sql_val(sql,df_28B1,cur,con_sap)                                    
                    except:                        
                        df_fail_in_["NODE"]="28B"
                        df_fail_in_ = pd.concat([df_fail_in,df_fail_in_])    
                        
                    ARDY_IN.append(aufnr)

            #紀錄XML失敗資料>>下次撈取的時間區間        
            else:
                print(aufnr+":"+boxno+"--XML入庫FAIL")
                df_fail_in_ = df_pda[df_pda["ZZTB_NO"]==boxno]
                df_fail_in_ =df_fail_in_.copy()
                df_fail_in_["NODE"]="XML"
                df_fail_in = pd.concat([df_fail_in,df_fail_in_])
                
        #退庫        
        else :
            xml_data = finishgoodsout_xml(fgdinno)
            code = send_xml(xml_data)

            #紀錄最後成功筆數>>下次撈取的時間區間
            if code == 200:            
                print(aufnr+":"+str(boxno)+"--XML退庫SUCCESS")
                df_success_out_ = df_pda[df_pda["AUFNR"]==fgdinno]
                df_success_out_ = df_success_out_.copy()
                df_success_out_["NODE"]="XML"
                df_success_out = pd.concat([df_success_out,df_success_out_])
                
            #紀錄失敗資料>>下次撈取的時間區間        
            else:
                print(aufnr+":"+boxno+"--XML退庫FAIL")
                df_fail_out_ = df_pda[df_pda["AUFNR"]==fgdinno]
                df_fail_out_ = df_fail_out_.copy()
                df_fail_out_["NODE"]="XML"
                df_fail_out = pd.concat([df_fail_out,df_fail_out_])                        
            
            
    df_success_in["STATUS"] = 'SUCCESS'
    df_fail_in["STATUS"] = 'FAIL'
    df_success_out["STATUS"] = 'SUCCESS'
    df_fail_out["STATUS"] = 'FAIL'

    df_success_in["DIRECT"] = 'IN'
    df_fail_in["DIRECT"] = 'IN'
    df_success_out["DIRECT"] = 'OUT'
    df_fail_out["DIRECT"] = 'OUT'

    #成功的只留最後一筆
    df_success_in = df_success_in[df_success_in["GETDAT"]==df_success_in["GETDAT"].max()]
    df_success_out = df_success_out[df_success_out["GETDAT"]==df_success_out["GETDAT"].max()]

    try:
        del df_success_in["index"]
    except:
        print('no index')
        
    try:
        del df_success_out["index"]
    except:
        print('no index')
       
    df_success_in.to_sql('TH_SAPSTOCK_LOG', con=con_mes, if_exists='append', index=False)
    df_success_out.to_sql('TH_SAPSTOCK_LOG', con=con_mes, if_exists='append', index=False)
    df_fail_in.to_sql('TH_SAPSTOCK_LOG', con=con_mes, if_exists='append', index=False)
    df_fail_out.to_sql('TH_SAPSTOCK_LOG', con=con_mes, if_exists='append', index=False)

else :
    print('無終段工單入退庫資料')

if(len(df_nonpda) > 0):
    # #僅工單關結 (前段工單)
    AUFNR_ARR = list(set(df_nonpda["AUFNR"].tolist()))
    if(len(AUFNR_ARR)>0):
        for i,v in enumerate(AUFNR_ARR):
            idbsno = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")

            df_28A,df_28B1 = CLOSEMO(idbsno,v,MANDT)
            cols =df_28A.columns.tolist()
            sql = sql_str('thsap.ZPPT0028A',cols)
            sql_val(sql,df_28A,cur,con_sap)        

            cols =df_28B1.columns.tolist()
            sql = sql_str('thsap.ZPPT0028B1',cols)
            sql_val(sql,df_28B1,cur,con_sap)          

else :
    print('無前段工單入庫資料')


# con_cim.dispose()
con_mes.dispose()
# con_sap.dispose()

# con_mes.close()
# con_cim.close()
# cur.close()

time.sleep(3)
