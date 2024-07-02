from sqlalchemy import types, create_engine
import pyodbc
import cx_Oracle
from sqlalchemy import text
from sqlalchemy.pool import NullPool


def connect(srv, db):

    if srv == 'MES':

        engine = create_engine(
            'mssql+pyodbc://CIMADMIN:theil4893701@10.21.150.108/'+db+'?charset=utf8mb4&driver=SQL+Server+Native+Client+11.0')
        # con = engine.connect()  # 建立連線

    elif srv == 'BU3BOSSMEN':
        engine = create_engine('mssql+pyodbc://ltcim:theil4893701@10.21.20.149/' + db + '?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')
    
#         engine = create_engine('mssql+pyodbc://:@10.21.20.149/'+db+'?charset=utf8mb4&driver=SQL+Server+Native+Client+11.0')
        # con = engine.connect()  # 建立連線

    return engine     

test = connect('BU3BOSSMEN',r'C:\PROGRAM FILES (X86)\BOSSMEN VIEW SERVER\DATABASE\BVSRV.MDF')
import pandas as pd
sql ="SELECT * from RegistorTable where DateTime >='2024-05-20 00:00:00'"
df = pd.read_sql(sql,test)
