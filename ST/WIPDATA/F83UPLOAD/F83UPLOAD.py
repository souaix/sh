import pandas as pd
import sys
import datetime
from sqlalchemy import text
import os


sys.path.append('/home/cim')
#sys.path.append('C:\\Users\\User\\Desktop\\python')
import connect.connect as cc
import global_fun.sftp_fun as ss



sftp = ss.sftp_upload('mft-ap.st.com','Share/gobmsftp_tonghsing_996Z/Yield','test.csv','/home/cim/global_fun/PublicKeyForST/STSFTP_DSAKEY')
