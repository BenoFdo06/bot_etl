# -*- coding: utf-8 -*-
"""
Created on Tue Jan 11 11:44:14 2022

@author: Joe Silvan
"""

from multiprocessing import Pool
import cx_Oracle
import pandas as pd 
from sqlalchemy import create_engine
import os
import warnings
warnings.filterwarnings("ignore")

    
class ETL_BOT():
    def __init__(self,USERNAME,PASSWORD,HOSTNAME,PORT,SID,QUERY,DES_TABLENAME):
        self.USERNAME=USERNAME
        self.PASSWORD=PASSWORD
        self.HOSTNAME=HOSTNAME
        self.PORT=PORT
        self.SID=SID
        self.QUERY=QUERY
        self.DES_TABLENAME=DES_TABLENAME
        
    def connection_test(self):
        pid = os.getpid()
        print('Bot process id {} for {}'.format(pid, self.DES_TABLENAME))
        connection = cx_Oracle.connect(self.USERNAME,self.PASSWORD,self.HOSTNAME+':'+self.PORT+'/'+self.SID)
        cur = connection.cursor()
        df = pd.read_sql_query(self.QUERY, con=connection)
        oracle_connection_string = ('oracle+cx_oracle://{username}:{password}@' +cx_Oracle.makedsn('{hostname}', '{port}', service_name='{service_name}'))
        engine = create_engine(oracle_connection_string.format(username='ADMIN_UAT',password='ADMIN_UAT',hostname='ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com',port='1521',service_name='ORCL'))
        connection = cx_Oracle.connect('ADMIN_UAT','ADMIN_UAT','ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com:1521/ORCL')
        Text_list = []
        for i in range(df.shape[1]):
            Col_Name = df.columns[i]
            Python = df.convert_dtypes().dtypes[i]
            if Python == float:
                Oralce = "FLOAT"
            elif Python == 'datetime64[ns]':
                Oracle = 'DATE'
            elif Python == 'Int64':
                Oracle = "NUMBER"
            else:
                Oracle = "VARCHAR(100)"
            Text_list.append(Col_Name)
            Text_list.append(' ')
            Text_list.append(Oracle)
            if i < (df.shape[1] - 1):
                Text_list.append(", ")
        Text_Block = ''.join(Text_list)
        Text_Block    
        print('process id :',os.getpid(), 'table : ',self.DES_TABLENAME)
    
        create ="""CREATE TABLE {} ({})""".format(self.DES_TABLENAME,Text_Block)
        print(create)
        cur.execute(create)
        connection.commit()
        data=df.head(10)
        data.to_sql(name=self.DES_TABLENAME,con=engine,if_exists='append',index=False)
        #connection.close()
        return('DATA INSERTED')


def use_etl_bot(configurations):
    # print('inside use etl bot')
    bot = ETL_BOT(configurations[0],configurations[1],configurations[2],configurations[3],configurations[4],configurations[5],configurations[6])
    bot.connection_test()

def config():
    config = [['ADMIN_UAT','ADMIN_UAT','ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com','1521','ORCL','SELECT VERTICAL,PRIMARY_SOL_ID,SOL_ID FROM STG_ACNT_SMA','DES_TEST301'],
                  ['ADMIN_UAT','ADMIN_UAT','ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com','1521','ORCL','SELECT VERTICAL,PRIMARY_SOL_ID,SOL_ID FROM STG_ACNT_SMA','DES_TEST302'],
                  ['ADMIN_UAT','ADMIN_UAT','ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com','1521','ORCL','SELECT VERTICAL,PRIMARY_SOL_ID,SOL_ID FROM STG_ACNT_SMA','DES_TEST303'],
                  ['ADMIN_UAT','ADMIN_UAT','ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com','1521','ORCL','SELECT VERTICAL,PRIMARY_SOL_ID,SOL_ID FROM STG_ACNT_SMA','DES_TEST304'],
                  ['ADMIN_UAT','ADMIN_UAT','ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com','1521','ORCL','SELECT VERTICAL,PRIMARY_SOL_ID,SOL_ID FROM STG_ACNT_SMA','DES_TEST305'],
                  ['ADMIN_UAT','ADMIN_UAT','ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com','1521','ORCL','SELECT VERTICAL,PRIMARY_SOL_ID,SOL_ID FROM STG_ACNT_SMA','DES_TEST306'],
                  ['ADMIN_UAT','ADMIN_UAT','ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com','1521','ORCL','SELECT VERTICAL,PRIMARY_SOL_ID,SOL_ID FROM STG_ACNT_SMA','DES_TEST307'],
                  ['ADMIN_UAT','ADMIN_UAT','ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com','1521','ORCL','SELECT VERTICAL,PRIMARY_SOL_ID,SOL_ID FROM STG_ACNT_SMA','DES_TEST308'],
                  ['ADMIN_UAT','ADMIN_UAT','ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com','1521','ORCL','SELECT VERTICAL,PRIMARY_SOL_ID,SOL_ID FROM STG_ACNT_SMA','DES_TEST309'],
                  ['ADMIN_UAT','ADMIN_UAT','ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com','1521','ORCL','SELECT VERTICAL,PRIMARY_SOL_ID,SOL_ID FROM STG_ACNT_SMA','DES_TEST310']]
    return config


def main():
    configurations = config()
    print('total bots', len(configurations))
    p= Pool(len(configurations))
    p.map(use_etl_bot, configurations)
    p.close()
    p.join()
    
    

if __name__ == '__main__':
    main()
