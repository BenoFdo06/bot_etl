# -*- coding: utf-8 -*-
"""
Created on Tue Jan 11 11:44:14 2022

@author: Joe Silvan
"""

import requests
from flask import jsonify,request
from multiprocessing import Pool
import cx_Oracle
import pandas as pd 
from sqlalchemy import create_engine
import os
import warnings
warnings.filterwarnings("ignore")
import time
import urllib
import ast
import json 
import flask
app = flask.Flask(__name__)
    


class ETL_BOT():
    def __init__(self,USERNAME,PASSWORD,HOSTNAME,PORT,SID,QUERY,SOURCE_TABLENAME,DES_TABLENAME,STATUS,DEPLOYMENT_SERVER_ID):
        self.USERNAME=USERNAME
        self.PASSWORD=PASSWORD
        self.HOSTNAME=HOSTNAME
        self.PORT=PORT
        self.SID=SID
        self.QUERY=QUERY
        self.SOURCE_TABLENAME=SOURCE_TABLENAME
        self.DES_TABLENAME=DES_TABLENAME
        self.STATUS=STATUS
        self.DEPLOYMENT_SERVER_ID=DEPLOYMENT_SERVER_ID
        
    def connection_test(self):
        if self.STATUS=='Active':
            connection = cx_Oracle.connect(self.USERNAME,self.PASSWORD,self.HOSTNAME+':'+self.PORT+'/'+self.SID)
            cur = connection.cursor()
            query=" select COUNT(*) from tab where tname= '"+self.DES_TABLENAME+"'"
            query=cur.execute(query)
            table_flag=cur.fetchone()[0]
            if table_flag ==0:
                pid = os.getpid()
                print('Bot process id {} for {}'.format(pid, self.DES_TABLENAME))
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
                return('DATA INSERTED into new table')
            else:
                print('self.SOURCE_TABLENAME', self.SOURCE_TABLENAME)
                print('self.DES_TABLENAME', self.DES_TABLENAME)
                query='SELECT * FROM {} minus SELECT * FROM {} '.format(self.SOURCE_TABLENAME,self.DES_TABLENAME)
                df = pd.read_sql_query(query, con=connection)
                size_of_df =len(df)
                print(size_of_df)
                if size_of_df > 0:
                    oracle_connection_string = ('oracle+cx_oracle://{username}:{password}@' +cx_Oracle.makedsn('{hostname}', '{port}', service_name='{service_name}'))
                    engine = create_engine(oracle_connection_string.format(username='ADMIN_UAT',password='ADMIN_UAT',hostname='ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com',port='1521',service_name='ORCL'))
                    connection = cx_Oracle.connect('ADMIN_UAT','ADMIN_UAT','ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com:1521/ORCL')
                    df.to_sql(name=self.DES_TABLENAME,con=engine,if_exists='append',index=False)
                    print('data inserted')
                else:
                    print('no new data')
            time.sleep(10)


def use_etl_bot(configurations):
    # print('inside use etl bot')
    bot = ETL_BOT(configurations[0],configurations[1],configurations[2],configurations[3],configurations[4],configurations[5],configurations[6],configurations[7],configurations[8],configurations[9])
    bot.connection_test()


@app.route('/get_config', methods=['POST'])
def config_get():
    a= request.host_url
    paras = {'a':a}
    content = requests.post(url='http://127.0.0.1:5000/config_bot',json=paras)
    result= json.loads(content.text)
    return result


@app.route('/start_platform', methods=['POST'])
def main():
    while True:
        print('Platform starting  ', request.host_url)
        configurations = config_get()
        print('total bots', len(configurations))
        p= Pool(len(configurations))
        p.map(use_etl_bot, configurations)
        p.close()
        p.join()
        time.sleep(10)
       
   
if __name__ == '__main__':
    app.run(port=5001)
    

