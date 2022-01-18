# -*- coding: utf-8 -*-
"""
Created on Tue Jan 11 11:44:14 2022

@author: Joe Silvan
"""

from flask import jsonify
from multiprocessing import Pool
import cx_Oracle
import pandas as pd 
from sqlalchemy import create_engine
import os
import warnings
warnings.filterwarnings("ignore")
import time
import urllib
import json 
import flask
from flask_cors import CORS, cross_origin
app = flask.Flask(__name__)
CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
    


class ETL_BOT():
    def __init__(self,USERNAME,PASSWORD,HOSTNAME,PORT,SID,QUERY,SOURCE_TABLENAME,DES_TABLENAME,STATUS):
        self.USERNAME=USERNAME
        self.PASSWORD=PASSWORD
        self.HOSTNAME=HOSTNAME
        self.PORT=PORT
        self.SID=SID
        self.QUERY=QUERY
        self.SOURCE_TABLENAME=SOURCE_TABLENAME
        self.DES_TABLENAME=DES_TABLENAME
        self.STATUS=STATUS
        
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
    bot = ETL_BOT(configurations[0],configurations[1],configurations[2],configurations[3],configurations[4],configurations[5],configurations[6],configurations[7],configurations[8])
    bot.connection_test()

def config():
    connection = cx_Oracle.connect('ADMIN_UAT','ADMIN_UAT','ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com:1521/ORCL')
    cur = connection.cursor()
    config=cur.execute("select source_username,source_password,source_hostname,source_port,source_sid,query_text,source_table_name,destination_table_name,status from BOT_LIST_CONFIG_DETAILS where status='Active'").fetchall()
    return config


@app.route('/get_config', methods=['GET'])
def config_get():
    content = urllib.request.urlopen('http://192.168.1.214:6001/config_bot').read().decode('utf-8')
    return jsonify({ 'config' : json.loads(content)})


@app.route('/start_platform', methods=['GET'])
def main():
    while True:
        configurations = config()
        print('total bots', len(configurations))
        p= Pool(len(configurations))
        p.map(use_etl_bot, configurations)
        p.close()
        p.join()
        time.sleep(10)
       
   
if __name__ == '__main__':
    app.run(host='192.168.1.214',port=6002)
    
