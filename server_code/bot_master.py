# -*- coding: utf-8 -*-
"""
Created on Tue Jan 18 08:04:17 2022

@author: Joe Silvan
"""

from flask import request,jsonify
import cx_Oracle
import json
import warnings
import pandas as pd
warnings.filterwarnings("ignore")
import flask
from flask_cors import CORS, cross_origin
app = flask.Flask(__name__)
CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

def connection_establish(USERNAME,PASSWORD,HOSTNAME,PORT,SID):
    connection = cx_Oracle.connect(USERNAME,PASSWORD,HOSTNAME+':'+PORT+'/'+SID)
    cur = connection.cursor()
    return cur,connection




@app.route('/Create_bot',methods=['POST'])
def bot_create():
    print(" inside create bot")
    USERNAME='ADMIN_UAT'
    PASSWORD='ADMIN_UAT'
    HOSTNAME='ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com'
    PORT= '1521'
    SID= 'ORCL'
    # print('SID', SID)
    cur, connection = connection_establish(USERNAME,PASSWORD,HOSTNAME,PORT,SID)
    SOURCE_TABLE_NAME = request.form['SOURCE_TABLE_NAME']
    SOURCE_USERNAME   = request.form['SOURCE_USERNAME']
    SOURCE_PASSWORD   = request.form['SOURCE_PASSWORD']
    SOURCE_HOSTNAME = request.form['SOURCE_HOSTNAME']     
    SOURCE_PORT  = request.form['SOURCE_PORT']    
    SOURCE_SID  = request.form['SOURCE_SID']      
    DESTINATION_TABLE_NAME     = request.form['DESTINATION_TABLE_NAME']  
    DESTINATION_USERNAME =  request.form['DESTINATION_USERNAME']  
    DESTINATION_PASSWORD = request.form['DESTINATION_PASSWORD']
    DESTINATION_HOSTNAME  = request.form['DESTINATION_HOSTNAME']   
    DESTINATION_PORT         =request.form['DESTINATION_PORT']
    DESTINATION_SID         =request.form['DESTINATION_SID']
    QUERY_TEXT            =  request.form['QUERY_TEXT']
    print(QUERY_TEXT)
    CREATED_BY    = 'user1'
    MODIFIED_BY  =  'user1'
    BOT_TYPE='ETL-BOT'
    BOT_SUB_TYPE='QUERY_LOAD'
    latestrecord = cur.execute("select count(*) from BOT_LIST_CONFIG_DETAILS")
    latestrecord = cur.fetchone()
    print('latestrecord', latestrecord)
    BOT_LIST_UNIQUE_ID='ETL-BOT'+str(latestrecord[0]+1)
    STATUS='Inactive'
    cur.execute("INSERT INTO BOT_LIST_CONFIG_DETAILS (BOT_LIST_UNIQUE_ID,BOT_TYPE,BOT_SUB_TYPE,SOURCE_TABLE_NAME,SOURCE_USERNAME,SOURCE_PASSWORD,SOURCE_HOSTNAME,SOURCE_PORT,SOURCE_SID,DESTINATION_TABLE_NAME,DESTINATION_USERNAME,DESTINATION_PASSWORD,DESTINATION_HOSTNAME,DESTINATION_PORT,DESTINATION_SID,QUERY_TEXT,CREATED_BY,MODIFIED_BY,STATUS) VALUES (:BOT_LIST_UNIQUE_ID,:BOT_TYPE,:BOT_SUB_TYPE,:SOURCE_TABLE_NAME,:SOURCE_USERNAME,:SOURCE_PASSWORD,:SOURCE_HOSTNAME,:SOURCE_PORT,:SOURCE_SID,:DESTINATION_TABLE_NAME,:DESTINATION_USERNAME,:DESTINATION_PASSWORD,:DESTINATION_HOSTNAME,:DESTINATION_PORT,:DESTINATION_SID,:QUERY_TEXT,:CREATED_BY,:MODIFIED_BY,:STATUS)",{'BOT_LIST_UNIQUE_ID':BOT_LIST_UNIQUE_ID,'BOT_TYPE':BOT_TYPE,'BOT_SUB_TYPE':BOT_SUB_TYPE, 'SOURCE_TABLE_NAME':SOURCE_TABLE_NAME,'SOURCE_USERNAME':SOURCE_USERNAME,'SOURCE_PASSWORD':SOURCE_PASSWORD,'SOURCE_HOSTNAME':SOURCE_HOSTNAME,'SOURCE_PORT':SOURCE_PORT,'SOURCE_SID':SOURCE_SID,'DESTINATION_TABLE_NAME':DESTINATION_TABLE_NAME,'DESTINATION_USERNAME':DESTINATION_USERNAME,'DESTINATION_PASSWORD':DESTINATION_PASSWORD,'DESTINATION_HOSTNAME':DESTINATION_HOSTNAME,'DESTINATION_PORT':DESTINATION_PORT,'DESTINATION_SID':DESTINATION_SID,'QUERY_TEXT':QUERY_TEXT,'CREATED_BY':CREATED_BY,'MODIFIED_BY':MODIFIED_BY,'STATUS':STATUS})
    connection.commit()
    return ('BOT CREATED..........')


@app.route('/start_bot',methods=['POST'])
def bot_start():
    connection = cx_Oracle.connect('ADMIN_UAT','ADMIN_UAT','ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com:1521/ORCL')
    cur = connection.cursor()
    BOT_LIST_UNIQUE_ID=request.form['BOT_LIST_UNIQUE_ID']
    cur.execute("UPDATE BOT_LIST_CONFIG_DETAILS SET status = 'Active' WHERE BOT_LIST_UNIQUE_ID='"+BOT_LIST_UNIQUE_ID+"'")
    connection.commit()
    return 'Bot started'



@app.route('/stop_bot',methods=['POST'])
def bot_stop():
    connection = cx_Oracle.connect('ADMIN_UAT','ADMIN_UAT','ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com:1521/ORCL')
    cur = connection.cursor()
    BOT_LIST_UNIQUE_ID=request.form['BOT_LIST_UNIQUE_ID']
    cur.execute("UPDATE BOT_LIST_CONFIG_DETAILS SET status = 'Inactive' WHERE BOT_LIST_UNIQUE_ID='"+BOT_LIST_UNIQUE_ID+"'")
    connection.commit()
    return 'Bot stopped'



def bot_list():
	connection = cx_Oracle.connect('ADMIN_UAT','ADMIN_UAT','ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com:1521/ORCL')
	cur = connection.cursor()
	SQL1="select bot_list_unique_id,bot_type,status from BOT_LIST_CONFIG_DETAILS"
	bot_li=pd.read_sql_query(SQL1, con=connection)
	return bot_li

def active_bot_list():
    connection = cx_Oracle.connect('ADMIN_UAT','ADMIN_UAT','ewss2db-mum.csujspl2bezo.ap-south-1.rds.amazonaws.com:1521/ORCL')
    cur = connection.cursor()
    bot_li=cur.execute("select source_username,source_password,source_hostname,source_port,source_sid,query_text,source_table_name,destination_table_name,status from BOT_LIST_CONFIG_DETAILS where status='Active'").fetchall()
    return bot_li

@app.route('/list_bot',methods=['get'])
def test():
	res = bot_list()
	res1= res.to_dict('records')
	result = json.dumps(res1)
	return result


@app.route('/config_bot', methods=['GET'])
def config_bot():
    config=active_bot_list()
    return jsonify(config)


if __name__ == '__main__':
    app.run(host='192.168.1.214',port=6001)