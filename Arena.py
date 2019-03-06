# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 23:07:46 2019

@author: nijoshi
"""

import requests
from urllib.request import Request, urlopen
from urllib.request import urlopen, URLError, HTTPError
import urllib.error
import json
import pandas as pd
from pandas.io.json import json_normalize
import os
import csv


ConnID_data_read=pd.read_csv('ConnID.csv', delimiter = ',')
df = pd.DataFrame(ConnID_data_read)



user_iics_loginURL=' https://dm-us.informaticacloud.com/ma/api/v2/user/login'

headers = {
        'Content-Type': "application/json",
        'Accept': "application/json",
        'cache-control': "no-cache"
        
        }


payload = "{\r\n\"@type\": \"login\",\r\n\"username\": \"nijoshi@informatica.com.nutanix\",\r\n\"password\": \"Informatica1\"\r\n}"

response = requests.request("POST", user_iics_loginURL, data=payload, headers=headers)
resp_obj = json.loads(response.text)
session_id = resp_obj['icSessionId']
server_URL = resp_obj['serverUrl']
print(session_id)
Finaldf = pd.DataFrame()
ListDF=[]
for index, row in df.iterrows():
    
    api_ver="/api/v2/connection/"+row['id']
    #print(api_ver)
    conndetails_url = server_URL+api_ver
    print(conndetails_url)
    act_headers = {
    'icSessionId': session_id,
    'Content-Type': "application/json",
    'cache-control': "no-cache",
    
    }
    act_response = requests.get(conndetails_url.strip(),headers=act_headers)
    print(act_response.text)
    print("Creating Data Frame on this***********************")
    act_json_data= json.loads(act_response.text)
    f = csv.writer(open("test.csv", "wb+"))
    flat_json = json_normalize(act_json_data)
    print(flat_json)
    row['id'] = pd.DataFrame(flat_json)
    ListDF.append(row['id'])
    print(ListDF)
    result=pd.concat(ListDF)
    
result.to_csv('NewTest.csv')

#print(MasterDict)
#Masterdf = pd.DataFrame(MasterDict)
    
#print(Masterdf)

'''
def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[str(name[:-1])] = str(x)

    flatten(y)
    return out

flat = flatten_json(act_json_data)
print("**************Flat JSON Function****************")
print(flat)

for index, row in df.iterrows():
    #print(row['id'])
    #api_ver="/api/v2/connection/"+row['id']
    api_ver="/api/v2/connection/"
    #print(api_ver)
    conndetails_url = server_URL+api_ver
    print(conndetails_url)
    act_headers = {
    'icSessionId': session_id,
    'Content-Type': "application/json",
    'cache-control': "no-cache",
    
    }
    act_response = requests.get(conndetails_url.strip(),headers=act_headers)
    print(act_response.text)
    print("Creating Data Frame on this***********************")
    act_json_data= json.loads(act_response.text)
    flat_json = json_normalize(act_json_data)
    print(flat_json)
    
    def flatten_json(y):
        out = {}
    
        def flatten(x, name=''):
            if type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + '_')
            elif type(x) is list:
                i = 0
                for a in x:
                    flatten(a, name + str(i) + '_')
                    i += 1
            else:
                out[str(name[:-1])] = str(x)
    
        flatten(y)
        return out

    flat = flatten_json(act_json_data)
    print("**************Flat JSON Function****************")
    print(flat)

    #ConnDetaildf = pd.DataFrame(act_json_data)
    #print (ConnDetaildf.describe())
    #dfConnDetail = pd.DataFrame(act_response)
    #print(dfConnDetail.describe())


#Now using this session we are calling activity log 
# https://na1.dm-us.informaticacloud.com/saas/api/v2/activity/activityLog"
#api_ver="/api/v2/connection/activityLog?rowLimit=1000"
#activity_url = server_URL+api_ver
#print(activity_url)
                

act_response = requests.get(conndetails_url.strip(),headers=act_headers)
# print(act_response.text)
print("Now loading Activity Log Data:-")


df = pd.DataFrame(act_json_data)
print (df.describe())
subjson=df['entries'][9]
print (subjson)
print (type(subjson))
print (len(subjson))
print(subjson[0])
json_string = json.dumps(subjson)
print(type(json_string))
newjson=json.loads(json_string)
print(type(newjson))
dfnew = pd.DataFrame(newjson)
print(dfnew.describe())
'''