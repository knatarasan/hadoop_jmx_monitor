import json
import urllib2
import sys
import sqlite3
import os

def readJsonFile(filename):
    with open(filename) as json_file:
        jsonDict = json.load(json_file)
        return jsonDict


def readJsonURL(url):
    response = urllib2.urlopen(url)
    nnjmx = response.read()
    jsonDict = json.loads(nnjmx)
    return jsonDict

jmx_data=readJsonFile('../ref/dn0064.json')                 # jmx file to be parsed
config=readJsonFile('config/dn_metrics_config.json')    # config json for columns to be parsed
# jmx_data=readJsonURL(url)

# read metrics --> get keys --> check headertype

features=jmx_data['beans']

# def getJmxData(config_headertype,config_key,config_columns):
    # print 'getJmxData',config_headertype,config_key,config_columns


def getDDLStruct():
    config_metrics=config['metrics']

    print 'to process partial columns - volume columns'
    for config_key in config_metrics:
        config_headertype=config_metrics.get(config_key)[0].get('headertype')
        config_columns=config_metrics.get(config_key)[1]
        # getJmxData(config_headertype,config_key,config_columns)

        table_columns_ddl=[]
        table_columns_values=[]
        # to process only partial columns
        if(config_headertype=='partial'):
            for jmx_feature in features:
                if(jmx_feature.get('name').find(config_key)==0):
                    jmx_key=jmx_feature.get('name')

                    for volume_feature_name,volume_feature_dtype in config_columns.iteritems():
                        if( jmx_key.find('Hadoop:service=DataNode,name=DataNodeVolume-') ==0):

                            #Prepare column name part
                            disknum=jmx_key.find('/')
                            volumename=jmx_key[disknum+1:disknum+9]
                            sq_columnname=volumename+'_'+ volume_feature_name+' '+volume_feature_dtype
                            table_columns_ddl.append(sq_columnname)

                            #Prepare column value part
                            volume_feature_value=jmx_feature.get( volume_feature_name)
                            table_columns_values.append(volume_feature_value)

                            # print 'jmx_key :',jmx_key,' k :',k,' value_from_jmx: ',i.get(k)

        else:
            print 'to process non partial columns'


    # print table_columns_ddl
    print table_columns_values

    #Prepare DDL for hourly_dn
    hourly_dn_table_ddl='create table hourly_dn('
    for i in range(0,len(table_columns_ddl)):
        if(i!=len(table_columns_ddl)-1):
            hourly_dn_table_ddl=hourly_dn_table_ddl+table_columns_ddl[i]+',\n'
        else:
            hourly_dn_table_ddl=hourly_dn_table_ddl+table_columns_ddl[i]
    hourly_dn_table_ddl=hourly_dn_table_ddl+')'

    # print hourly_dn_table_ddl


    #Prepare insert statement for  hourly_dn
    hourly_dn_insert='insert into hourly_dn values('
    for i in range(0,len(table_columns_values)):
        if(i!=len(table_columns_values)-1):
            hourly_dn_insert=hourly_dn_insert+str(table_columns_values[i])+','
        else:
            hourly_dn_insert=hourly_dn_insert+str(table_columns_values[i])

    hourly_dn_insert=hourly_dn_insert+')'

    print hourly_dn_insert

    os.remove('db/hadoop_jmx.db')

    conn = sqlite3.connect('db/hadoop_jmx.db')

    c = conn.cursor()
    c.execute(hourly_dn_table_ddl)          #create table
    c.execute(hourly_dn_insert)             #insert into  table
    conn.commit()
    conn.close()

    conn = sqlite3.connect('db/hadoop_jmx.db')
    c = conn.cursor()
    print 'select query'
    for row in c.execute('select * from hourly_dn'):
        print row

    conn.close()



print getDDLStruct()

#https://weknowinc.com/blog/running-multiple-python-versions-mac-osx
