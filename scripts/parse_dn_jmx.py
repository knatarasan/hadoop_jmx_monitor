import json,urllib2,sqlite3,os,time,sys





def getDDLStruct(jmx_data):

    features=jmx_data['beans']

    config_metrics=config['metrics']

    table_column_name_dtypes=[]
    table_columns_values=[]


    for config_key in config_metrics:
        config_headertype=config_metrics.get(config_key)[0].get('headertype')
        config_columns=config_metrics.get(config_key)[1]

        if(config_headertype=='partial'):
            # print '# To process partial columns'
            for jmx_feature in features:
                if(jmx_feature.get('name').find(config_key)==0):
                    jmx_key=jmx_feature.get('name')

                    sq_columnname=''

                    for feature_name,feature_dtype in config_columns.iteritems():
                        #Prepare only volume related columns
                        if( jmx_key.find('Hadoop:service=DataNode,name=DataNodeVolume-') ==0):

                            #Prepare column name part

                            # carve out volumen name example from the following pick hadoop09
                            # Hadoop:service=DataNode,name=DataNodeVolume-/hadoop09/hdfs/data
                            volumename=jmx_key[find_nth(jmx_key,'/',1)  +1 : find_nth(jmx_key,'/',2)]
                            sq_columnname=volumename+'_'+ feature_name+' '+feature_dtype
                            table_column_name_dtypes.append(sq_columnname)

                            #Prepare column value part
                            volume_feature_value=jmx_feature.get( feature_name)
                            table_columns_values.append(volume_feature_value)
                        else:

                            #Prepare column name part for non volume columns'
                            sq_columnname=feature_name+' '+feature_dtype
                            table_column_name_dtypes.append(sq_columnname)

                            #Prepare column value part
                            table_columns_values.append(jmx_feature.get( feature_name))

        elif(config_headertype=='full'):
            # print '# To process non partial columns'
            for jmx_feature in features:
                if(jmx_feature.get('name')==config_key):
                    jmx_key=jmx_feature.get('name')

                    sq_columnname=''

                    for feature_name,feature_dtype in config_columns.iteritems():
                        #Prepare column name part
                        sq_columnname=feature_name+' '+feature_dtype
                        table_column_name_dtypes.append(sq_columnname)

                        #Prepare column value part
                        volume_feature_value=jmx_feature.get( feature_name)
                        table_columns_values.append(volume_feature_value)


    # print 'ddl :',table_column_name_dtypes
    # print 'col value ',table_columns_values


    #Prepare DDL for hourly_dn
    hourly_dn_table_ddl='create table if not exists hourly_dn (hostname text,poll_time int,'
    for i in range(0,len(table_column_name_dtypes)):
        if(i!=len(table_column_name_dtypes)-1):
            hourly_dn_table_ddl=hourly_dn_table_ddl+table_column_name_dtypes[i]+',\n'
        else:
            hourly_dn_table_ddl=hourly_dn_table_ddl+table_column_name_dtypes[i]
    hourly_dn_table_ddl=hourly_dn_table_ddl+')'

    # print 'table col name',table_column_name_dtypes


    #Prepare insert statement for  hourly_dn
    hourly_dn_insert='insert into hourly_dn(hostname,poll_time,'

    #Add columns names
    for i in range(0,len(table_column_name_dtypes)):
        if(i!=len(table_column_name_dtypes)-1):
            hourly_dn_insert=hourly_dn_insert+table_column_name_dtypes[i].split()[0]+','
        else:
            hourly_dn_insert=hourly_dn_insert+table_column_name_dtypes[i].split()[0]

    hourly_dn_insert=hourly_dn_insert+') values('

    #Add column values

    #Add hostname and poll_time
    hourly_dn_insert=hourly_dn_insert+'\''+host_name+'\''+','+poll_time+','


    for i in range(0,len(table_columns_values)):
        if(i!=len(table_columns_values)-1):
            hourly_dn_insert=hourly_dn_insert+str(table_columns_values[i])+','
        else:
            hourly_dn_insert=hourly_dn_insert+str(table_columns_values[i])


    hourly_dn_insert=hourly_dn_insert+')'

    # print 'ddl :',hourly_dn_table_ddl
    # print 'insert query :',hourly_dn_insert

    # if os.path.exists('db/hadoop_jmx.db'):
    #     os.remove('db/hadoop_jmx.db')

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



poll_time=time.strftime('%Y%m%d%H%M%S')
host_name=''
dn_port=50075

def find_nth(haystack, needle, n):
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start

def readJsonFile(filename):
    with open(filename) as json_file:
        jsonDict = json.load(json_file)
        return jsonDict


def readJsonURL(url):
    response = urllib2.urlopen(url)
    nnjmx = response.read()
    jsonDict = json.loads(nnjmx)
    return jsonDict

config=readJsonFile('config/dn_metrics_config.json')    # config json for columns to be parsed


f = open('config/dn_hosts', 'r')
hosts=f.read().splitlines()

url=''
for l in hosts:
    host_name=l
    url='http://'+l+':50075/jmx'
    print url
    jmx_data=readJsonURL(url)
    getDDLStruct(jmx_data)


# jmx_data=readJsonFile('../ref/dn0482.json')



# getDDLStruct()
# getDDLStruct('../ref/dn0064.json')



#https://weknowinc.com/blog/running-multiple-python-versions-mac-osx
