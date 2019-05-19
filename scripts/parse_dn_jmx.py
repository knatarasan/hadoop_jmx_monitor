import json
import urllib2
import sys


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

config_metrics=config['metrics']
config_keyname=config_metrics.keys()[0]
print config_keyname                    # eg : Hadoop:service=DataNode,name=DataNodeActivity
config_headertype=config_metrics.get('Hadoop:service=DataNode,name=DataNodeActivity')[0].get('headertype')
config_columns=config_metrics.get('Hadoop:service=DataNode,name=DataNodeActivity')[1]
print 'config_columns : ',config_columns
for columnname,datatype in config_columns.iteritems():
    print columnname,':',datatype

#continue from here -------------------------------------

print '--------------'
print config_headertype                 # eg : partial

jmx_key=''
if(config_headertype=='partial'):
    for i in jmx_data['beans']:
        if(i.get('name').find(config_keyname)==0):           # eg => i.get('name') : 'Hadoop:service=DataNode,name=DataNodeActivity-phxhdc19dn0064.phx.paypalinc.com-1019'
            jmx_key=i.get('name')




print 'jmx_key :',jmx_key





    # dn_li=element.get('Hadoop:service=DataNode,name=DataNodeActivity')



# li=jmx_data['beans']

# keylist=[]
# namevalues=[]

# for i in li:
#     print(i.get("name"))
#     namevalues.append(i.get("name"))

# namevalues.sort()

# f=open('../ref/dn_list.txt','w')
# for i in namevalues:
#     f.write(i)
#     f.write('\n')



# for i in li:
#     for j in i:
#         keylist.append(j)


# keylist.sort()

# for i in keylist:
#     f.write(i)
#     f.write('\n')

# for i in li:
#     print(i.keys())
# for i in li:
#     val= i.get("name")
#     if(val.find("DataNodeVolume-/hadoop")>0):
#         print(val)
#         print("WriteIoRateAvgTime : ",i.get("WriteIoRateAvgTime"))


# li2=[]
# for i in li:
#     if( 'SlowPeersReport' in i.keys()):
#         if isinstance(i.get('SlowPeersReport') , unicode ):
#             li2=eval(i.get('SlowPeersReport'))
#         elif isinstance(i.get('SlowPeersReport') , list ):
#             li2=i.get('SlowPeersReport')

# for i in li2:
    # print(i.get('SlowNode'))


#https://weknowinc.com/blog/running-multiple-python-versions-mac-osx
