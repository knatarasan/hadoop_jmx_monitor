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

data=readJsonFile('../ref/dn0064.json')
config=readJsonFile('config/dn_metrics_config.json')
# data=readJsonURL(url)


# read metrics --> get keys --> check headertype
dict=config['metrics'][0]
lis=dict.get( dict.keys()[0] )

keyname=dict.keys()[0] # eg : Hadoop:service=DataNode,name=DataNodeActivity

if(lis[0].get('headertype')=='partial'):
    for i in data['beans']:
        if(i.get('name').find(keyname)==0):           # eg => i.get('name') : 'Hadoop:service=DataNode,name=DataNodeActivity-phxhdc19dn0064.phx.paypalinc.com-1019'
            print i.get('name')



    #     print lis
    #         for j in lis
    #             print j


    # dn_li=element.get('Hadoop:service=DataNode,name=DataNodeActivity')



# li=data['beans']

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
