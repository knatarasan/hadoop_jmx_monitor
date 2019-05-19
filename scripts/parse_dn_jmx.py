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

def getDDLStruct():
    config_metrics=config['metrics']

    for config_key in config_metrics:
        config_headertype=config_metrics.get(config_key)[0].get('headertype')
        config_columns=config_metrics.get(config_key)[1]
        # print config_key,':',config_headertype
        for column,dtype in config_columns.iteritems():
            # print column,':',dtype

            jmx_key=''
            if(config_headertype=='partial'):
                for i in jmx_data['beans']:
                    if(i.get('name').find(config_key)==0):      # eg => i.get('name') : 'Hadoop:service=DataNode,name=DataNodeActivity-phxhdc19dn0064.phx.paypalinc.com-1019'
                        jmx_key=i.get('name')
                        # print 'jmx_key : ',jmx_key,' config_columns :',config_columns
                        print jmx_key,':',column,' : ',i.get(column)



    print '    --- '

print getDDLStruct()

#https://weknowinc.com/blog/running-multiple-python-versions-mac-osx
