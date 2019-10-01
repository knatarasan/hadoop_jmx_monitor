import json
import urllib2
url = "http://cent7-hdp-1.field.hortonworks.com:50070/jmx"

print("url ",url)

def readJsonFile(filename):
    with open(filename) as json_file:
        jsonDict = json.load(json_file)
        return jsonDict


def readJsonURL(url):
    response = urllib2.urlopen(url)
    nnjmx = response.read()
    jsonDict = json.loads(nnjmx)
    return jsonDict

# data=readJsonFile('voyager.jmx.json')
data=readJsonURL(url)

li=data['beans']

li2=[]
for i in li:
    if( 'SlowPeersReport' in i.keys()):
        if isinstance(i.get('SlowPeersReport') , unicode ):
            li2=eval(i.get('SlowPeersReport'))
        elif isinstance(i.get('SlowPeersReport') , list ):
            li2=i.get('SlowPeersReport')

print('slowNodes')
# print(li2)
# for i in li2:
#     print('SN ',i.get('SlowNode'))
#     print('RN ',i.get('ReportingNodes'))


f=open('log/SlowNode.txt', 'w')
print >> f,('Voyager has following slow Nodes')
print >> f,('---------------------------------')
for i in li2:
    print >> f,(i.get('SlowNode'))
    # print >> f, 'Filename:', filename     # Python 2.x

print >> f,('---------------------------------')