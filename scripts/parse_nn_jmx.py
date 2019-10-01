import json,datetime,urllib2



nn_hostname=''
#picks the first line from nn_hosts
with open('config/nn_hosts') as host_file:
    #To avoid newline char at the end of line
    nn_hostname=host_file.read().splitlines()[0]



url = 'http://'+nn_hostname+':50070/jmx'


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

slownodes=[]
for i in li:
    if( 'SlowPeersReport' in i.keys()):
        if isinstance(i.get('SlowPeersReport') , unicode ):
            slownodes=eval(i.get('SlowPeersReport'))
        elif isinstance(i.get('SlowPeersReport') , list ):
            slownodes=i.get('SlowPeersReport')


datetime=datetime.datetime.utcnow()
f=open('log/SlowNode.txt', 'w')
print >> f,'Subject:Slow nodes reported by NN :',nn_hostname,' at ',datetime
print >> f,('---------------------------------')
for i in slownodes:
    print >> f,(i.get('SlowNode'))
    # print >> f, 'Filename:', filename     # Python 2.x

print >> f,('---------------------------------')