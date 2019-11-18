mrlog=$1
echo "Log is $mrlog"

logTime(){
echo TIMER: $@ at `date +"%m-%d-%y %H:%M:%S"`
}

## values based on cluster
CLUSTER_MAP=10
#CLUSTER_MAP_SLOTS=6992
echo CLUSTER_MAP_SLOTS $CLUSTER_MAP
CLUSTER_REDUCE=10
#CLUSTER_REDUCE_SLOTS=5224
echo CLUSTER_REDUCE $CLUSTER_REDUCE
TERASORT_VOLUME_IN_GB=1
echo TERASORT_VOLUME_IN_GB $TERASORT_VOLUME_IN_GB
TERASORT_NUMBER_OF_RECORDS=$(( $TERASORT_VOLUME_IN_GB * 10000000 ))
echo TERASORT_NUMBER_OF_RECORDS $TERASORT_NUMBER_OF_RECORDS


hadoop fs -rm -r -skipTrash /tmp/terasort-input
hadoop fs -rm -r -skipTrash /tmp/terasort-output
### TeraGen ###
logTime Starting TeraGen
#hadoop jar ${HADOOP_JAR} teragen ${CLUSTER_OPTS} ${DEFAULT_OPTS}  ${TERASORT_NUMBER_OF_RECORDS}  ${OUT_PUTBASE_DIR}/data/input/terasort_run_k1
time yarn jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar teragen -Dmapred.map.tasks.speculative.execution=false  -Dmapred.map.tasks=${CLUSTER_MAP}  -Dmapred.reduce.tasks=${CLUSTER_REDUCE} ${TERASORT_NUMBER_OF_RECORDS} /tmp/terasort-input 2>>$mrlog
logTime Finished TeraGen



### TeraSort ###
#logTime Starting TeraSort
#time yarn jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples.jar terasort -Dmapred.map.tasks.speculative.execution=false  -Dmapred.map.tasks=${CLUSTER_MAP}  -Dmapred.reduce.tasks=${CLUSTER_REDUCE} /tmp/terasort-input /tmp/terasort-output 2>>$mrlog

#logTime Finished TeraSort

#logfile="terasort_${now}.log"


