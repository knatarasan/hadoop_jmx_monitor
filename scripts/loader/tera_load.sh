now=`date +"%m-%d-%y#%H-%M-%S"`

mkdir -p log
logfile=log/"teragensort_${now}.log"

sh teragensort.sh $logfile > $logfile
