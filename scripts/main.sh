# set -x
#https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/Metrics.html


log_dir=log

hourly_report_dir=report/hourly

db_dir=db
mkdir -p log report/ db
poll_id=poll_id_`date +%Y_%m_%d_%H_%M_%S`
date_id=`date +%Y_%m_%d`
# date_id=2019_05_06
month_id=`date +%Y_%m`

log_file=${log_dir}/app_log_${poll_id}.log

missing_hosts=${log_dir}/missing_hosts
sql_log=${log_dir}/sql_log.sql


rm ${missing_hosts} ${log_file} ${sql_log}                                 #   TMP to be removed
touch ${log_file} ${missing_hosts} ${sql_log}

logg(){
#    echo "`date +%Y_%m_%d_%H_%M_%S` :  $1" >> ${log_file}
    echo "`date +%Y_%m_%d_%H_%M_%S` :  $1"
}

logg  "main starts here"


if [ ! -f config/dn_hosts ]
then
  logg  "check for file config/dn_hosts with required host names in it "
  exit 1
elif [ ! -f config/nm_hosts ]
then
  logg  "check for file config/nm_hosts with required host names in it "
  exit 1
elif [ ! -f config/dn_metrics_config ]
then
  logg  "check for file config/dn_metrics_config with required host names in it "
  exit 1
elif [ ! -f config/nm_metrics_config ]
then
  logg  "check for file config/nm_metrics_config with required host names in it "
  exit 1
fi



#health_check(){
#  # Read current hourly file , compare with weekly avg , if found +/- 30% of weekly average return health red
#
#  sqlite3 db/dn_jmx.db  "create table ${node}_current as select * from  ${node}_hourly where 0"
#  echo  ".separator ","\n.import ${hourly_report_file} ${node}_current" | sqlite3 db/dn_jmx.db
#
#      health_check_query="select cur.HostName"
#      for col in ${metrics_list};do
#        health_check_query="${health_check_query} ,case when ( (cur.${col}-week.${col})/cur.${col} ) >0.3 then 1 else 0 end as ${col}"
#      done
#      health_check_query="${health_check_query} from ${node}_current cur inner join ${node}_weekly week  where cur.HostName=week.HostName;"
#
#  echo  "\nhealth check ${node} : \n ${health_check_query}"  >>${sql_log}
#  echo  ".separator ","\n.headers on\n.output ${health_check}\n${health_check_query}" | sqlite3 db/dn_jmx.db
#  health_check=${log_dir}/health-current-${node}-check_log_${poll_id}.sql
#  echo  ".separator ","\n.headers on\n.output ${health_check}\nselect * from ${node}_current;\n.quit" | sqlite3 db/dn_jmx.db
#
#}

clean_up(){
  # drop db
  rm db/dn_jmx.db
  logg  "\n main ends here"
}
verbo(){
  cat ${log_file}
}

collect_master_jmx(){
  logg "NN jmx call is made"
  /usr/bin/python2.7 scripts/parse_nn_jmx.py
}

collect_worker_jmx(){
  echo "jmx call is made to collect hourly metrcis from dataNode and nodeManager matrics"
  python scripts/parse_dn_jmx.py

  dn_hourly_file='report/hourly_dn.csv'
  nm_hourly_file='report/hourly_nm.csv'

  echo  ".mode csv\n.headers on\n.output ${dn_hourly_file}\nSelect * from hourly_dn;\n.quit" | sqlite3 db/hadoop_jmx.db
  echo  ".mode csv\n.headers on\n.output ${nm_hourly_file}\nSelect * from hourly_nm;\n.quit" | sqlite3 db/hadoop_jmx.db
}


exec_run(){
#  collect_worker_jmx
  collect_master_jmx
  # email slow node list if it is not null
  sendmail knatarasan@cloudera.com < log/SlowNode.txt
#  health_check
}


#work on DNs
node=dn
node_port=50075
exec_run


# check_nn
# verbo
# clean_up


#yarn node -list|grep RUNNING|cut -d' ' -f1|cut -d':' -f1>config/nm_hosts
#echo "# of NM hosts : `wc -l config/nm_hosts`"
