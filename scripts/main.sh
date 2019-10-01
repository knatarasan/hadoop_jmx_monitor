# set -x
#https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/Metrics.html


log_dir=log

hourly_report_dir=report/hourly

db_dir=db
mkdir -p log report/ db
poll_id=poll_id_`date +%Y_%m_%d_%H_%M_%S`
date_id=`date +%Y_%m_%d`
month_id=`date +%Y_%m`

log_file=${log_dir}/app_log_${poll_id}.log

missing_hosts=${log_dir}/missing_hosts
sql_log=${log_dir}/sql_log.sql


rm ${missing_hosts} ${log_file} ${sql_log}                                 #   TMP to be removed
touch ${log_file} ${missing_hosts} ${sql_log}

logg(){
    echo "`date +%Y_%m_%d_%H_%M_%S` :  $1" >> ${log_file}
#    echo "`date +%Y_%m_%d_%H_%M_%S` :  $1"
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
  logg "jmx call is made to collect hourly metrcis from dataNode and nodeManager matrics"
  python scripts/parse_dn_jmx.py

  dn_hourly_file='report/hourly_dn.csv'
  nm_hourly_file='report/hourly_nm.csv'

  echo  ".mode csv\n.headers on\n.output ${dn_hourly_file}\nSelect * from hourly_dn;\n.quit" | sqlite3 db/hadoop_jmx.db
  echo  ".mode csv\n.headers on\n.output ${nm_hourly_file}\nSelect * from hourly_nm;\n.quit" | sqlite3 db/hadoop_jmx.db
}

send_mail(){
  logg `cat log/SlowNode.txt`
  sendmail `cat config/email_list` < log/SlowNode.txt
}


exec_run(){
#  collect_worker_jmx
    collect_master_jmx
    # email slow node list if it is not null
    send_mail
#  health_check
}


exec_run


# check_nn
# verbo
# clean_up


#yarn node -list|grep RUNNING|cut -d' ' -f1|cut -d':' -f1>config/nm_hosts
#echo "# of NM hosts : `wc -l config/nm_hosts`"
