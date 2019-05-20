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


#Send hourly files into folder with name as date

logg(){
    echo "`date +%Y_%m_%d_%H_%M_%S` :  $1" >> ${log_file}
}

logg  "main starts here"


prep_hourly_table(){

  logg "prep_${node}_hourly_table would be executed"
  # This hourly table is ddl is derived from metrics list provided from config file

  #derive column names from config file
  for i in $metrics_list;do
    column_list="${column_list} ${i}"
  done

  column_arr=($column_list)
  # hourly_table=${node}_hourly

  #Prepare DDL of table
  hourly_tbl_ddl="create table ${node}_hourly ("

  length=${#column_arr[@]}
  current=0

  for col in "${column_arr[@]}"; do
    current=$((current + 1))

    if [[ "$current" -eq 1 ]]; then
       hourly_tbl_ddl="${hourly_tbl_ddl} ${col} TEXT,"    # First column with Text datatype
    elif [[ "$current" -eq 2 ]]; then
       hourly_tbl_ddl="${hourly_tbl_ddl} ${col} TEXT,"    # Second  column with Text datatype
    elif [[ "$current" -eq "$length" ]]; then
       hourly_tbl_ddl="${hourly_tbl_ddl} ${col} INT"     # Last column with REAL datatype with no comma
     else
       hourly_tbl_ddl="${hourly_tbl_ddl} ${col} INT,"    # Third to rest of the columns with REAL datatype
     fi
  done

  hourly_tbl_ddl="${hourly_tbl_ddl} );"

}



poll_hourly_jmx(){

  #Hourly files is prepared by polling all nodes for every 30 min interval, metrics collected would be
  #stored under report/hourly/yyyy_mm_dd

  logg  "Poll hourly table ${node}"
  #initialize row values
  row=""

  #traverse through all slave nodes supplied from File
  while read host_name; do
   curl http://${host_name}:${node_port}/jmx>tmp_${host_name}

   if [ $? -ne 0 ]
   then
     echo "${host_name} is not reachable" >>${missing_hosts}
     logg "${host_name} is not reachable"
     rm tmp_${host_name}
     continue
   fi

   #Populate first two columns
   row="${host_name},${poll_id},"

   #grep and populate metrics values , metrics names provided from config file
   for i in ${metrics_list};do
     #"head -1" has been added since metric "NumFailedVolumes" had more than one value
     row="$row"`grep \"${i}\" tmp_${host_name}|head -1|cut -f2 -d ':'`
   done
   row=${row%?}

   echo $row>>${hourly_report_file}

   row=""
  rm tmp_${host_name}
  done <config/dn_hosts

}


prep_hourly_file(){

  # read all hourly files under report/hourly/yyyy_mm_dd which was pulled by poll_hourly_jmx
  # and load into hourly table

  #connect DB create tabel
  sqlite3 db/dn_jmx.db  "${hourly_tbl_ddl}" >>${sql_log}

  sqlite3 db/dn_jmx.db  ".schema ${node}_hourly" >>${sql_log}
  #connect DB and import report file into daily table


  echo "About to start hourly load "
  for hourly_file in ${hourly_report_dir}/${date_id}/${node}*;do
    echo  ".separator ","\n.import ${hourly_file} ${node}_hourly" | sqlite3 db/dn_jmx.db
    echo "After hourly load , value of status code : $?"
      if [ $? -eq 0 ]
      then
        logg  "** file  ##  ${hourly_file} ## is inserted into hourly to  aggregate into daily "
      else
        echo "File load failed"
      fi
  done


  echo  " Number of rows on ${node}_hourly  : " >>${sql_log}
  sqlite3 db/dn_jmx.db  "select count(*) from ${node}_hourly" >>${sql_log}

}

prep_daily_file(){

  # Aggregate across all hourly data and make one daily file and place under report/daily/

  sqlite3 db/dn_jmx.db  "create table ${node}_daily as select * from  ${node}_hourly where 0"

  sqlite3 db/dn_jmx.db  ".schema ${node}_daily"   >>${sql_log}
  #connect DB and import report file into daily table

    aggregate_hourly_table_query="insert into ${node}_daily select HostName,substr(poll_id,9,10) as date_id"
    for col in ${metrics_list};do
      aggregate_hourly_table_query="${aggregate_hourly_table_query} ,avg(${col})"
    done
    aggregate_hourly_table_query="${aggregate_hourly_table_query} from ${node}_hourly group by HostName,substr(poll_id,9,10);"

  echo  "Insert into ${node}_daily Query : \n ${aggregate_hourly_table_query} \n" >>${sql_log}
  sqlite3 db/dn_jmx.db  "${aggregate_hourly_table_query}"

  echo  ".mode csv\n.output ${daily_agg_file}\nSelect * from ${node}_daily;\n.quit" | sqlite3 db/dn_jmx.db

  # sqlite3 db/dn_jmx.db  "delete from dn_daily"

}

prep_weekly_file(){
  # Read all files under report/daily/* populate dn_daily --> aggregate and load into dn_weekly and
  # upload it to report/weekly/

  for daily_file in report/daily/${node}*;do
    echo  ".separator ","\n.import ${daily_file} ${node}_daily" | sqlite3 db/dn_jmx.db
    logg  "** file  ##  ${daily_file} ## is taken for aggregated into weekly  "
  done


  sqlite3 db/dn_jmx.db  "create table ${node}_weekly as select * from  ${node}_daily where 0"

  sqlite3 db/dn_jmx.db  ".schema ${node}_weekly" >> ${sql_log}
  #connect DB and import report file into daily table

    aggregate_weekly_table_query="insert into ${node}_weekly select HostName,substr(poll_id,1,7) as date_id"
    for col in ${metrics_list};do
      aggregate_weekly_table_query="${aggregate_weekly_table_query} ,avg(${col})"
    done
    aggregate_weekly_table_query="${aggregate_weekly_table_query} from ${node}_daily group by HostName,substr(poll_id,1,7);"

  echo  "Insert into ${node}_weekly \n: ${aggregate_weekly_table_query}" >>${sql_log}
  sqlite3 db/dn_jmx.db  "${aggregate_weekly_table_query}"
  echo  ".mode csv\n.output ${weekly_agg_file}\nSelect * from ${node}_weekly;\n.quit" | sqlite3 db/dn_jmx.db


}


health_check(){
  # Read current hourly file , compare with weekly avg , if found +/- 30% of weekly average return health red

  sqlite3 db/dn_jmx.db  "create table ${node}_current as select * from  ${node}_hourly where 0"
  echo  ".separator ","\n.import ${hourly_report_file} ${node}_current" | sqlite3 db/dn_jmx.db

      health_check_query="select cur.HostName"
      for col in ${metrics_list};do
        health_check_query="${health_check_query} ,case when ( (cur.${col}-week.${col})/cur.${col} ) >0.3 then 1 else 0 end as ${col}"
      done
      health_check_query="${health_check_query} from ${node}_current cur inner join ${node}_weekly week  where cur.HostName=week.HostName;"

  echo  "\nhealth check ${node} : \n ${health_check_query}"  >>${sql_log}
  echo  ".separator ","\n.headers on\n.output ${health_check}\n${health_check_query}" | sqlite3 db/dn_jmx.db
  health_check=${log_dir}/health-current-${node}-check_log_${poll_id}.sql
  echo  ".separator ","\n.headers on\n.output ${health_check}\nselect * from ${node}_current;\n.quit" | sqlite3 db/dn_jmx.db

}

clean_up(){
  # drop db
  rm db/dn_jmx.db
  logg  "\n main ends here"
}
verbo(){
  cat ${log_file}
}

check_nn(){
  logg "NN jmx call is made"
  /usr/bin/python2.7 scripts/parse_nn_jmx.py
}

check_dn(){
  echo "NN jmx call is made"
  python scripts/parse_dn_jmx.py

  hourly_file='report/hourly_dn.csv'
  echo  ".mode csv\n.output ${hourly_file}\nSelect * from hourly_dn;\n.quit" | sqlite3 db/hadoop_jmx.db
}


exec_run(){
  health_check=${log_dir}/health-${node}-check_log_${poll_id}.sql
  echo "Run starts for : ${node}"
  check_dn
  # prep_files
  # prep_hourly_table
  # poll_hourly_jmx
  # prep_hourly_file
  # prep_daily_file
  # prep_weekly_file
  # health_check
}


#work on DNs
node=dn
node_port=50075
exec_run

#work on NMs
# node=nm
# node_port=8042
# exec_run

# check_nn

# verbo
# clean_up


#yarn node -list|grep RUNNING|cut -d' ' -f1|cut -d':' -f1>config/nm_hosts
#echo "# of NM hosts : `wc -l config/nm_hosts`"
