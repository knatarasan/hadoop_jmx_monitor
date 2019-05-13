# set -x
#https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/Metrics.html


log_dir=log

hourly_report_dir=report/hourly

db_dir=db
mkdir -p log report/weekly report/hourly report/daily db
poll_id=poll_id_`date +%Y_%m_%d_%H_%M_%S`
date_id=`date +%Y_%m_%d`
# date_id=2019_05_06
month_id=`date +%Y_%m`
mkdir -p report/hourly/${date_id}

# log_file=${log_dir}/app_log_${poll_id}.log

log_file=${log_dir}/app_log.log
missing_hosts=${log_dir}/missing_hosts
sql_log=${log_dir}/sql_log.sql

rm ${missing_hosts} ${log_file} ${sql_log}                                 #   TMP to be removed
touch ${log_file} ${missing_hosts} ${sql_log}


#Send hourly files into folder with name as date
dn_hourly_report_file=${hourly_report_dir}/${date_id}/dn_report_${poll_id}.csv

dn_daily_agg_file=report/daily/dn_daily_${date_id}.csv
dn_weekly_agg_file=report/weekly/dn_weekly_${month_id}.csv
# dn_hourly_report_file=${hourly_report_dir}/dn_report_poll_id_2019_05_06_16_49_11.csv


logg(){
    echo "`date +%Y_%m_%d_%H_%M_%S` :  $1" >> ${log_file}
}

logg  "main starts here"

if [ ! -f config/dn_hosts ]
then
  logg  "check for file config/dn_hosts with required host names in it "
  exit 1
fi

##############################################  DN ######################################################
dn_metrics_list=`cat config/dn_metrics_config`
dn_column_list="HostName poll_id "


prep_dn_hourly_table(){

  logg "prep_dn_hourly_table would be executed"
  # This hourly table is ddl is derived from metrics list provided from config file

  #derive column names from config file
  for i in $dn_metrics_list;do
    dn_column_list="${dn_column_list} ${i}"
  done

  dn_column_arr=($dn_column_list)


  dn_hourly_table=dn_hourly

  #Prepare DDL of table
  hourly_tbl_ddl="create table dn_hourly ("

  length=${#dn_column_arr[@]}
  current=0

  for col in "${dn_column_arr[@]}"; do
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



poll_hourly_dn_jmx(){

  #Hourly files is prepared by polling all DNs for every 30 min interval, metrics collected would be
  #stored under report/hourly/yyyy_mm_dd

  logg  "Poll hourly table "
  #initialize row values
  row=""

  #traverse through all slave nodes supplied from File
  while read host_name; do
   curl http://${host_name}:50075/jmx>tmp_${host_name}

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
   for i in ${dn_metrics_list};do
     #"head -1" has been added since metric "NumFailedVolumes" had more than one value
     row="$row"`grep \"${i}\" tmp_${host_name}|head -1|cut -f2 -d ':'`
   done
   row=${row%?}

   echo $row>>${dn_hourly_report_file}

   row=""
  # rm tmp_${host_name}
  done <config/dn_hosts

}


prep_hourly_file(){

  # read all hourly files under report/hourly/yyyy_mm_dd which was pulled by poll_hourly_dn_jmx
  # and load into dn_hourly table


  #connect DB create tabel
  sqlite3 db/dn_jmx.db  "${hourly_tbl_ddl}" >>${sql_log}

  sqlite3 db/dn_jmx.db  ".schema dn_hourly" >>${sql_log}
  #connect DB and import report file into daily table



  for hourly_file in ${hourly_report_dir}/${date_id}/*;do
    echo  ".separator ","\n.import ${hourly_file} dn_hourly" | sqlite3 db/dn_jmx.db
    logg  "** file  ##  ${hourly_file} ## is loaded "
  done


  echo  " Number of rows on dn_hourly  : " >>${sql_log}
  sqlite3 db/dn_jmx.db  "select count(*) from dn_hourly" >>${sql_log}

}

prep_daily_file(){

  # Aggregate across all hourly data and make one daily file and place under report/daily/

  sqlite3 db/dn_jmx.db  "create table dn_daily as select * from  dn_hourly where 0"

  sqlite3 db/dn_jmx.db  ".schema dn_daily"   >>${sql_log}
  #connect DB and import report file into daily table

    aggregate_hourly_table_query="insert into dn_daily select HostName,substr(poll_id,9,10) as date_id"
    for col in ${dn_metrics_list};do
      aggregate_hourly_table_query="${aggregate_hourly_table_query} ,avg(${col})"
    done
    aggregate_hourly_table_query="${aggregate_hourly_table_query} from dn_hourly group by HostName,substr(poll_id,9,10);"

  echo  "Insert into dn_daily \n: ${aggregate_hourly_table_query} \n" >>${sql_log}
  sqlite3 db/dn_jmx.db  "${aggregate_hourly_table_query}"

  echo  ".mode csv\n.output ${dn_daily_agg_file}\nSelect * from dn_daily;\n.quit" | sqlite3 db/dn_jmx.db

  # sqlite3 db/dn_jmx.db  "delete from dn_daily"

}

prep_weekly_file(){
  # Read all files under report/daily/* populate dn_daily --> aggregate and load into dn_weekly and
  # upload it to report/weekly/

  for daily_file in report/daily/*;do
    echo  ".separator ","\n.import ${daily_file} dn_daily" | sqlite3 db/dn_jmx.db
    logg  "** file  ##  ${daily_file} ## is loaded "
  done


  sqlite3 db/dn_jmx.db  "create table dn_weekly as select * from  dn_daily where 0"

  sqlite3 db/dn_jmx.db  ".schema dn_weekly" >> ${sql_log}
  #connect DB and import report file into daily table

    aggregate_weekly_table_query="insert into dn_weekly select HostName,substr(poll_id,1,7) as date_id"
    for col in ${dn_metrics_list};do
      aggregate_weekly_table_query="${aggregate_weekly_table_query} ,avg(${col})"
    done
    aggregate_weekly_table_query="${aggregate_weekly_table_query} from dn_daily group by HostName,substr(poll_id,1,7);"

  echo  "Insert into dn_weekly \n: ${aggregate_weekly_table_query}" >>${sql_log}
  sqlite3 db/dn_jmx.db  "${aggregate_weekly_table_query}"
  echo  ".mode csv\n.output ${dn_weekly_agg_file}\nSelect * from dn_weekly;\n.quit" | sqlite3 db/dn_jmx.db


  # sqlite3 db/dn_jmx.db  "select * from dn_weekly" >>${log_file}
  # sqlite3 db/dn_jmx.db  "delete from dn_weekly"

}

########################################## DN ends ####################################################################
#work on DNs
#work on NMs

health_check(){
  # Read current hourly file , compare with weekly avg , if found +/- 30% of weekly average return health red

  sqlite3 db/dn_jmx.db  "create table dn_current as select * from  dn_hourly where 0"
  echo  ".separator ","\n.import ${dn_hourly_report_file} dn_current" | sqlite3 db/dn_jmx.db

      health_check_query="select cur.HostName"
      for col in ${dn_metrics_list};do
        health_check_query="${health_check_query} ,case when ( (cur.${col}-week.${col})/cur.${col} ) >0.3 then \"${col} :1 ***\" else \"${col} :0\" end"
      done
      health_check_query="${health_check_query} from dn_current cur inner join dn_weekly week  where cur.HostName=week.HostName;"

  echo  "\nhealth check query : \n ${health_check_query}"  >>${sql_log}
  sqlite3 db/dn_jmx.db  "${health_check_query}"  >>${sql_log}

  # sqlite3 db/dn_jmx.db  "delete from dn_weekly"

}

clean_up(){
  # drop db
  rm db/dn_jmx.db
  logg  "\n main ends here"
}
verbo(){
  cat ${log_file}
}


prep_dn_hourly_table
poll_hourly_dn_jmx
prep_hourly_file
prep_daily_file
prep_weekly_file
health_check
# verbo
# clean_up
