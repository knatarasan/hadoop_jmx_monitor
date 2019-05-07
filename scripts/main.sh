# set -x

log_dir=log

hourly_report_dir=report/hourly
daily_report_dir=report/daily
db_dir=db
mkdir -p log report/hourly report/daily db
poll_id=poll_id_`date +%Y_%m_%d_%H_%M_%S`
date_id=`date +%Y_%m_%d`
month_id=`date +%Y_%m`
mkdir -p report/hourly/${date_id}

log_file=${log_dir}/app_log_${poll_id}.log
#Send hourly files into folder with name as date
dn_hourly_report_file=${hourly_report_dir}/${date_id}/dn_report_${poll_id}.csv
dn_daily_agg_file=${daily_report_dir}/dn_daily_${date_id}.csv

# dn_hourly_report_file=${hourly_report_dir}/dn_report_poll_id_2019_05_06_16_49_11.csv

echo "main starts here"



if [ ! -f config/dn_hosts ]
then
  echo "check for file config/dn_hosts with required host names in it "
  exit 1
fi



##############################################  DN ######################################################


prep_dn_hourly_table(){
  dn_metrics_list=`cat config/dn_metrics_config`
  dn_column_list="HostName poll_id "

  #derive column names from config file
  for i in $dn_metrics_list;do
    dn_column_list="${dn_column_list} ${i}"
  done

  dn_column_arr=($dn_column_list)


  dn_hourly_table=dn_hourly

  #Prepare DDL of table
  hourly_tbl_ddl="create table ${dn_hourly_table} ("

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

  echo "\nPoll hourly table \n"
  #initialize row values
  row=""

  #traverse through all slave nodes supplied from File
  while read host_name; do
   curl http://${host_name}:50075/jmx>tmp_${host_name}

   #Populate first two columns
   row="${host_name},${poll_id},"

   #grep and populate metrics values , metrics names provided from config file
   for i in ${dn_metrics_list};do
        row="$row"`grep \"${i}\" tmp_${host_name}|cut -f2 -d ':'`
   done
   row=${row%?}

   echo $row>>${dn_hourly_report_file}

   row=""
  rm tmp_${host_name}
  done <config/dn_hosts

}


load_hourly_table(){

  echo "\nDDL before create table :\n ${hourly_tbl_ddl}"
  #connect DB create tabel
  sqlite3 db/dn_jmx.db  "${hourly_tbl_ddl}"
  echo "\nDDL from Show table :"
  sqlite3 db/dn_jmx.db  ".schema ${dn_hourly_table}"
  #connect DB and import report file into daily table

  for hourly_file in ${hourly_report_dir}/${date_id}/*;do
    echo ".separator ","\n.import ${hourly_file} ${dn_hourly_table}" | sqlite3 db/dn_jmx.db
    echo "** file  ##  ${hourly_file} ## is loaded "
  done

  echo "\n # Number of rows on ${dn_hourly_table}  :\n"
  sqlite3 db/dn_jmx.db  "select count(*) from ${dn_hourly_table}"

}

load_daily_table(){

  sqlite3 db/dn_jmx.db  "create table dn_daily as select * from  ${dn_hourly_table} where 0"
  echo "\nDDL from Show dn_daily :"
  sqlite3 db/dn_jmx.db  ".schema dn_daily"
  #connect DB and import report file into daily table
  sqlite3 db/dn_jmx.db "select * from ${dn_hourly_table} where HostName=\"cent7-hdp-1.field.hortonworks.com\";"

    aggregate_hourly_table_query="insert into dn_daily select HostName,substr(poll_id,9,10) as date_id"
    for col in ${dn_metrics_list};do
      aggregate_hourly_table_query="${aggregate_hourly_table_query} ,avg(${col})"
    done
    aggregate_hourly_table_query="${aggregate_hourly_table_query} from ${dn_hourly_table} group by HostName,substr(poll_id,9,10);"

  echo " agg query is \n: ${aggregate_hourly_table_query}"
  sqlite3 db/dn_jmx.db  "${aggregate_hourly_table_query}"

  echo "\nSelect table dn_daily troubleshoot purpose only :\n"

  echo ".mode csv\n.output ${dn_daily_agg_file}\nSelect * from dn_daily;\n.quit" | sqlite3 db/dn_jmx.db

  sqlite3 db/dn_jmx.db  "select * from dn_daily"
  sqlite3 db/dn_jmx.db  "delete from dn_daily"

}
########################################## DN ends ####################################################################
#work on DNs
#work on NMs

clean_up(){
  # drop db
  rm db/dn_jmx.db
  echo "\n main ends here"
}


prep_dn_hourly_table
poll_hourly_dn_jmx
load_hourly_table
load_daily_table

clean_up
