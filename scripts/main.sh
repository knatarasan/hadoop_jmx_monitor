
log_dir=log
report_dir=report
db_dir=db

poll_id=poll_id_`date +%Y_%m_%d_%H_%M_%S`

log_file=${log_dir}/app_log_${poll_id}.log
dn_report_file=${report_dir}/dn_report_${poll_id}.csv

echo "main starts here"

if [ ! -d ${log_dir} ]
then
  mkdir log
fi

if [ ! -d ${report_dir} ]
then
  mkdir report
fi

if [ ! -d ${db_dir} ]
then
  mkdir db
fi

if [ ! -f config/dn_hosts ]
then
  echo "check for file config/dn_hosts with required host names in it "
  exit 1
fi



##############################################  DN ######################################################

prep_dn_columns(){
  dn_metrics_list=`cat config/dn_metrics_config`
  dn_column_list="HostName,poll_id,"

  #set up column names
  for i in $dn_metrics_list;do
    dn_column_list="${dn_column_list}${i},"
  done

  dn_column_list=${dn_column_list%?}

}



poll_dn_jmx(){

  echo $dn_column_list>${dn_report_file}
  #initialize row values
  row=""

  #traverse through all slave nodes supplied from File
  while read host_name; do
   curl http://${host_name}:50075/jmx>tmp_${host_name}

   row="${host_name},${poll_id},"
   for i in $dn_metrics_list;do
        row="$row"`grep \"${i}\" tmp_${host_name}|cut -f2 -d ':'`
   done
   row=${row%?}

   echo $row>>${dn_report_file}

   row=""
  rm tmp_${host_name}
  done <config/dn_hosts

}


load_into_db(){

  dn_table=dn_report

  # daily_tbl="create table n (id INTEGER PRIMARY KEY,f TEXT,l TEXT);"

  daily_tbl="create table ${dn_table} ("

  for col in ${dn_column_list};do
    daily_tbl="${daily_tbl} ${col} TEXT"
  done
  daily_tbl="${daily_tbl} );"


  sqlite3 db/dn_jmx.db  "${daily_tbl}"
  sqlite3 db/dn_jmx.db  ".mode csv"
  sqlite3 db/dn_jmx.db  ".import report/dn_report_poll_id_2019_05_06_13_14_42.csv ${dn_table}"
  sqlite3 db/dn_jmx.db  ".schema ${dn_table}";
  sqlite3 db/dn_jmx.db  "select * from ${dn_table}"

  # drop db
  rm db/dn_jmx.db
}
########################################## DN ends ####################################################################

#work on DNs


prep_dn_columns
# poll_dn_jmx
load_into_db


#work on NMs
echo "main ends here"
