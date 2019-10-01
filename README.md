# hadoop_jmx_monitor
Collect jmx data from all the nodes of a cluster, process and raise alerts

## Execution
1. Update following files under config/
dn_hosts
nm_hosts
nn_hosts
email_list

2.Trigger following script
sh scripts/main.sh

