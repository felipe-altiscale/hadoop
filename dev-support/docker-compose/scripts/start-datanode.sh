#!/bin/bash

sleep 5
kinit -kt /var/lib/krb5kdc/root.keytab root/datanode1.hadoop@HADOOP
/opt/hadoop/hadoop-dist/target/hadoop-2.7.4/sbin/hadoop-daemon.sh --config /etc/hadoop --script hdfs start datanode
tail -f /dev/null
