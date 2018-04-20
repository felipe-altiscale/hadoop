#!/bin/bash

if [ $FORMAT_NAMENODE == true ]; then
  /opt/hadoop/hadoop-dist/target/hadoop-2.7.4/bin/hdfs namenode -format
fi

/opt/hadoop/hadoop-dist/target/hadoop-2.7.4/sbin/hadoop-daemon.sh --config /etc/hadoop --script hdfs start namenode
/opt/hadoop/hadoop-dist/target/hadoop-2.7.4/sbin/httpfs.sh start
tail -f /dev/null
