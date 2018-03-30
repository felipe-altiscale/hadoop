#!/bin/bash

sleep 5
kinit -kt /var/lib/krb5kdc/root.keytab root/resourcemanager.hadoop@HADOOP

/opt/hadoop/hadoop-dist/target/hadoop-2.7.4/sbin/yarn-daemon.sh --config /etc/hadoop start resourcemanager
tail -f /dev/null
