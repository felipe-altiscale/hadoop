#!/bin/bash

/etc/init.d/mysql start

cd ${HIVE_HOME}/scripts/metastore/upgrade/mysql && \
  mysql -e "create database if not exists metastore; use metastore; source hive-schema-2.1.0.mysql.sql; create user if not exists 'hive'@'localhost' identified by 'password'; revoke all privileges, grant option from 'hive'@'localhost'; grant all privileges on metastore.* to 'hive'@'localhost'; flush privileges;"
$HIVE_HOME/bin/hive --service metastore &
sleep 5
kinit -kt /var/lib/krb5kdc/root.keytab root/hive.hadoop@HADOOP
tail -f /dev/null
