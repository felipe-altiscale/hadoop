#!/bin/bash

sleep 5
/opt/hadoop/hadoop-dist/target/hadoop-2.7.4/sbin/httpfs.sh start
tail -f /dev/null
