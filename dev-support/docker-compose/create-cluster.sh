#!/bin/bash

export HADOOP_COMPOSE_DIR=`pwd`
export FORMAT_NAMENODE=true
rm -r $HADOOP_COMPOSE_DIR/storage/datanode*
rm -r $HADOOP_COMPOSE_DIR/storage/namenode
docker-compose up --force-recreate
