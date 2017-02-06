#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

MYNAME="${0##*/}"

if [[ -f "${HADOOP_CONF_DIR}/httpfs-env.sh" ]]; then
  # shellcheck disable=SC1090
  . "${HADOOP_CONF_DIR}/httpfs-env.sh"
fi

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd`

DEFAULT_LIBEXEC_DIR="$bin"/../libexec

HADOOP_LIBEXEC_DIR=${HADOOP_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
. $HADOOP_LIBEXEC_DIR/hadoop-config.sh

HADOOP_CLASSNAME=org.apache.hadoop.fs.http.server.HttpFSServerWebServer

HTTPFS_OPTIONS="-Dhttpfs.home.dir=${HTTPFS_HOME:-${HADOOP_HDFS_HOME}}"
HTTPFS_OPTIONS="$HTTPFS_OPTIONS -Dhttpfs.config.dir=${HTTPFS_CONFIG:-${HADOOP_CONF_DIR}}"
HTTPFS_OPTIONS="$HTTPFS_OPTIONS -Dhttpfs.log.dir=${HTTPFS_LOG:-${HADOOP_LOG_DIR}}"
HTTPFS_OPTIONS="$HTTPFS_OPTIONS -Dhttpfs.temp.dir=${HTTPFS_TEMP:-${HADOOP_HDFS_HOME}/temp}"

export CLASSPATH=$CLASSPATH
exec "$JAVA" "$HADOOP_CLASSNAME" $HADOOP_HTTPFS_HEAPSIZE $HADOOP_OPTS $HTTPFS_OPTIONS

