#!/usr/bin/env bash

# Create the DB
# kdb5_util create -s -r HADOOP

# Running a single KDC
# docker run --rm --mount type=bind,source="$(pwd)"/krb5kdc,target=/var/lib/krb5kdc --mount type=bind,source="$(pwd)"/logs,target=/var/log/kerberos -it jack/hadoop-krb:0.1 bash

/etc/init.d/krb5-kdc start
/etc/init.d/krb5-admin-server start
tail -f /dev/null
