# Adding a principal (example)
````
$ kadmin.local
$ addprinc -randkey yarn/yarn.hadoop@HADOOP
$ ktadd -k /var/lib/krb5kdc/yarn.keytab -norandkey yarn/yarn.hadoop@HADOOP
$ kinit -kt /var/lib/krb5kdc/yarn.keytab yarn/yarn.hadoop@HADOOP
```

# Sample JKS keystores used
```
https://github.com/apache/cxf/blob/master/testutils/src/test/resources/keys/cxfca.jks
https://github.com/apache/cxf/blob/master/testutils/src/test/resources/keys/bob.jks
```
