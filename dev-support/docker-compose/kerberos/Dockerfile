FROM hadoop:2.7.4

COPY conf/kdc.conf /etc/krb5kdc/kdc.conf
COPY conf/kadm5.acl /etc/krb5kdc/kadm5.acl
COPY conf/krb5.conf /etc/krb5.conf
COPY kerberos.sh /etc/krb5kdc/kerberos.sh

RUN apt-get install -y krb5-admin-server
RUN ln -s /var/lib/krb5kdc/stash /etc/krb5kdc/stash

CMD "/etc/krb5kdc/kerberos.sh"
