#!/bin/bash

if [ ! -d /etc/xylem ];
then
    mkdir -p /etc/xylem
    cat >/etc/xylem/xylem.yml <<EOL
queues:
   # - name: gluster
   #   plugin: seed.xylem.gluster
   #   gluster_mounts:
   #     - /data
   #   gluster_nodes:
   #     - gluster01.foo.bar
   #     - gluster02.foo.bar
   #   gluster_replica: 2
EOL
fi

update-rc.d xylem defaults
service xylem status >/dev/null 2>&1

if [ "$?" -gt "0" ];
then
    service xylem start 2>&1
else
    service xylem restart 2>&1
fi 

exit 0
