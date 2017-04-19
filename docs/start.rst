Getting started
***************

Installation
============

Xylem can be installed from the APT repository
https://praekeltfoundation.github.io/packages/

Configuration
=============

Xylem is built using the Rhumba framework and makes use of its configuration.

An example configuration providing both the GlusterFS and Postgres management
modules is as follows ::

    queues:
        - name: gluster
          plugin: seed.xylem.gluster
          gluster_mounts:
            - /data
          gluster_nodes:
            - gluster01.foo.bar
            - gluster02.foo.bar
          gluster_replica: 2

        - name: postgres
          plugin: seed.xylem.postgres
          key: mysecretkey
          servers:
            - hostname: localhost
              username: postgres

Usage
=====

Xylem can be called using the Rhumba client libraries acting directly on the
backend queue or using the HTTP API. 

HTTP API
========

For more details see the Rhumba documentation https://github.com/calston/rhumba#http-api

Postgres
--------

Creating a database ::

    curl -X POST -d '{"name": "mydatabase"}' http://xylemserver:7701/queues/postgres/wait/create_database

Gluster
-------

See the Docker-Xylem plugin for Gluster management https://github.com/praekeltfoundation/docker-xylem

