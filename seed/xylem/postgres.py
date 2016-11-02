import base64
import hashlib
import uuid
import time
import re
import random
import psycopg2
from psycopg2 import errorcodes

from Crypto.Cipher import AES
from Crypto import Random

from twisted.internet import defer, reactor
from twisted.enterprise import adbapi

from rhumba import RhumbaPlugin
from rhumba.utils import fork

class Plugin(RhumbaPlugin):
    # FIXME: Setup is asynchronous and there may be a race condition if we try
    #        to process a request before setup finishes.
    def __init__(self, *args, **kw):
        setup_db = kw.pop('setup_db', True)
        super(Plugin, self).__init__(*args, **kw)

        self.servers = self.config['servers']

        # Details for Xylems internal DB 
        self.db = self.config.get('db_name', 'xylem')
        self.host = self.config.get('db_host', 'localhost')
        self.port = self.config.get('db_port', 5432)
        self.password = self.config.get('db_password', '')
        self.username = self.config.get('db_username', 'postgres')

        self.key = self.config['key']

        if setup_db:
            reactor.callWhenRunning(self._setup_db)

    def _encrypt(self, s):
        key_iv = Random.new().read(AES.block_size)

        cip = AES.new(hashlib.md5(self.key).hexdigest(), AES.MODE_CFB,
            key_iv)

        pwenc = key_iv + cip.encrypt(s)

        return base64.b64encode(pwenc)

    def _decrypt(self, e):
        msg = base64.b64decode(e)
        
        key_iv = msg[:AES.block_size]
        cip = AES.new(hashlib.md5(self.key).hexdigest(), AES.MODE_CFB,
            key_iv)

        return cip.decrypt(msg[AES.block_size:])

    @defer.inlineCallbacks
    def _setup_db(self):
        db_table = "CREATE TABLE databases (name varchar(66) UNIQUE, host"\
            " varchar(256), username varchar(256), password varchar(256));"

        cur = self._get_xylem_db()

        try:
            yield cur.runOperation(db_table)
        except psycopg2.ProgrammingError, e:
            if e.pgcode != errorcodes.DUPLICATE_TABLE:
                raise e

        cur.close()
        
    def _create_password(self):
        # Guranteed random dice rolls 
        return base64.b64encode(hashlib.sha1(uuid.uuid1().hex).hexdigest()
            )[:24]

    def _create_username(self, db):
        return base64.b64encode("mydb" + str(
            time.time()+random.random()*time.time())).strip('=').lower()

    def _get_connection(self, db, host, port, user, password):
        return adbapi.ConnectionPool('psycopg2',
            database=db,
            host=host,
            port=port,
            user=user,
            password=password,
            cp_min=1,
            cp_max=2,
            cp_openfun=self._fixdb
        )

    def _get_xylem_db(self):
        return adbapi.ConnectionPool('psycopg2',
            database=self.db,
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            cp_min=1,
            cp_max=2,
            cp_openfun=self._fixdb
        )

    def _fixdb(self, conn):
        conn.autocommit = True

    @defer.inlineCallbacks
    def call_create_database(self, args):
        name = args['name']

        if not re.match('^\w+$', name):
            defer.returnValue({"Err": "Database name must be alphanumeric"})


        check = "SELECT * FROM pg_database WHERE datname=%s;"

        xylemdb = self._get_xylem_db()

        find_db = "SELECT name, host, username, password FROM databases"\
            " WHERE name=%s"
        
        row = yield xylemdb.runQuery(find_db, (name,))

        if row:
            xylemdb.close()
            defer.returnValue({
                'Err': None, 
                'name': row[0][0], 
                'host': row[0][1],
                'username': row[0][2],
                'password': self._decrypt(row[0][3])
            })

        else:
            server = random.choice(self.servers)

            rdb = self._get_connection('postgres',
                server['hostname'], 
                int(server.get('port', 5432)),
                server.get('username', 'postgres'),
                server.get('password')
            )

            r = yield rdb.runQuery(check, (name,))

            if not r:
                user = self._create_username(name)
                password = self._create_password()

                create_u = "CREATE USER %s WITH ENCRYPTED PASSWORD %%s;" % user
                create_d = "CREATE DATABASE %s ENCODING 'UTF8' OWNER %s;" % (
                    name, user)

                r = yield rdb.runOperation(create_u, (password,))
                r = yield rdb.runOperation(create_d, (password,))

                yield xylemdb.runOperation(
                    "INSERT INTO databases (name, host, username, password)"\
                    " VALUES (%s, %s, %s, %s);",
                    (name, server['hostname'], user, self._encrypt(password))
                )

                xylemdb.close()
                rdb.close()
                defer.returnValue({
                    'Err': None, 
                    'hostname': server['hostname'],
                    'name': name,
                    'user': user,
                    'password': password
                })
            else:
                xylemdb.close()
                rdb.close()
                defer.returnValue({
                    'Err': 'Database exists but not known to xylem'
                })
