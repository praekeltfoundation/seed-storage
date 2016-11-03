import pytest
from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase

from seed.xylem import postgres
from seed.xylem.pg_compat import psycopg2, errorcodes


def trap_pg_error(d, exc_type, pgcode=None):
    def trap_err(f):
        f.trap(exc_type)
        if pgcode is not None and f.value.pgcode != pgcode:
            return f
    return d.addErrback(trap_err)


def close_cursor(cur):
    if cur.running:
        cur.close()


def passthrough(f, *args, **kw):
    def cb(r):
        f(*args, **kw)
        return r
    return cb


class TestPostgresPlugin(TestCase):
    def get_plugin_no_setup(self):
        """
        Create a plugin without running setup.
        """
        return postgres.Plugin({
            'db_name': 'xylem_test_db',
            'name': 'postgres',
            'key': 'mysecretkey',
            'servers': [{
                'hostname': 'localhost'
            }]
        }, None, setup_db=False)

    def get_plugin(self):
        """
        Create a plugin and run its setup.

        We can't rely on the autosetup, because that happens asynchronously and
        we have no way of waiting for it.

        Additionally, we drop any existing xylem table to avoid leaking state
        between tests.
        """
        plug = self.get_plugin_no_setup()
        d = self.cleanup_databases_table(plug)
        d.addCallback(lambda _: plug._setup_db())
        d.addCallback(lambda _: plug)
        return d

    def cleanup_databases_table(self, plug):
        d = self.run_operation(plug, "DROP TABLE databases;")
        trap_pg_error(d, psycopg2.ProgrammingError, errorcodes.UNDEFINED_TABLE)
        return d

    def dropdb(self, plug, dbname):
        self.addCleanup(self._dropdb, plug, dbname)
        return self._dropdb(plug, dbname)

    def _dropdb(self, plug, dbname):
        d = self.run_operation(plug, "DROP DATABASE %s;" % (dbname,))
        trap_pg_error(
            d, psycopg2.ProgrammingError, errorcodes.INVALID_CATALOG_NAME)
        return d

    def run_query(self, plug, *args, **kw):
        cur = plug._get_xylem_db()
        self.addCleanup(close_cursor, cur)
        d = cur.runQuery(*args, **kw)
        return d.addBoth(passthrough(close_cursor, cur))

    def run_operation(self, plug, *args, **kw):
        cur = plug._get_xylem_db()
        self.addCleanup(close_cursor, cur)
        d = cur.runOperation(*args, **kw)
        return d.addBoth(passthrough(close_cursor, cur))

    def list_dbs(self, plug):
        d = self.run_query(
            plug, "SELECT datname FROM pg_database WHERE NOT datistemplate;")
        return d.addCallback(lambda r: [x[0] for x in r])

    @inlineCallbacks
    def assert_pg_error(self, d, exc_type, pgcode=None):
        e = yield self.assertFailure(d, exc_type)
        if pgcode is not None:
            self.assertEqual(e.pgcode, pgcode)

    ###################
    # Tests start here.

    def test_pwgens(self):
        """
        We can encrypt and decrypt passwords.
        """
        plug = self.get_plugin_no_setup()
        enc = plug._encrypt('Test string')
        self.assertNotEqual(enc, 'Test string')
        dec = plug._decrypt(enc)
        self.assertEqual(dec, 'Test string')

    @inlineCallbacks
    def test_setup_db(self):
        """
        We can create our own database table.
        """
        plug = self.get_plugin_no_setup()
        yield self.cleanup_databases_table(plug)
        # After cleanup, our table should not exist.
        yield self.assert_pg_error(
            self.run_query(plug, "SELECT * FROM databases"),
            psycopg2.ProgrammingError, errorcodes.UNDEFINED_TABLE)
        yield plug._setup_db()
        # After setup, our table should exist.
        rows = yield self.run_query(plug, "SELECT * FROM databases")
        self.assertEqual(rows, [])

    @inlineCallbacks
    def test_setup_db_again(self):
        """
        Database setup is idempotent.
        """
        plug = yield self.get_plugin()
        # After (auto)setup, our table should exist.
        rows = yield self.run_query(plug, "SELECT * FROM databases")
        self.assertEqual(rows, [])
        yield plug._setup_db()
        # After a second setup, our table should still exist.
        rows = yield self.run_query(plug, "SELECT * FROM databases")
        self.assertEqual(rows, [])

    @inlineCallbacks
    def test_call_create_database_bad_name(self):
        """
        We can't create a database with a bad name.
        """
        plug = yield self.get_plugin()
        names = ["", " ", ".", "-", '"; -- bobby tables']

        for name in names:
            r = yield plug.call_create_database({"name": name})
            self.assertEqual(r, {"Err": "Database name must be alphanumeric"})

    @inlineCallbacks
    def test_call_create_database_new(self):
        """
        We can create a new database.
        """
        dbname = "xylem_test_create_new"
        plug = yield self.get_plugin()
        yield self.dropdb(plug, dbname)
        dbs = yield self.list_dbs(plug)
        self.assertFalse(dbname in dbs)

        result = yield plug.call_create_database({"name": dbname})
        user = result["user"]
        password = result["password"]
        self.assertEqual(result, {
            "name": dbname,
            "user": user,
            "password": password,
            "hostname": "localhost",
            "Err": None,
        })

        dbs = yield self.list_dbs(plug)
        self.assertTrue(dbname in dbs)

    @pytest.mark.xfail(reason="The response fields are different.")
    @inlineCallbacks
    def test_call_create_database_idempotent(self):
        """
        Creating a database is idempotent and returns the same response every
        time.

        FIXME: The response contains different field names depending on whether
        the database already exists or not. This needs to be fixed in the code.
        """
        dbname = "xylem_test_create_idem"
        plug = yield self.get_plugin()
        yield self.dropdb(plug, dbname)
        dbs = yield self.list_dbs(plug)
        self.assertFalse(dbname in dbs)

        # Create the database
        result = yield plug.call_create_database({"name": dbname})
        user = result["user"]
        password = result["password"]
        expected_result = {
            "name": dbname,
            "user": user,
            "password": password,
            "hostname": "localhost",
            "Err": None,
        }
        self.assertEqual(result, expected_result)
        dbs = yield self.list_dbs(plug)
        self.assertTrue(dbname in dbs)

        # Recreate the database
        result = yield plug.call_create_database({"name": dbname})
        self.assertEqual(result, expected_result)
        dbs = yield self.list_dbs(plug)
        self.assertTrue(dbname in dbs)

    @inlineCallbacks
    def test_call_create_database_idempotent_broken(self):
        """
        Creating a database is idempotent and returns the same response every
        time.

        FIXME: The response contains different field names depending on whether
        the database already exists or not. This needs to be fixed in the code.
        """
        dbname = "xylem_test_create_idem"
        plug = yield self.get_plugin()
        yield self.dropdb(plug, dbname)
        dbs = yield self.list_dbs(plug)
        self.assertFalse(dbname in dbs)

        # Create the database
        result = yield plug.call_create_database({"name": dbname})
        user = result["user"]
        password = result["password"]
        expected_result = {
            "name": dbname,
            "user": user,
            "password": password,
            "hostname": "localhost",
            "Err": None,
        }
        self.assertEqual(result, expected_result)
        dbs = yield self.list_dbs(plug)
        self.assertTrue(dbname in dbs)

        # Recreate the database
        result = yield plug.call_create_database({"name": dbname})
        expected_result = {
            "name": dbname,
            "username": user,
            "password": password,
            "host": "localhost",
            "Err": None,
        }
        self.assertEqual(result, expected_result)
        dbs = yield self.list_dbs(plug)
        self.assertTrue(dbname in dbs)
