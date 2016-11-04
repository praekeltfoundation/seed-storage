from twisted.internet.defer import inlineCallbacks, succeed, fail
from twisted.trial.unittest import TestCase

from seed.xylem import postgres
from seed.xylem.postgres import ignore_pg_error, cursor_closer
from seed.xylem.pg_compat import psycopg2, errorcodes


class TestPostgresHelpers(TestCase):
    def test_ignore_pg_error_no_err(self):
        """
        Nothing to ignore, let's move on.
        """
        d = ignore_pg_error(succeed("Yay"), errorcodes.UNDEFINED_TABLE)
        self.assertEqual(self.successResultOf(d), "Yay")

    def test_ignore_pg_error_ignore(self):
        """
        Ignore the error we're told to ignore.
        """
        err = psycopg2.ProgrammingError()
        err.pgcode = errorcodes.UNDEFINED_TABLE
        d = ignore_pg_error(fail(err), errorcodes.UNDEFINED_TABLE)
        self.assertEqual(self.successResultOf(d), None)

    def test_ignore_pg_error_other_error(self):
        """
        This is a different error. Explode!
        """
        err = psycopg2.ProgrammingError()
        err.pgcode = errorcodes.INVALID_CATALOG_NAME
        d = ignore_pg_error(fail(err), errorcodes.UNDEFINED_TABLE)
        self.assertEqual(self.failureResultOf(d).value, err)

    def test_ignore_pg_error_other_exception(self):
        """
        This isn't even a postgres error. Explode!
        """
        err = Exception("Goodbye, cruel world.")
        d = ignore_pg_error(fail(err), errorcodes.UNDEFINED_TABLE)
        self.assertEqual(self.failureResultOf(d).value, err)


class TestPostgresPlugin(TestCase):
    def get_plugin_no_setup(self, config_override={}):
        """
        Create a plugin without running setup.
        """
        config = {
            'db_name': 'xylem_test_db',
            'name': 'postgres',
            'key': 'mysecretkey',
            'servers': [{
                'hostname': 'localhost'
            }]
        }
        config.update(config_override)
        return postgres.Plugin(config, None, setup_db=False)

    def get_plugin(self, config_override={}):
        """
        Create a plugin and run its setup.

        We can't rely on the autosetup, because that happens asynchronously and
        we have no way of waiting for it.

        Additionally, we drop any existing xylem table to avoid leaking state
        between tests.
        """
        plug = self.get_plugin_no_setup(config_override=config_override)
        d = self.cleanup_databases_table(plug)
        d.addCallback(lambda _: plug._setup_db())
        d.addCallback(lambda _: plug)
        return d

    def cleanup_databases_table(self, plug):
        d = self.run_operation(plug, "DROP TABLE databases;")
        ignore_pg_error(d, errorcodes.UNDEFINED_TABLE)
        return d

    def dropdb(self, plug, dbname):
        self.addCleanup(self._dropdb, plug, dbname)
        return self._dropdb(plug, dbname)

    def _dropdb(self, plug, dbname):
        d = self.run_operation(plug, "DROP DATABASE %s;" % (dbname,))
        ignore_pg_error(d, errorcodes.INVALID_CATALOG_NAME)
        return d

    def run_query(self, plug, *args, **kw):
        cur = plug._get_xylem_db()
        self.addCleanup(cursor_closer(cur))
        d = cur.runQuery(*args, **kw)
        return d.addBoth(cursor_closer(cur))

    def run_operation(self, plug, *args, **kw):
        cur = plug._get_xylem_db()
        self.addCleanup(cursor_closer(cur))
        d = cur.runOperation(*args, **kw)
        return d.addBoth(cursor_closer(cur))

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

    @inlineCallbacks
    def test_call_create_database_idempotent(self):
        """
        Creating a database is idempotent and returns the same response every
        time.
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
    def test_call_create_database_existing_unknown(self):
        """
        If a database exists but we don't know about it, we don't touch it.
        """
        dbname = "xylem_test_create_unknown"
        plug = yield self.get_plugin()
        yield self.dropdb(plug, dbname)

        # Create the database outside of xylem
        yield self.run_operation(plug, "CREATE DATABASE %s;" % (dbname,))
        dbs = yield self.list_dbs(plug)
        self.assertTrue(dbname in dbs)

        # Try to create it through xylem
        result = yield plug.call_create_database({"name": dbname})
        self.assertEqual(
            result, {"Err": "Database exists but not known to xylem"})

        dbs = yield self.list_dbs(plug)
        self.assertTrue(dbname in dbs)

    @inlineCallbacks
    def test_call_create_database_connect_addr(self):
        """
        We can create a new database.
        """
        dbname = "xylem_test_create_connaddr"
        plug = yield self.get_plugin({'servers': [{
            "hostname": "db.example.com",
            "connect_addr": "localhost",
        }]})
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
            "hostname": "db.example.com",
            "Err": None,
        })

        dbs = yield self.list_dbs(plug)
        self.assertTrue(dbname in dbs)
