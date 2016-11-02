try:
    # Partly to keep flake8 happy, partly to support psycopg2.
    from psycopg2cffi import compat
    compat.register()
except ImportError:
    pass
import psycopg2
from psycopg2 import errorcodes
from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase

from seed.xylem import postgres


def trap_pg_error(d, exc_type, pgcode=None):
    def trap_err(f):
        f.trap(exc_type)
        if pgcode is not None and f.value.pgcode != pgcode:
            return f
    return d.addErrback(trap_err)


def close_cursor(d, cur):
    def close_cb(r):
        cur.close()
        return r
    return d.addBoth(close_cb)


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
        d = self.cleanup_database(plug)
        d.addCallback(lambda _: plug._setup_db())
        d.addCallback(lambda _: plug)
        return d

    def cleanup_database(self, plug):
        d = self.run_operation(plug, "DROP TABLE databases;")
        trap_pg_error(d, psycopg2.ProgrammingError, errorcodes.UNDEFINED_TABLE)
        return d

    def run_query(self, plug, *args, **kw):
        cur = plug._get_xylem_db()
        d = cur.runQuery(*args, **kw)
        return close_cursor(d, cur)

    def run_operation(self, plug, *args, **kw):
        cur = plug._get_xylem_db()
        d = cur.runOperation(*args, **kw)
        return close_cursor(d, cur)

    @inlineCallbacks
    def assert_pg_error(self, d, exc_type, pgcode=None):
        e = yield self.assertFailure(d, exc_type)
        if pgcode is not None:
            self.assertEqual(e.pgcode, pgcode)

    ###################
    # Tests start here.

    def test_pwgens_old_old(self):
        """
        We can encrypt and decrypt passwords.
        """
        plug = self.get_plugin_no_setup()
        enc = plug._encrypt_old('Test string')
        self.assertNotEqual(enc, 'Test string')
        dec = plug._decrypt_old(enc)
        self.assertEqual(dec, 'Test string')

    def test_pwgens_new_new(self):
        """
        We can encrypt and decrypt passwords.
        """
        plug = self.get_plugin_no_setup()
        enc = plug._encrypt('Test string')
        self.assertNotEqual(enc, 'Test string')
        dec = plug._decrypt(enc)
        self.assertEqual(dec, 'Test string')

    def test_pwgens_new_old(self):
        """
        We can encrypt and decrypt passwords.
        """
        plug = self.get_plugin_no_setup()
        enc = plug._encrypt('Test string')
        self.assertNotEqual(enc, 'Test string')
        dec = plug._decrypt_old(enc)
        self.assertEqual(dec, 'Test string')

    def test_pwgens_old_new(self):
        """
        We can encrypt and decrypt passwords.
        """
        plug = self.get_plugin_no_setup()
        enc = plug._encrypt_old('Test string')
        self.assertNotEqual(enc, 'Test string')
        dec = plug._decrypt(enc)
        self.assertEqual(dec, 'Test string')

    @inlineCallbacks
    def test_setup_db(self):
        """
        We can create our own database table.
        """
        plug = self.get_plugin_no_setup()
        yield self.cleanup_database(plug)
        # After cleanup, our table should not exist.
        yield self.assert_pg_error(
            self.run_query(plug, "SELECT * FROM databases"),
            psycopg2.ProgrammingError, errorcodes.UNDEFINED_TABLE)
        yield plug._setup_db()
        # After setup, our table should exist.
        rows = yield self.run_query(plug, "SELECT * FROM databases")
        self.assertEqual(rows, [])
