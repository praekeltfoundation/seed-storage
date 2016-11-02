from twisted.trial.unittest import TestCase

from seed.xylem import postgres


class TestPostgresPlugin(TestCase):
    def setUp(self):
        self.plug = postgres.Plugin({
            'name': 'postgres',
            'key': 'mysecretkey',
            'servers': [{
                'hostname': 'localhost'
            }]
        }, None)

    def test_pwgens(self):
        enc = self.plug._encrypt('Test string')

        dec = self.plug._decrypt(enc)

        self.assertEquals(dec, 'Test string')
