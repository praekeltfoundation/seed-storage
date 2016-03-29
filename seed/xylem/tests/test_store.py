from twisted.trial import unittest

from twisted.internet import defer

from seed.xylem import gluster, postgres

class Gluster(unittest.TestCase):
    def setUp(self):
        self.plug = gluster.Plugin({
            'name': 'gluster', 
            'gluster_nodes': ['test'],
            'gluster_mounts': ['/data'],
        }, None)

        self.plug.callGluster = lambda *args: defer.maybeDeferred(
            self.fakeGlusterCommand, *args)

    def fakeGlusterCommand(self, *args):
        if args[0] == 'volume' and args[1] == 'info':
            return [
                '',
                'Volume Name: gv0',
                'Type: Distribute',
                'Volume ID: 8368a90e-2137-49d6-aa7f-377710018c88',
                'Status: Started',
                'Number of Bricks: 1',
                'Transport-type: tcp',
                'Bricks:',
                'Brick1: qa-mesos-persistence:/data/testbrick',
                'Options Reconfigured:',
                'performance.readdir-ahead: on',
                '',
                'Volume Name: gv2',
                'Type: Distribute',
                'Volume ID: 8bda3daa-4fe8-4021-8acd-4100ea2833fb',
                'Status: Started',
                'Number of Bricks: 1',
                'Transport-type: tcp',
                'Bricks:',
                'Brick1: qa-mesos-persistence:/data/br-gv2',
                'Options Reconfigured:',
                'performance.readdir-ahead: on'
            ]

        return []

    @defer.inlineCallbacks
    def test_volume_info(self):
        vols = yield self.plug.getVolumes()

        self.assertEquals(vols['gv0']['id'], '8368a90e-2137-49d6-aa7f-377710018c88')

    @defer.inlineCallbacks
    def test_volume_create(self):
        self.plug.gluster_mounts = ['/data1', '/data2']
        self.plug.gluster_stripe = 2
        create = yield self.plug._createArgs('testvol', createpath=False)

        self.assertEquals(create[2], 'testvol')
        self.assertEquals(create[3], 'stripe')
        self.assertEquals(create[5], 'test:/data1/xylem-testvol')
        self.assertEquals(create[6], 'test:/data2/xylem-testvol')

class Postgres(unittest.TestCase):
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

