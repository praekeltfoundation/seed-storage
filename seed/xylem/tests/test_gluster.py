from uuid import uuid4

from twisted.internet import defer
from twisted.trial.unittest import TestCase

from seed.xylem import gluster


class FakeVolume(object):
    def __init__(self, name, bricks, volume_id=None):
        self.name = name
        self.bricks = list(bricks)
        if volume_id is None:
            volume_id = str(uuid4())
        self.volume_id = volume_id

    def info(self):
        return [
            '',
            'Volume Name: {0}'.format(self.name),
            'Type: Distribute',
            'Volume ID: {0}'.format(self.volume_id),
            'Status: Started',
            'Number of Bricks: {0}'.format(len(self.bricks)),
            'Transport-type: tcp',
            'Bricks:',
        ] + ['Brick{0}: {1}'.format(i+1, brick)
             for i, brick in enumerate(self.bricks)] + [
            'Options Reconfigured:',
            'performance.readdir-ahead: on',
        ]


class FakeGluster(object):
    def __init__(self):
        self.volumes = {}

    def add_volume(self, name, *args, **kw):
        assert name not in self.volumes
        vol = FakeVolume(name, *args, **kw)
        self.volumes[name] = vol
        return vol

    def cmd_volume_info(self):
        return sum([vol.info() for vol in self.volumes.values()], [])

    def call(self, cmd0, cmd1, *args):
        meth = getattr(self, '_'.join(['cmd', cmd0, cmd1]))
        return meth(*args)


class TestGlusterPlugin(TestCase):
    def setUp(self):
        self.plug = gluster.Plugin({
            'name': 'gluster',
            'gluster_nodes': ['test'],
            'gluster_mounts': ['/data'],
        }, None)

        self.fake_gluster = FakeGluster()
        self.plug.callGluster = lambda *args: defer.maybeDeferred(
            self.fake_gluster.call, *args)

    @defer.inlineCallbacks
    def test_volume_info(self):
        gv0 = self.fake_gluster.add_volume(
            'gv0', ['qa-mesos-persistence:/data/testbrick'])
        gv2 = self.fake_gluster.add_volume(
            'gv2', ['qa-mesos-persistence:/data/br-gv2'])

        vols = yield self.plug.getVolumes()

        self.assertEquals(vols['gv0']['id'], gv0.volume_id)
        self.assertEquals(vols['gv2']['id'], gv2.volume_id)

    @defer.inlineCallbacks
    def test_volume_create(self):
        self.plug.gluster_mounts = ['/data1', '/data2']
        self.plug.gluster_stripe = 2
        create = yield self.plug._createArgs('testvol', createpath=False)
        print create
        assert False

        self.assertEquals(create[2], 'testvol')
        self.assertEquals(create[3], 'stripe')
        self.assertEquals(create[5], 'test:/data1/xylem-testvol')
        self.assertEquals(create[6], 'test:/data2/xylem-testvol')
