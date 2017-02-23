from uuid import uuid4

from twisted.internet import defer
from twisted.trial.unittest import TestCase

from seed.xylem import gluster


class FakeVolume(object):
    def __init__(self, name, bricks, status='Started', volume_id=None):
        self.name = name
        self.bricks = list(bricks)
        self.status = status
        if volume_id is None:
            volume_id = str(uuid4())
        self.volume_id = volume_id

    def info(self):
        return [
            '',
            'Volume Name: {0}'.format(self.name),
            'Type: Distribute',
            'Volume ID: {0}'.format(self.volume_id),
            'Status: {0}'.format(self.status),
            'Number of Bricks: {0}'.format(len(self.bricks)),
            'Transport-type: tcp',
            'Bricks:',
        ] + ['Brick{0}: {1}'.format(i+1, brick)
             for i, brick in enumerate(self.bricks)] + [
            'Options Reconfigured:',
            'performance.readdir-ahead: on',
        ]


class FakeGluster(object):
    """
    Fake gluster implementation that tracks (some) volume state and responds to
    various commands.
    """
    def __init__(self):
        self.volumes = {}

    def add_volume(self, name, *args, **kw):
        assert name not in self.volumes
        vol = FakeVolume(name, *args, **kw)
        self.volumes[name] = vol
        return vol

    def cmd_volume_info(self, name=None):
        if name:
            if name not in self.volumes:
                raise Exception('Volume {0} does not exist'.format(name))
            vols = [self.volumes[name]]
        else:
            vols = self.volumes.values()
        return sum([vol.info() for vol in vols], [])

    def cmd_volume_create(self, name, *args):
        while args[0] in ['replica', 'stripe', 'arbiter', 'transport']:
            args = args[2:]
        if args[-1] == 'force':
            args = args[:-1]
        self.add_volume(name, bricks=list(args), status='Stopped')
        return []

    def cmd_volume_start(self, name):
        vol = self.volumes[name]
        assert vol.status != 'Started'
        vol.status = 'Started'
        return []

    def call(self, cmd0, cmd1, *args):
        meth = getattr(self, '_'.join(['cmd', cmd0, cmd1]))
        return meth(*args)


class FakeRhumbaClient(object):
    """
    Fake rhumba client that successfully returns made up data for all
    (implemented) methods. This is to stub out the the path stuff in the volume
    creation tests below.
    """
    def __init__(self, plug):
        self.plug = plug

    def clusterQueues(self):
        return defer.succeed({self.plug.queue_name: [{'uuid': str(uuid4())}]})

    def queue(self, *args, **kw):
        return defer.succeed(str(uuid4()))

    def waitForResult(self, *args, **kw):
        return defer.succeed(None)


class TestGlusterPlugin(TestCase):
    def setUp(self):
        self.plug = gluster.Plugin({
            'name': 'gluster',
            'gluster_nodes': ['test'],
            'gluster_mounts': ['/data'],
        }, None)
        self.plug.client = FakeRhumbaClient(self.plug)

        self.fake_gluster = FakeGluster()
        self.plug.callGluster = lambda *args: defer.maybeDeferred(
            self.fake_gluster.call, *args)

    @defer.inlineCallbacks
    def test_volume_info(self):
        """
        We can correctly parse volume info listings.
        """
        gv0 = self.fake_gluster.add_volume(
            'gv0', ['qa-mesos-persistence:/data/testbrick'])
        gv2 = self.fake_gluster.add_volume(
            'gv2', ['qa-mesos-persistence:/data/br-gv2'])

        vols = yield self.plug.getVolumes()
        self.assertEqual(len(vols), 2)
        self.assertEqual(vols['gv0']['id'], gv0.volume_id)
        self.assertEqual(vols['gv2']['id'], gv2.volume_id)

    @defer.inlineCallbacks
    def test_volume_info_single(self):
        """
        We can correctly parse volume info listings for a single volume.
        """
        gv0 = self.fake_gluster.add_volume(
            'gv0', ['qa-mesos-persistence:/data/testbrick'])
        self.fake_gluster.add_volume(
            'gv2', ['qa-mesos-persistence:/data/br-gv2'])

        vol = yield self.plug.getVolume('gv0')
        self.assertEqual(vol['id'], gv0.volume_id)

    @defer.inlineCallbacks
    def test_volume_info_missing(self):
        """
        If we ask about a missing volume, we get `None`.
        """
        self.fake_gluster.add_volume(
            'gv0', ['qa-mesos-persistence:/data/testbrick'])
        self.fake_gluster.add_volume(
            'gv2', ['qa-mesos-persistence:/data/br-gv2'])

        vol = yield self.plug.getVolume('gv1')
        self.assertEqual(vol, None)

    @defer.inlineCallbacks
    def test_volume_create(self):
        """
        A volume that does not exist is created.
        """
        self.assertEqual(self.fake_gluster.volumes.get('testvol'), None)

        self.plug.gluster_mounts = ['/data1', '/data2']
        self.plug.gluster_stripe = 2
        yield self.plug.call_createvolume({'name': 'testvol'})

        vol = self.fake_gluster.volumes['testvol']
        self.assertEqual(vol.bricks, [
            'test:/data1/xylem-testvol', 'test:/data2/xylem-testvol'])
        self.assertEqual(vol.status, 'Started')

    @defer.inlineCallbacks
    def test_volume_create_existing(self):
        """
        An existing running volume is not modified.
        """
        origvol = self.fake_gluster.add_volume('testvol', bricks=[
            'test:/data1/xylem-testvol', 'test:/data2/xylem-testvol'])
        originfo = origvol.info()

        self.plug.gluster_mounts = ['/data1', '/data2']
        self.plug.gluster_stripe = 2
        yield self.plug.call_createvolume({'name': 'testvol'})

        vol = self.fake_gluster.volumes['testvol']
        self.assertEqual(vol.info(), originfo)

    @defer.inlineCallbacks
    def test_volume_create_stopped(self):
        """
        An existing stopped volume is started.
        """
        origvol = self.fake_gluster.add_volume(
            'testvol',
            status='Stopped',
            bricks=['test:/data1/xylem-testvol', 'test:/data2/xylem-testvol'])
        self.assertEqual(origvol.status, 'Stopped')

        self.plug.gluster_mounts = ['/data1', '/data2']
        self.plug.gluster_stripe = 2
        yield self.plug.call_createvolume({'name': 'testvol'})

        vol = self.fake_gluster.volumes['testvol']
        self.assertEqual(vol.status, 'Started')
