from twisted.trial import unittest

from twisted.internet import defer

from seed.xylem import gluster

class Test(unittest.TestCase):
    def setUp(self):
        self.plug = gluster.Plugin({
            'name': 'gluster', 
            'gluster_nodes': 'test',
            'gluster_mounts': ['/data'],
        })

        self.plug.callGluster = defer.maybeDeferred(self.fakeGlusterCommand)

    def fakeGlusterCommand(self, *args):
        return ""

    @defer.inlineCallbacks
    def test_gluster_commands(self):
        vols = yield self.plug.getVolumes()

        print vols

