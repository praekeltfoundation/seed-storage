from twisted.trial import unittest

from twisted.internet import defer

from seedstore import gluster

class Test(unittest.TestCase):

    @defer.inlineCallbacks
    def test_gluster_commands(self):
        plug = gluster.Plugin(None)

        vols = yield plug.getVolumes()

        print vols

