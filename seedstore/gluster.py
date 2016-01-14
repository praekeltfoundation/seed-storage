import os

from twisted.internet import defer, reactor
from rhumba import RhumbaPlugin
from rhumba.utils import fork

class Plugin(RhumbaPlugin):
    @defer.inlineCallbacks
    def getVolumes(self):
        """ Gets volume information from glusterfs on this server
        """

        out, err, code = yield fork('/usr/sbin/gluster',
            args=('volume', 'info'))

        vols = {}

        vol = None

        for l in out.split('\n'):
            if not ':' in l:
                continue

            k, v = l.split(':', 1)
            v = v.strip()
            if k == 'Volume Name':
                vol = v
                vols[vol] = {'bricks':[], 'running': False}

            if vol:
                if k == 'Volume ID':
                    vols[vol]['id'] = v

                if k == 'Status':
                    if v == 'Started': 
                        vols[vol]['running'] = True

                if k.startswith('Brick') and v:
                    vols[vol]['bricks'].append(v)

        defer.returnValue(vols)

    @defer.inlineCallbacks
    def createVolume(self, name):
        """ Creates a Gluster volume
        """
        
        path = '/data/br-%s' % name

        os.makedirs(path)

        out, err, code = yield fork('/usr/sbin/gluster',
            args=('volume', 'create', name, 'qa-mesos-persistence:%s' % path, 'force'))

        out, err, code = yield fork('/usr/sbin/gluster',
            args=('volume', 'start', name))


    @defer.inlineCallbacks
    def call_createvolume(self, args):
        self.log("Test call %s" % repr(args))

        name = args.get('name')

        vols = yield self.getVolumes()

        if name in vols:
            defer.returnValue(vols[name])

        else:
            yield self.createVolume(name)

            vols = yield self.getVolumes()

            defer.returnValue(vols[name])
            
