import os

from twisted.internet import defer, reactor
from rhumba import RhumbaPlugin
from rhumba.utils import fork

class Plugin(RhumbaPlugin):
    def __init__(self, config):
        super(Plugin, self).__init__(config)

        self.gluster_path = config.get('gluster_path', '/usr/sbin/gluster')
        self.gluster_nodes = config['gluster_nodes']
        self.gluster_mounts = config.get('gluster_mounts', ['/data'])
        self.gluster_replica = config.get('gluster_replica')
        self.gluster_stripe = config.get('gluster_stripe')

    @defer.inlineCallbacks
    def callGluster(self, *args):
        """ Calls the gluster CLI tool with `*args`
        """
        out, err, code = yield fork(self.gluster_path, args=args)

        if code > 0:
            raise Exception(err)

        else:
            defer.returnValue(out.strip('\n').split('\n'))

    @defer.inlineCallbacks
    def getVolumes(self):
        """ Gets volume information from glusterfs on this server
        """

        volumeInfo = yield self.callGluster('volume', 'info')

        vols = {}

        vol = None

        for l in volumeInfo:
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

    def _createArgs(self, name, createpath=True):
        args = ['volume', 'create', name]

        if self.gluster_stripe:
            args.append('stripe %s' % self.gluster_stripe)

        if self.gluster_replica:
            args.append('replica %s' % self.gluster_replica)

        for mount in self.gluster_mounts:
            for node in self.gluster_nodes:
                path = os.path.join(mount, 'xylem-%s' % name)
                if createpath:
                    os.makedirs(path)
                args.append('%s:%s' % (node, mount))

        args.append('force')

        return tuple(args)

    @defer.inlineCallbacks
    def createVolume(self, name):
        """ Creates a Gluster volume
        """

        yield self.callGluster(*self._createArgs(name))

        yield self.callGluster('volume', 'start', name)


    @defer.inlineCallbacks
    def call_createvolume(self, args):

        name = args.get('name')

        vols = yield self.getVolumes()

        if name in vols:
            self.log("Volume exists %s" % name)
            defer.returnValue(vols[name])

        else:
            yield self.createVolume(name)

            vols = yield self.getVolumes()
            self.log("Volume created %s" % repr(vols[name]))

            defer.returnValue(vols[name])
            
