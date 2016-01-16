import os

from twisted.internet import defer, reactor
from rhumba import RhumbaPlugin
from rhumba.utils import fork

class Plugin(RhumbaPlugin):
    def __init__(self, *a):
        super(Plugin, self).__init__(*a)

        self.gluster_path = self.config.get('gluster_path', '/usr/sbin/gluster')
        self.gluster_nodes = self.config['gluster_nodes']
        self.gluster_mounts = self.config.get('gluster_mounts', ['/data'])
        self.gluster_replica = self.config.get('gluster_replica')
        self.gluster_stripe = self.config.get('gluster_stripe')

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
        """ Build argument list for volume creation
        """
        args = ['volume', 'create', name]

        if self.gluster_stripe:
            args.extend(['stripe', str(self.gluster_stripe)])

        if self.gluster_replica:
            args.extend(['replica', str(self.gluster_replica)])

        for mount in self.gluster_mounts:
            path = os.path.join(mount, 'xylem-%s' % name)
            for node in self.gluster_nodes:
                args.append('%s:%s' % (node, path))

        args.append('force')

        return tuple(args)

    @defer.inlineCallbacks
    def createVolume(self, name):
        """ Creates a Gluster volume
        """

        args = self._createArgs(name)

        # Fan out in rhumba and create volume paths
        queue = self.queue_name
        cluster_queues = yield self.client.clusterQueues()
        server_uuids = [i['uuid'] for i in cluster_queues[queue]]

        id = yield self.client.queue(queue, 'createdirs', {'name': name},
            uids=server_uuids)

        # Wait for all servers to finish
        for uid in server_uuids:
            yield self.client.waitForResult(queue, id, timeout=60, suid=uid)

        self.log('[gluster] %s' % ' '.join(args))

        yield self.callGluster(*args)

        yield self.callGluster('volume', 'start', name)

    def call_createdirs(self, args):
        """Fan out call to create directories
        """
        name = args['name']

        for mount in self.gluster_mounts:
            path = os.path.join(mount, name)
            try:
                os.makedirs(path)
            except os.error, e:
                # Raise any error except path exists
                if e.errno != 17:
                    return {'Err': str(e)}

        return {'Err': None}

    @defer.inlineCallbacks
    def call_createvolume(self, args):

        name = args['name']

        vols = yield self.getVolumes()

        if name in vols:
            self.log("Volume exists %s" % name)
            defer.returnValue(vols[name])

        else:
            yield self.createVolume(name)

            vols = yield self.getVolumes()
            self.log("Volume created %s" % repr(vols[name]))

            defer.returnValue(vols[name])
            
