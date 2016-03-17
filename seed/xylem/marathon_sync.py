from functools import wraps

from twisted.internet.defer import gatherResults
from twisted.web.client import getPage

from rhumba import RhumbaPlugin, cron


def unpack_args(fn):
    return wraps(fn)(lambda self, args: fn(self, **args))


class Plugin(RhumbaPlugin):
    """
    A plugin to periodically push an application group definition to Marathon.
    """

    def __init__(self, *args, **kw):
        super(Plugin, self).__init__(*args, **kw)

        self.marathon_host = self.config.get("marathon_host", "localhost")
        self.marathon_port = self.config.get("marathon_port", "8080")
        self.group_json_files = self.config["group_json_files"]

    @cron(min="*/1")
    @unpack_args
    def call_update_groups(self):
        """
        Send app group definitions to Marathon.
        """
        ds = []
        for filepath in self.group_json_files:
            ds.append(self.call_update_group({'group_json_file': filepath}))
        return gatherResults(ds)

    @unpack_args
    def call_update_group(self, group_json_file):
        self.log("Updating %r" % (group_json_file,))
        body = self.readfile(group_json_file)
        d = self._call_marathon("PUT", "v2/groups", body)
        d.addBoth(self._logcb, "API response for %s: %%r" % (group_json_file,))
        return d

    def _logcb(self, r, msgfmt):
        self.log(msgfmt % (r,))
        return r

    def _call_marathon(self, method, path, body=None):
        uri = b"http://%s:%s/%s" % (
            self.marathon_host, self.marathon_port, path)
        return self.getPage(uri, method=method, postdata=body)

    def readfile(self, filepath):
        """
        Read a file and return its content.
        """
        with open(filepath, "r") as f:
            return f.read()

    def getPage(self, *args, **kw):
        """
        Proxy twisted.web.client.getPage so we can stub it out in tests.
        """
        return getPage(*args, **kw)
