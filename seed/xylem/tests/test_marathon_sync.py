from twisted.internet.defer import succeed, inlineCallbacks
from twisted.trial.unittest import TestCase

from seed.xylem import marathon_sync


class TestMarathonSync(TestCase):
    def get_plugin(self, **kw):
        kw.setdefault("name", "marathon_sync")
        return marathon_sync.Plugin(kw, None)

    def fake_getPage(self, e_url, e_method, e_postdata, resp):
        def getPage(url, method, postdata):
            self.assertEqual(url, e_url)
            self.assertEqual(method, e_method)
            self.assertEqual(postdata, e_postdata)
            return succeed(resp)
        return getPage

    def fake_readfile(self, fn_content_mapping):
        def readfile(filepath):
            return fn_content_mapping[filepath]
        return readfile

    @inlineCallbacks
    def test_update_group(self):
        """
        call_update_group makes an appropriate Marathon API request.
        """
        plugin = self.get_plugin(group_json_files=[])
        plugin.readfile = self.fake_readfile({
            "foo.json": '{"id": "/t", "apps": []}',
        })
        plugin.getPage = self.fake_getPage(
            "http://localhost:8080/v2/groups", "PUT",
            '{"id": "/t", "apps": []}',
            '{"version":"vvv","deploymentId":"ddd"}')
        resp = yield plugin.call_update_group("foo.json")
        self.assertEqual(resp, '{"version":"vvv","deploymentId":"ddd"}')
