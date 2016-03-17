from twisted.internet.defer import succeed, inlineCallbacks
from twisted.trial.unittest import TestCase

from seed.xylem import marathon_sync


class TestMarathonSync(TestCase):
    def get_plugin(self, **kw):
        kw.setdefault("name", "marathon_sync")
        return marathon_sync.Plugin(kw, None)

    def fake_getPage(self, req_mapping):
        def getPage(url, method, postdata):
            return succeed(req_mapping[(url, method, postdata)])
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
        req = ("http://localhost:8080/v2/groups", "PUT",
               '{"id": "/t", "apps": []}')
        plugin.getPage = self.fake_getPage({
            req: '{"version":"vvv","deploymentId":"ddd"}',
        })
        resp = yield plugin.call_update_group({"group_json_file": "foo.json"})
        self.assertEqual(resp, '{"version":"vvv","deploymentId":"ddd"}')

    @inlineCallbacks
    def test_update_groups(self):
        """
        call_update_group makes an appropriate Marathon API request.
        """
        plugin = self.get_plugin(group_json_files=["foo.json", "bar.json"])
        plugin.readfile = self.fake_readfile({
            "foo.json": '{"id": "/f", "apps": []}',
            "bar.json": '{"id": "/b", "apps": []}',
        })
        fooreq = ("http://localhost:8080/v2/groups", "PUT",
                  '{"id": "/f", "apps": []}')
        barreq = ("http://localhost:8080/v2/groups", "PUT",
                  '{"id": "/b", "apps": []}')
        plugin.getPage = self.fake_getPage({
            fooreq: '{"version":"vvv","deploymentId":"dddf"}',
            barreq: '{"version":"vvv","deploymentId":"dddb"}',
        })
        resp = yield plugin.call_update_groups({})
        self.assertEqual(sorted(resp), sorted([
            '{"version":"vvv","deploymentId":"dddf"}',
            '{"version":"vvv","deploymentId":"dddb"}']))
