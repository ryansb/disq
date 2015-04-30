# Copyright 2015 Ryan Brown <sb@ryansb.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import six


class TestDisqueServerCommands(object):
    def test_multiclient(self, dq, dq2):
        h1 = dq.cluster_nodes()
        h2 = dq2.cluster_nodes()
        first = [n for n in six.itervalues(h1) if n['myself']][0]
        second = [n for n in six.itervalues(h2) if n['myself']][0]
        # Make sure the cluster is connected
        assert first['id'] in h2
        assert second['id'] in h1
        assert not h1[second['id']]['myself']
        assert not h2[first['id']]['myself']

    def test_hello(self, dq):
        hi = dq.hello()
        assert len(hi['nodes']) == 4
        assert hi['id']in [h[0] for h in hi['nodes']]

    def test_rewriteaof(self, dq, dq2, dq3, dq4):
        assert dq.bgrewriteaof()
        assert dq2.bgrewriteaof()
        assert dq3.bgrewriteaof()
        assert dq4.bgrewriteaof()

    def test_info(self, dq):
        i = dq.info()
        assert i.keys()
        i = dq.info('SERVER')
        assert type(i['process_id']) in six.integer_types

    def test_time(self, dq):
        t = dq.time()
        assert len(t) == 2
