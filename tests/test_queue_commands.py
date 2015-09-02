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


class TestDisqueQueueCommands(object):
    def test_qlen(self, dq):
        qname = 'qlenq'
        assert dq.getjob(qname, timeout_ms=1) is None
        for i in range(100):
            dq.addjob(qname, 'foo {0}'.format(i))
            assert dq.qlen(qname) == i + 1
        for i in range(100):
            dq.getjob(qname)
            assert dq.qlen(qname) == 99 - i

    def test_qscan(self, dq):
        dq.addjob('testq', 'foobar')
        result = dq.qscan()
        assert result[0] == b'0'
        assert result[1] == [b'testq']

    def test_qscan_cursor(self, dq):
        self._populate_queues(dq)
        queues = []
        cursor = None

        while cursor != 0:
            cursor, new_queues = dq.qscan(cursor or 0)
            queues += new_queues
            cursor = int(cursor)

        assert len(list(set(queues))) == 512

    def test_qscan_busyloop(self, dq):
        self._populate_queues(dq)
        result = dq.qscan(busyloop=True)
        assert len(list(set(result[1]))) == 512

    def test_qscan_minlen(self, dq):
        dq.addjob('testq1', 'foobar1')
        dq.addjob('testq2', 'foobar1')
        dq.addjob('testq2', 'foobar2')

        result = dq.qscan(0, minlen=2, busyloop=True)
        assert result[1] == [b'testq2']

    def test_qscan_maxlen(self, dq):
        dq.addjob('testq1', 'foobar1')
        dq.addjob('testq2', 'foobar1')
        dq.addjob('testq2', 'foobar2')

        result = dq.qscan(0, maxlen=1, busyloop=True)
        assert result[1] == [b'testq1']

    def test_qscan_importrate(self, dq, dq2):
        dq.addjob('testq1', 'foobar')
        dq.addjob('testq2', 'foobar')
        dq.addjob('testq3', 'foobar')
        dq.addjob('testq4', 'foobar')

        dq2.getjob('testq1')
        dq2.getjob('testq2')
        dq2.getjob('testq3')
        dq2.getjob('testq4')

        result = dq2.qscan(importrate=1, busyloop=True)
        assert len(result[1]) == 4

    def _populate_queues(self, dq):
        for i in range(512):
            dq.addjob("testq{0}".format(i), 'foobar')
