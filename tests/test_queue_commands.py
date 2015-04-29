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
            dq.addjob(qname, 'foo {}'.format(i))
            assert dq.qlen(qname) == i + 1
        for i in range(100):
            dq.getjob(qname)
            assert dq.qlen(qname) == 99 - i
