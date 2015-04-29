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

import pytest
import six

import disq


class TestDisqueJobCommands(object):
    def test_round_trip(self, dq):
        qname = 'rttq'
        assert dq.getjob('empty', timeout_ms=1) is None
        id = dq.addjob(qname, 'foobar')
        assert id
        jobs = dq.getjob(qname, timeout_ms=1)
        assert jobs
        assert len(jobs) == 1
        job = jobs[0]
        assert job[0] == qname
        assert job[1] == id
        assert job[2] == b'foobar'

    def test_del_job(self, dq):
        pass
