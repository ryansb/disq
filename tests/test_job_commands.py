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

import json
import pytest
import time

import disq


def test_round_trip(dq):
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


def test_del_job(dq):
    qname = 'delq'
    assert dq.getjob(qname, timeout_ms=1) is None
    id = dq.addjob(qname, 'foobar')

    assert dq.qlen(qname) == 1
    assert dq.deljob(id) == 1
    assert dq.qlen(qname) == 0


def test_expiring_job(dq):
    qname = 'expq'
    assert dq.getjob(qname, timeout_ms=1) is None
    dq.addjob(qname, 'foobar', ttl_secs=1)
    assert dq.qlen(qname) == 1
    time.sleep(1.5)
    assert dq.qlen(qname) == 0


def test_delay_job(dq):
    qname = 'delayq'
    assert dq.getjob(qname, timeout_ms=1) is None
    dq.addjob(qname, 'foobar', delay_secs=1)
    assert dq.qlen(qname) == 0
    time.sleep(0.5)
    assert dq.qlen(qname) == 0
    time.sleep(1)
    assert dq.qlen(qname) == 1


def test_async_job(dq):
    qname = 'delayq'
    assert dq.getjob(qname, timeout_ms=1) is None
    dq.addjob(qname, 'foobar', async=True)
    assert dq.getjob(qname)


def test_unreplicated_job(dq, dq2):
    qname = 'unreplq'
    assert dq.getjob(qname, timeout_ms=1) is None
    assert dq2.getjob(qname, timeout_ms=1) is None
    id = dq.addjob(qname, 'foobar', replicate=1)
    print(id,)
    assert dq2.getjob(qname, timeout_ms=1) is None
    assert dq.getjob(qname, timeout_ms=1)


def test_overcrowded_job(dq, dq2):
    qname = 'crowdedq'
    assert dq.getjob(qname, timeout_ms=1) is None
    for i in range(11):
        dq.addjob(qname, 'foobar {}'.format(i), maxlen=10)
    with pytest.raises(disq.ResponseError):
        dq.addjob(qname, 'foobar', maxlen=10)


def test_json_job():
    qname = 'jsonq'
    job = {'hello': 'world'}
    q = disq.Disque()

    q.set_response_callback('GETJOB', disq.parsers.read_json_job)

    q.addjob(qname, json.dumps(job))
    j = q.getjob(qname)
    assert j[0][2] == job
