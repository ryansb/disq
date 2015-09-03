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


def addjob(dq, **kwargs):
    def inner():
        dq.addjob(**kwargs)
    return inner


def getjob(dq, **kwargs):
    def inner():
        dq.getjob(**kwargs)
    return inner


def test_addjob_benchmarks(dq, benchmark):
    qname = 'benchwriteq'
    assert dq.getjob(qname, timeout_ms=1) is None
    benchmark(addjob(dq, queue=qname, body='foo'))
    assert dq.qlen(qname)


def test_addjob_no_replicate_bench(dq, benchmark):
    qname = 'benchnonreplq'
    assert dq.getjob(qname, timeout_ms=1) is None
    benchmark(addjob(dq, queue=qname, body='foo', replicate=1))
    assert dq.qlen(qname)


def test_addjob_async_bench(dq, benchmark):
    qname = 'benchasyncq'
    assert dq.getjob(qname, timeout_ms=1) is None
    benchmark(addjob(dq, queue=qname, body='foo', async=True))
    assert dq.qlen(qname)


def test_getjob_bench(dq, benchmark):
    qname = 'benchjobconsume'
    assert dq.getjob(qname, timeout_ms=1) is None
    for _ in six.moves.range(10000):
        dq.addjob(queue=qname, body='foo')
    benchmark(getjob(dq, queue=qname, timeout_ms=1))
