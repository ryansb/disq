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

import disq


def test_connection_from_url():
    c = disq.Disque.from_url('disque://localhost:7711')
    assert c.hello()
    assert c.__repr__().startswith('DisqueAlpha')


def test_connection_switch_workload():
    qname = 'swapworkloadq'
    first = disq.Disque(port=7711, record_job_origin=True)

    # add jobs on both nodes, but more on node 1
    for _ in range(90):
        first.addjob(qname, 'foobar')
    second = disq.Disque(port=7712, record_job_origin=True)
    for _ in range(10):
        second.addjob(qname, 'foobar')

    # Test that by default it uses the initially connected node (2)
    conn, node = second._get_connection('GETJOB')
    assert node == second.default_node
    second._release_connection(conn, node)

    # get a bunch of jobs
    for _ in range(100):
        second.getjob(qname)

    assert first.default_node in second._job_score.keys()

    # assert that the preferred read node is node 1
    conn, node = second._get_connection('GETJOB')
    assert node != second.default_node
    assert node == first.default_node
    second._release_connection(conn, node)

    # assert that the preferred write node is still node 2
    conn, node = second._get_connection('ADDJOB')
    assert node == second.default_node
    assert node != first.default_node
    second._release_connection(conn, node)
