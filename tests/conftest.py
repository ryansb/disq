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
import disq


@pytest.fixture()
def dq():
    return _get_client(disq.DisqueAlpha)


@pytest.fixture()
def dq2():
    return _get_client(disq.DisqueAlpha, port=7712)


@pytest.fixture()
def dq3():
    return _get_client(disq.DisqueAlpha, port=7713)


@pytest.fixture()
def dq4():
    return _get_client(disq.DisqueAlpha, port=7714)


def _get_client(cls, request=None, **kwargs):
    params = {'host': 'localhost', 'port': 7711}
    params.update(kwargs)
    client = cls(**params)
    client.debug_flushall()
    if request:
        def teardown():
            client.debug_flushall()
            client.connection_pool.disconnect()
        request.addfinalizer(teardown)
    return client
