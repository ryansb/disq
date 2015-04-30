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

import time
from disq.rolling_counter import RollingCounter


class TestRollingCounter(object):
    def test_rank(self):
        rc = RollingCounter()
        for _ in range(100):
            rc.add('foo')
        for _ in range(10):
            rc.add('bar')
        for _ in range(40):
            rc.add('baz')
        for _ in range(60):
            rc.add('quux')
        assert rc.max() == 'foo'
        assert rc.min() == 'bar'
        assert [x[0] for x in rc.ranked()] == ['bar', 'baz', 'quux', 'foo']

    def test_expiration(self):
        rc = RollingCounter(ttl_secs=0.5)
        for _ in range(10):
            rc.add('foo')
        for _ in range(5):
            rc.add('bar')
        assert len(rc.keys()) == 2
        assert rc.count('foo') == 10
        time.sleep(1)
        assert len(rc.keys()) == 0
        assert rc.max() is None
        assert rc.min() is None
        assert not rc.ranked()
        assert rc.count('foo') == 0
