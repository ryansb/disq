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

import sys
import pytest

from disq.client import bin_to_int


class TestConversions(object):
    def test_binary_to_int(self):
        assert bin_to_int(b'0') == 0
        assert bin_to_int(b'10000') == 10000
        assert bin_to_int(b'-90') == -90
        if sys.version_info[0] < 3:
            with pytest.raises(ValueError):
                bin_to_int('abc')
            assert bin_to_int(1) == 1
        else:
            with pytest.raises(TypeError):
                bin_to_int('abc')
            with pytest.raises(ValueError):
                bin_to_int(1)
