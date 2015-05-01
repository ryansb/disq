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
try:
    import ujson as json
except ImportError:
    import json


def bin_to_int(raw):
    return int(six.binary_type(raw).decode())


def bin_to_str(raw):
    return six.text_type(six.binary_type(raw).decode())


def read_json_job(response):
    if response is None:
        return None
    return [
        [
            bin_to_str(r[0]),
            bin_to_str(r[1]),
            json.loads(bin_to_str(r[2]))
        ]
        for r in response]


def write_json_job(job):
    return six.binary_type(json.dumps(job))
