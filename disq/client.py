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

from redis.connection import (ConnectionPool, UnixDomainSocketConnection,
                              Token)
from redis.exceptions import (
    ConnectionError,
    RedisError,
    ResponseError,
    TimeoutError,
)

from redis.client import (dict_merge, string_keys_to_dict, parse_client_list,
                          bool_ok, parse_config_get, parse_info)

from disq.rolling_counter import RollingCounter
from disq.parsers import bin_to_str, bin_to_int

DisqueError = RedisError


def parse_job_resp(response):
    if response is None:
        return None
    return [[bin_to_str(r[0]), bin_to_str(r[1]), six.binary_type(r[2])]
            for r in response]


def parse_cluster_nodes(response):
    nodes = {}
    fields = ['myself', 'id', 'address', 'flags', 'ping_sent', 'pong_received',
              'status']
    for c in six.binary_type(response).decode().splitlines():
        node = c.split(' ')
        nodes[node[0]] = dict(zip(fields, [node[2] == 'myself'] + node))
    return nodes


def parse_hello(response):
    fields = ['version', 'id', 'nodes']
    return dict(zip(fields, response[:2] + [response[2:]]))


def parse_time(response):
    return bin_to_int(response[0]), bin_to_int(response[1])


class DisqueAlpha(object):
    """
    Implementation of the Redis protocol.

    This abstract class provides a Python interface to all Redis commands
    and an implementation of the Redis protocol.

    Connection and Pipeline derive from this, implementing how
    the commands are sent and received to the Redis server
    """

    _job_score = None

    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict(
            'GETJOB', parse_job_resp
        ),
        string_keys_to_dict(
            'QLEN ACKJOB FASTACK', int
        ),
        string_keys_to_dict(
            'ADDJOB', lambda r: six.text_type(six.binary_type(r).decode())
        ),
        {
            'INFO': parse_info,
            'CLIENT GETNAME': lambda r: r and six.text_type(r),
            'CLIENT KILL': bool_ok,
            'CLIENT LIST': parse_client_list,
            'CLIENT SETNAME': bool_ok,
            'CONFIG GET': parse_config_get,
            'CONFIG RESETSTAT': bool_ok,
            'CONFIG SET': bool_ok,
            'CLUSTER NODES': parse_cluster_nodes,
            'HELLO': parse_hello,
            'TIME': parse_time,
        },
        string_keys_to_dict('BGREWRITEAOF', lambda r: True),
    )

    @classmethod
    def from_url(cls, url, **kwargs):
        """
        Return a Disque client object configured from the given URL.

        For example::

            disque://[:password]@localhost:6379
            unix://[:password]@/path/to/socket.sock

        Any additional querystring arguments and keyword arguments will be
        passed along to the ConnectionPool class's initializer. In the case
        of conflicting arguments, querystring arguments always win.
        """
        connection_pool = ConnectionPool.from_url(url, **kwargs)
        return cls(connection_pool=connection_pool)

    def __init__(self, host='localhost', port=7711,
                 password=None, socket_timeout=None,
                 socket_connect_timeout=None,
                 socket_keepalive=None, socket_keepalive_options=None,
                 connection_pool=None, unix_socket_path=None,
                 encoding='utf-8', encoding_errors='strict',
                 decode_responses=False, retry_on_timeout=False,
                 job_origin_ttl_secs=5):
        """
        job_origin_ttl_secs is the number of seconds to store counts of
        incoming jobs. The higher the throughput you're expecting, the lower
        this number should be.
        """

        kwargs = {
            'password': password,
            'socket_timeout': socket_timeout,
            'encoding': encoding,
            'encoding_errors': encoding_errors,
            'decode_responses': decode_responses,
            'retry_on_timeout': retry_on_timeout,
            'db': 0,
        }
        # based on input, setup appropriate connection args
        if unix_socket_path is not None:
            kwargs.update({
                'path': unix_socket_path,
                'connection_class': UnixDomainSocketConnection
            })
        else:
            # TCP specific options
            kwargs.update({
                'host': host,
                'port': port,
                'socket_connect_timeout': socket_connect_timeout,
                'socket_keepalive': socket_keepalive,
                'socket_keepalive_options': socket_keepalive_options,
            })

        if not connection_pool:
            connection_pool = ConnectionPool(**kwargs)

        self.response_callbacks = self.__class__.RESPONSE_CALLBACKS.copy()

        self.connection_pool = {'default': connection_pool}
        self.default_node = 'default'

        self._job_score = RollingCounter(ttl_secs=job_origin_ttl_secs)

        self.__connect_cluster(kwargs)

    def __connect_cluster(self, connection_kwargs):
        hi = self.hello()

        self.default_node = bin_to_str(hi['id'][:8])
        self.connection_pool.pop('default')
        for node, ip, port, version in hi['nodes']:
            connection_kwargs.update(dict(host=ip, port=port))
            self.connection_pool[bin_to_str(node[:8])] = ConnectionPool(
                **connection_kwargs)

    def __repr__(self):
        return "%s<%s>" % (type(self).__name__, repr(self.connection_pool))

    def set_response_callback(self, command, callback):
        "Set a custom Response Callback"
        self.response_callbacks[command] = callback

    __read_cmds = {'GETJOB': 0, 'ACKJOB': 0, 'FASTACK': 0}

    def _get_connection(self, command_name, **options):
        node = self.default_node
        if command_name in self.__read_cmds:
            node = self._job_score.max(node)

        pool = self.connection_pool.get(node)
        if pool is None:
            pool = self.connection_pool[self.default_node]
            node = self.default_node

        return pool.get_connection(command_name, **options), node

    def _release_connection(self, connection, node):
        return self.connection_pool[node].release(connection)

    def execute_command(self, *args, **options):
        "Execute a command and return a parsed response"
        command_name = args[0]
        connection, node = self._get_connection(command_name, **options)
        try:
            connection.send_command(*args)
            return self.parse_response(connection, command_name, **options)
        except (ConnectionError, TimeoutError) as e:
            connection.disconnect()
            if not connection.retry_on_timeout and isinstance(e, TimeoutError):
                raise
            connection.send_command(*args)
            return self.parse_response(connection, command_name, **options)
        finally:
            self._release_connection(connection, node)

    def parse_response(self, connection, command_name, **options):
        "Parses a response from the Redis server"
        response = connection.read_response()
        if command_name in self.response_callbacks:
            return self.response_callbacks[command_name](response, **options)
        return response

    # SERVER INFORMATION
    def bgrewriteaof(self):
        "Tell the Redis server to rewrite the AOF file from data in memory."
        return self.execute_command('BGREWRITEAOF')

    def client_kill(self, address):
        "Disconnects the client at ``address`` (ip:port)"
        return self.execute_command('CLIENT KILL', address)

    def client_list(self):
        "Returns a list of currently connected clients"
        return self.execute_command('CLIENT LIST')

    def client_getname(self):
        "Returns the current connection name"
        return self.execute_command('CLIENT GETNAME')

    def client_setname(self, name):
        "Sets the current connection name"
        return self.execute_command('CLIENT SETNAME', name)

    def client_pause(self, pause_msec):
        return self.execute_command('CLIENT PAUSE', pause_msec)

    def config_get(self, pattern="*"):
        "Return a dictionary of configuration based on the ``pattern``"
        return self.execute_command('CONFIG GET', pattern)

    def config_set(self, name, value):
        "Set config item ``name`` with ``value``"
        return self.execute_command('CONFIG SET', name, value)

    def config_resetstat(self):
        "Reset runtime statistics"
        return self.execute_command('CONFIG RESETSTAT')

    def config_rewrite(self):
        "Rewrite config file with the minimal change to reflect running config"
        return self.execute_command('CONFIG REWRITE')

    # Danger: debug commands ahead

    def debug_segfault(self):
        """ Danger: will segfault connected Disque instance"""
        return self.execute_command('DEBUG SEGFAULT')

    def debug_oom(self):
        """ Danger: will OOM connected Disque instance"""
        return self.execute_command('DEBUG OOM')

    def debug_flushall(self):
        return self.execute_command('DEBUG FLUSHALL')

    def debug_loadaof(self):
        return self.execute_command('DEBUG LOADAOF')

    def debug_sleep(self, sleep_secs):
        return self.execute_command('DEBUG SLEEP', sleep_secs)

    def debug_error(self, message):
        return self.execute_command('DEBUG ERROR', message)

    def debug_structsize(self):
        return self.execute_command('DEBUG STRUCTSIZE')

    # Cluster admin commands

    def cluster_meet(self, ip, port):
        return self.execute_command('CLUSTER MEET', ip, port)

    def cluster_nodes(self):
        return self.execute_command('CLUSTER NODES')

    def cluster_saveconfig(self):
        return self.execute_command('CLUSTER SAVECONFIG')

    def cluster_forget(self, node):
        return self.execute_command('CLUSTER FORGET', node)

    def _cluster_reset(self, reset):
        return self.execute_command('CLUSTER RESET', reset)

    def cluster_reset_hard(self):
        return self._cluster_reset(Token('HARD'))

    def cluster_reset_soft(self):
        return self._cluster_reset(Token('SOFT'))

    def cluster_info(self):
        return self.execute_command('CLUSTER INFO')

    def hello(self):
        return self.execute_command('HELLO')

    def info(self, section=None):
        """
        Returns a dictionary containing information about the Disque server

        The ``section`` option can be used to select a specific section
        of information

        Valid section names are:
            SERVER, CLIENTS, MEMORY, JOBS, QUEUES, PERSISTENCE, STATS, CPU
        """
        if section is None:
            return self.execute_command('INFO')
        else:
            return self.execute_command('INFO', section)

    def ping(self):
        "Ping the Redis server"
        return self.execute_command('PING')

    def shutdown(self):
        "Shutdown the server"
        try:
            self.execute_command('SHUTDOWN')
        except ConnectionError:
            # a ConnectionError here is expected
            return
        raise DisqueError("SHUTDOWN seems to have failed.")

    def slowlog_get(self, num=None):
        """
        Get the entries from the slowlog. If ``num`` is specified, get the
        most recent ``num`` items.
        """
        args = ['SLOWLOG GET']
        if num is not None:
            args.append(num)
        return self.execute_command(*args)

    def slowlog_len(self):
        "Get the number of items in the slowlog"
        return self.execute_command('SLOWLOG LEN')

    def slowlog_reset(self):
        "Remove all items in the slowlog"
        return self.execute_command('SLOWLOG RESET')

    def time(self):
        """
        Returns the server time as a 2-item tuple of ints:
        (seconds since epoch, microseconds into this second).
        """
        return self.execute_command('TIME')

    # BASIC JOB COMMANDS

    def addjob(self, queue, body, timeout_ms=0, replicate=0, delay_secs=0,
               retry_secs=-1, ttl_secs=0, maxlen=0, async=False):
        args = ['ADDJOB', queue, body, timeout_ms]
        if replicate > 0:
            args += [Token('REPLICATE'), replicate]
        if delay_secs > 0:
            args += [Token('DELAY'), delay_secs]
        if retry_secs >= 0:
            args += [Token('RETRY'), retry_secs]
        if ttl_secs > 0:
            args += [Token('TTL'), ttl_secs]
        if maxlen > 0:
            args += [Token('MAXLEN'), maxlen]
        if async:
            args += [Token('ASYNC')]

        return self.execute_command(*args)

    def getjob(self, queue, timeout_ms=0, count=1, queues=None):
        """ This function accepts a queue name as "queue" and a list of
        additional queues as "queues="

        e.x. `getjob('firstone', queues=['another', 'and', 'another'])`

        History: This function signature is odd because of Python 2.7
        compatibility.

        PEP 3102 means the following works in Python 3.x:
            def getjob(self, *queues, timeout_ms=0, count=1):
                return self.execute_command('GETJOB', *queues)

        But that throws a SyntaxError in anything less than Python 3
        """
        if queues is None:
            queues = []
        jobs = self.execute_command(
            'GETJOB', Token('TIMEOUT'), timeout_ms, Token('COUNT'), count,
            Token('FROM'), queue, *queues)
        if jobs is None:
            return
        for _, job_id, _ in jobs:
            # pull the origin node out of the job_id
            # https;//github.com/antirez/disque#job-ids
            self._job_score.add(job_id[2:10])
        return jobs

    def ackjob(self, *jobs):
        return self.execute_command('ACKJOB', *jobs)

    def fastack(self, *jobs):
        return self.execute_command('FASTACK', *jobs)

    def deljob(self, *jobs):
        return self.execute_command('DELJOB', *jobs)

    def show(self, job):
        return self.execute_command('SHOW', job)

    def scan(self):
        raise NotImplementedError("Sorry, SCAN isn't implemented in disque "
                                  "yet, so clients can't use it")

    def enqueue(self, *jobs):
        return self.execute_command('ENQUEUE', *jobs)

    def dequeue(self, *jobs):
        return self.execute_command('DEQUEUE', *jobs)

    # QUEUE COMMANDS

    def qlen(self, queue):
        return self.execute_command('QLEN', queue)

    def qstat(self, queue):
        raise NotImplementedError("Sorry, QSTAT isn't implemented in disque "
                                  "yet, so clients can't use it")

    def qpeek(self, queue, count=1):
        return self.execute_command('QPEEK', queue, count)
