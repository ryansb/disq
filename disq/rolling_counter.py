import six
import time
from bisect import bisect
from collections import defaultdict


class RollingCounter(object):
    """
    The argument to RollingCounter (ttl_secs) indicates how long each event
    should count towards the total for its key, the default is 10 seconds, but
    you may use any float to indicate how long (in seconds) an event should
    stay in the count.

    Example:

    >>> rc = RollingCounter(ttl_secs=0.5)
    >>> rc.add('foo')
    >>> rc.max()
    'foo'
    >>> time.sleep(1)
    >>> rc.max()
    None
    >>> rc.keys()
    []

    Usage:
        use .add('itemname') to increment a count for some id
    """
    def __init__(self, ttl_secs=10):
        self._counts = defaultdict(list)
        if ttl_secs <= 0:
            raise ValueError("TTL must be >=0")
        self._ttl_seconds = ttl_secs

    def add(self, id):
        self._counts[id].append(time.time())

    def max(self, default=None):
        r = self._rank(reverse=True)
        if len(r):
            return r[0]
        return default

    def min(self, default=None):
        r = self._rank(reverse=False)
        if len(r):
            return r[0]
        return default

    def ranked(self):
        self._expire()
        return [
            (x[0], len(x[1])) for x in sorted(
                six.iteritems(self._counts),
                key=lambda x: len(x[1])
            )
        ]

    def count(self, id):
        self._expire()
        return len(self._counts[id])

    def _rank(self, reverse=False):
        self._expire()
        return [
            x[0] for x in sorted(
                six.iteritems(self._counts),
                key=lambda x: len(x[1]),
                reverse=reverse
            )
        ]

    def _expire(self):
        # cast key iterable to list because this loop can delete keys
        for k in list(six.iterkeys(self._counts)):
            # find the location where all times are less than (current - ttl)
            # and delete all lesser elements
            del self._counts[k][
                :bisect(self._counts[k], time.time() - self._ttl_seconds)
            ]
            if len(self._counts[k]) == 0:
                self.remove(k)

    def remove(self, id):
        del self._counts[id]

    def keys(self):
        self._expire()
        return list(six.iterkeys(self._counts))
