import pytest
import disq


@pytest.fixture()
def dq():
    return _get_client(disq.DisqueAlpha)


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


class TestDisqueServerCommands(object):
    def test_hello(self, dq):
        h = dq.hello()
        assert isinstance(h, list)
