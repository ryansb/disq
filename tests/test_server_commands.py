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


class TestDisqueServerCommands(object):
    def test_multiclient(self, dq, dq2):
        h1 = dq.cluster_nodes()
        h2 = dq2.cluster_nodes()
