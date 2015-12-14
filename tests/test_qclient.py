import json
import os
import signal
import time
import pytest
from qclient import QClient, NoCacheAvailable, NodeRing


def data_source(content):
    return json.dumps([{'foo': content, 'bar': 123},
                       {'foo': 'abc',   'bar': 321}])


def spawn_servers(*ports, **kwargs):
    args = [os.P_NOWAIT, "qcache", "qcache"]
    if 'certfile' in kwargs:
        args.append("--cert-file=%s" % kwargs['certfile'])

    if 'auth' in kwargs:
        args.append('--basic-auth=%s' % kwargs['auth'])

    pids = [os.spawnlp(*(args + ["--port=%s" % port])) for port in ports]

    # Let the processes start
    time.sleep(2.0)
    return pids


def kill_servers(pids):
    for pid in pids:
        os.kill(pid, signal.SIGTERM)


def test_basic_query_with_no_prior_data():
    pids = spawn_servers('2222', '2223')
    client = QClient(['http://localhost:2222', 'http://localhost:2223'])
    result = client.query('test_key', q=dict(select=['foo', 'bar']), load_fn=data_source,
                          load_fn_kwargs=dict(content='baz'), content_type='application/json')

    result_data = json.loads(result.content)
    assert result_data == [{'foo': 'baz', 'bar': 123}, {'foo': 'abc',   'bar': 321}]

    kill_servers(pids)


def test_no_nodes_available():
    client = QClient(['http://localhost:2222', 'http://localhost:2223'])
    with pytest.raises(NoCacheAvailable):
        client.query('test_key', q=dict(select=['foo', 'bar']), load_fn=data_source,
                     load_fn_kwargs=dict(content='baz'), content_type='application/json')

    # The exact behaviour seem to vary between platforms. Connection timout on Linux
    # Connection Error on MacOSX.
    assert client.statistics['http://localhost:2222']['connect_timeout'] + \
           client.statistics['http://localhost:2222']['connection_error'] == 1
    assert client.statistics['http://localhost:2223']['connect_timeout'] + \
           client.statistics['http://localhost:2223']['connection_error'] == 1


def test_no_nodes_available_then_node_becomes_available_again():
    client = QClient(['http://localhost:2222', 'http://localhost:2223'])
    with pytest.raises(NoCacheAvailable):
        client.get('test_key', q=dict())

    # Start a server and validate that the client resumes the connection
    pids = spawn_servers('2222')
    result = client.get('test_key', q=dict())
    kill_servers(pids)

    assert result is None
    assert client.statistics['http://localhost:2222']['retry_error'] == 1
    assert client.statistics['http://localhost:2223']['retry_error'] == 2


def _infinite_keys():
    i = 1000
    while True:
        yield str(i)
        i += 1


def _get_key_on_node(nodes, destination_node):
    # This is slightly intrusive but will provide us with a key that will
    # hash to the requested destination node.
    node_ring = NodeRing(nodes)
    for key in _infinite_keys():
        if node_ring.get_node(key) == destination_node:
            return key


def test_one_node_unavailable_then_appears():
    pids1 = spawn_servers('2222')
    nodes = ['http://localhost:2222', 'http://localhost:2223']
    client = QClient(nodes)
    key = _get_key_on_node(nodes, 'http://localhost:2223')

    content = data_source('foo')
    client.post(key, content, content_type='application/json')

    # Verify that the data is indeed available even though the primary destination node
    # was not available.
    assert client.get(key, q={}) is not None

    # Now start the server that the key is destined for and re-post the data
    # a number of times until it is moved to the destination node.
    pids2 = spawn_servers('2223')
    for _ in range(10):
        client.post(key, content, content_type='application/json')

    # Kill the first server to make sure that no stale data exists
    # and perform a get to verify that the data has indeed been moved to
    # the original destination node.
    kill_servers(pids1)
    assert client.get(key, q={}) is not None
    assert client.statistics['http://localhost:2223']['resurrections']

    kill_servers(pids2)


def test_delete():
    pids = spawn_servers('2222')
    nodes = ['http://localhost:2222']
    client = QClient(nodes)
    content = data_source('foo')
    key = '12345'

    client.post(key, content, content_type='application/json')
    assert client.get(key, q={}) is not None

    client.delete(key)
    assert client.get(key, q={}) is None

    kill_servers(pids)


def test_https():
    pids = spawn_servers('2222', certfile='tests/host.pem')
    nodes = ['https://localhost:2222']
    client = QClient(nodes, verify='tests/rootCA.crt')
    content = data_source('foo')
    key = '12345'

    client.post(key, content, content_type='application/json')
    assert client.get(key, q={}) is not None

    client.delete(key)
    assert client.get(key, q={}) is None

    kill_servers(pids)


def test_https_with_basic_auth():
    pids = spawn_servers('2222', certfile='tests/host.pem', auth='abc:123')
    nodes = ['https://localhost:2222']
    client = QClient(nodes, verify='tests/rootCA.crt', auth=('abc', '123'))
    content = data_source('foo')
    key = '12345'

    client.post(key, content, content_type='application/json')
    assert client.get(key, q={}) is not None

    client.delete(key)
    assert client.get(key, q={}) is None

    kill_servers(pids)


def data_source_csv():
    return "foo,bar\r\ncba,123\r\nabc,321"



def test_query_with_custom_post_header():
    pids = spawn_servers('2222')
    client = QClient(['http://localhost:2222'])
    result = client.query('test_key', q=dict(where=['==', 'bar', "'321'"]), load_fn=data_source_csv,
                          post_headers={'X-QCache-types': 'bar=string'})

    result_data = json.loads(result.content)
    assert result_data == [{'foo': 'abc',   'bar': '321'}]

    kill_servers(pids)