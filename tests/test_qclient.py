import json
import os
import random
import string
import time
import pytest
import signal

import requests

from qclient import QClient, NoCacheAvailable, NodeRing, TooManyConsecutiveErrors, UnexpectedServerResponse

# Version to test against
QCACHE_VERSION = '0.9.3'


def data_source(content):
    return json.dumps([{'foo': content, 'bar': 123},
                       {'foo': 'abc', 'bar': 321}])


@pytest.yield_fixture
def qcache_factory():
    class QCacheFactory(object):
        def __init__(self):
            self.caches = set()
            self.platform = 'docker'

        @staticmethod
        def wait_qcaches(protocol="http", *ports):
            for port in ports:
                attempt = 0
                while True:
                    try:
                        resp = requests.get("{protocol}://localhost:{port}/qcache/status".format(protocol=protocol, port=port), timeout=(1.0, 1.0))
                        if resp.status_code == 200:
                            return
                        print("Unexpected response code waiting for qcache: {code}".format(code=resp.status_code))
                    except Exception:
                        if attempt > 5:
                            print("QCache at port {port} not reachable".format(port=port))
                            return

                        time.sleep(1.0)
                        attempt += 1

        def spawn_caches(self, *ports, **kwargs):
            cert_dir = os.path.dirname(os.path.abspath(__file__)) + '/../tls'
            self.platform = kwargs.get('platform', 'docker')
            protocol = "https" if 'certfile' in kwargs else "http"
            if self.platform == 'docker':
                # Mainly for Travis and other systems where QCache is not installed
                names = []
                for port in ports:
                    name = "qcache{port}".format(port=port)
                    names.append(name)
                    args = [os.P_NOWAIT, "docker", "docker",
                            "run",
                            "--net=host",
                            "-v", "{dir}:/certs".format(dir=cert_dir),
                            "--rm",
                            "--name", name,
                            "tobgu/qcache:{version}".format(version=QCACHE_VERSION),
                            "qcache", "--port={port}".format(port=port)]

                    if 'certfile' in kwargs:
                        args.append("--cert-file=/certs/%s" % kwargs['certfile'])
                    if 'cafile' in kwargs:
                        args.append("--ca-file=/certs/%s" % kwargs['cafile'])
                    if 'auth' in kwargs:
                        args.append('--basic-auth=%s' % kwargs['auth'])

                    os.spawnlp(os.P_NOWAIT, "docker", "docker", "rm", "-f", name)
                    time.sleep(1.0)
                    os.spawnlp(*args)

                # Let the processes start
                self.wait_qcaches(protocol, *ports)
                self.caches.update(names)
                return names
            else:
                # Mainly for local development
                pids = []
                for port in ports:
                    args = [os.P_NOWAIT,
                            "qcache", "qcache",
                            "--port={port}".format(port=port)]

                    if 'certfile' in kwargs:
                        args.append("--cert-file=/%s/%s" % (cert_dir, kwargs['certfile']))
                    if 'cafile' in kwargs:
                        args.append("--ca-file=/%s/%s" % (cert_dir, kwargs['cafile']))
                    if 'auth' in kwargs:
                        args.append('--basic-auth=%s' % kwargs['auth'])

                    pids.append(os.spawnlp(*args))

                # Let the processes start
                self.wait_qcaches(protocol, *ports)
                self.caches.update(pids)
                return pids

        def kill_caches(self, *names):
            if self.platform == 'docker':
                for name in names:
                    os.spawnlp(os.P_NOWAIT, "docker", "docker", "kill", "{name}".format(name=name))
                    self.caches.discard(name)
            else:
                for pid in names:
                    os.kill(pid, signal.SIGTERM)

            time.sleep(1)

        def kill_all(self):
            self.kill_caches(*list(self.caches))

    factory = QCacheFactory()
    yield factory
    factory.kill_all()


def data_source2(content):
    return json.dumps([{'foo': content, 'bar': 123},
                       {'foo': 'abc', 'bar': 321}])


def test_warmup(qcache_factory):
    # This test is just there fetch docker images, etc. before running the
    # real tests. Travis seems to flake out otherwise.
    qcache_factory.spawn_caches('2222')


def test_basic_query_using_post_with_no_prior_data(qcache_factory):
    qcache_factory.spawn_caches('2222', '2223')
    client = QClient(['http://localhost:2222', 'http://localhost:2223'], read_timeout=1.0)

    # A query of this size is not possible to execute using a GET (on my machine at least)
    where = ["in", "foo", [("%s" % i) for i in range(300000)] + ["baz"]]
    result = client.query('test_key', q=dict(select=['foo', 'bar'], where=where), load_fn=data_source2,
                          load_fn_kwargs=dict(content='baz'), content_type='application/json', post_query=True)

    result_data = json.loads(result.content.decode('utf8'))
    assert result_data == [{'foo': 'baz', 'bar': 123}]


def test_query_using_compression(qcache_factory):
    qcache_factory.spawn_caches('2222')
    client = QClient(['http://localhost:2222'])
    result = client.query('test_key', q={}, load_fn=data_source, content_type='application/json',
                          load_fn_kwargs=dict(content='baz'), query_headers={'Accept-Encoding': 'lz4,gzip'})

    assert result.encoding == 'lz4'


def test_circuit_breaker_kicks_in_after_too_many_failures(qcache_factory):
    qcache_factory.spawn_caches('2222', '2223')
    client = QClient(['http://localhost:2222', 'http://localhost:2223'], read_timeout=0.2,
                     consecutive_error_count_limit=5)

    # A query of this size is not possible to execute using a GET (on my machine at least)
    # The circuit breaker should kick in after configured number of retries.
    where = ["in", "foo", [("%s" % i) for i in range(300000)] + ["baz"]]
    with pytest.raises(TooManyConsecutiveErrors):
        client.query('test_key', q=dict(select=['foo', 'bar'], where=where), load_fn=data_source2,
                     load_fn_kwargs=dict(content='baz'), content_type='application/json')


def test_basic_query_with_no_prior_data(qcache_factory):
    qcache_factory.spawn_caches('2222', '2223')
    client = QClient(['http://localhost:2222', 'http://localhost:2223'])
    result = client.query('test_key', q=dict(select=['foo', 'bar']), load_fn=data_source,
                          load_fn_kwargs=dict(content='baz'), content_type='application/json')

    result_data = json.loads(result.content.decode('utf8'))

    assert result_data == [{'foo': 'baz', 'bar': 123}, {'foo': 'abc', 'bar': 321}]
    assert 'baz' in str(result)


def test_no_nodes_available(qcache_factory):
    if qcache_factory.platform == "docker":
        qcache_factory.kill_caches('2222', '3333')

    client = QClient(['http://localhost:2222', 'http://localhost:2223'])
    with pytest.raises(NoCacheAvailable):
        client.query('test_key', q=dict(select=['foo', 'bar']), load_fn=data_source,
                     load_fn_kwargs=dict(content='baz'), content_type='application/json')

    # The exact behaviour seem to vary between platforms. Connection timout on Linux
    # Connection Error on MacOSX.
    statistics = client.statistics
    assert statistics['http://localhost:2222']['connect_timeout'] + \
           statistics['http://localhost:2222']['connection_error'] == 1
    assert statistics['http://localhost:2223']['connect_timeout'] + \
           statistics['http://localhost:2223']['connection_error'] == 1
    assert statistics is client.get_statistics()

    # Statistics reset after getting it
    assert len(client.get_statistics()) == 0


def test_no_nodes_available_then_node_becomes_available_again(qcache_factory):
    client = QClient(['http://localhost:2222', 'http://localhost:2223'])
    with pytest.raises(NoCacheAvailable):
        client.get('test_key', q=dict())

    # Start a server and validate that the client resumes the connection
    qcache_factory.spawn_caches('2222')
    result = client.get('test_key', q=dict())

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


def test_one_node_unavailable_then_appears(qcache_factory):
    names1 = qcache_factory.spawn_caches('2222')
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
    qcache_factory.spawn_caches('2223')
    for _ in range(10):
        client.post(key, content, content_type='application/json')

    # Kill the first server to make sure that no stale data exists
    # and perform a get to verify that the data has indeed been moved to
    # the original destination node.
    qcache_factory.kill_caches(*names1)
    assert client.get(key, q={}) is not None
    assert client.statistics['http://localhost:2223']['resurrections']


def test_post_with_invalid_content(qcache_factory):
    qcache_factory.spawn_caches('2222')
    client = QClient(['http://localhost:2222'], read_timeout=1.0)
    with pytest.raises(UnexpectedServerResponse) as e:
        client.post("test-key", "non-expected-json", content_type='application/json')

    assert "Internal Server Error" in str(e)
    assert "code 500" in str(e)


def test_delete(qcache_factory):
    qcache_factory.spawn_caches('2222')
    nodes = ['http://localhost:2222']
    client = QClient(nodes)
    content = data_source('foo')
    key = '12345'

    client.post(key, content, content_type='application/json')
    assert client.get(key, q={}) is not None

    client.delete(key)
    assert client.get(key, q={}) is None


def test_https(qcache_factory):
    qcache_factory.spawn_caches('2222', certfile='host.pem')
    nodes = ['https://localhost:2222']
    client = QClient(nodes, verify='tls/ca.pem')
    content = data_source('foo')
    key = '12345'

    client.post(key, content, content_type='application/json')
    assert client.get(key, q={}) is not None

    client.delete(key)
    assert client.get(key, q={}) is None


def test_https_with_basic_auth(qcache_factory):
    qcache_factory.spawn_caches('2222', certfile='host.pem', auth='abc:123')
    nodes = ['https://localhost:2222']
    client = QClient(nodes, verify='tls/ca.pem', auth=('abc', '123'))
    content = data_source('foo')
    key = '12345'

    client.post(key, content, content_type='application/json')
    assert client.get(key, q={}) is not None

    client.delete(key)
    assert client.get(key, q={}) is None


def test_https_with_client_cert_verification(qcache_factory):
    qcache_factory.spawn_caches('2222', certfile='host.pem', cafile='ca.pem')
    client = QClient(['https://localhost:2222'], verify='tls/ca.pem', cert='tls/host.pem')

    key = '12345'
    client.post(key, data_source('foo'), content_type='application/json')
    assert client.get(key, q={}) is not None


def test_https_with_client_cert_verification_no_cert_presented(qcache_factory):
    qcache_factory.spawn_caches('2222', certfile='host.pem', cafile='ca.pem')
    client = QClient(['https://localhost:2222'], verify='tls/ca.pem')

    with pytest.raises(NoCacheAvailable):
        client.post("a", data_source('foo'), content_type='application/json')


def data_source_csv():
    return "foo,bar\r\ncba,123\r\nabc,321"


def test_query_with_custom_post_header(qcache_factory):
    qcache_factory.spawn_caches('2222')
    client = QClient(['http://localhost:2222'])
    result = client.query('test_key', q=dict(where=['==', 'bar', "'321'"]), load_fn=data_source_csv,
                          post_headers={'X-QCache-types': 'bar=string'})

    result_data = json.loads(result.content.decode('utf8'))
    assert result_data == [{'foo': 'abc', 'bar': '321'}]


def test_query_with_custom_query_header(qcache_factory):
    qcache_factory.spawn_caches('2222')
    client = QClient(['http://localhost:2222'])
    result = client.query('test_key', q=dict(where=['like', 'foo', "'%b%'"]), load_fn=data_source_csv,
                          query_headers={'X-QCache-filter-engine': 'pandas'})
    result_data = json.loads(result.content.decode('utf8'))
    assert result_data == [{'foo': 'cba', 'bar': 123}, {'foo': 'abc', 'bar': 321}]


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


@pytest.mark.skipif(True, 'Only used for occasional performance tests')
def xtest_repeated_posts_on_small_dataset():
    client = QClient(['http://localhost:8882'])
    content = data_source('foo')

    for x in range(1000):
        t0 = time.time()
        client.post(id_generator(), content, content_type='application/json')
        print("Loop: {num}, duration: {dur}".format(num=x, dur=time.time() - t0))
