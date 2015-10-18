import json
import os
import signal
import time
import pytest
from qclient import QClient, NoCacheAvailable


def data_source(content):
    return json.dumps([{'foo': content, 'bar': 123},
                       {'foo': 'abc',   'bar': 321}])


def spawn_servers(ports):
    pids = [os.spawnlp(os.P_NOWAIT, "qcache", "-p", port) for port in ports]

    # Let the processes start
    time.sleep(2.0)
    return pids


def kill_servers(pids):
    for pid in pids:
        os.kill(pid, signal.SIGTERM)


def test_basic_query_with_no_prior_data():
    pids = spawn_servers(['2222', '2223'])
    client = QClient(['http://localhost:2222', 'http://localhost:2223'])
    json_result = client.query('test_key', q=dict(select=['foo', 'bar']), load_fn=data_source,
                               load_fn_kwargs=dict(content='baz'), content_type='application/json')

    result = json.loads(json_result)
    assert result == [{'foo': 'baz', 'bar': 123}, {'foo': 'abc',   'bar': 321}]

    kill_servers(pids)


def test_no_nodes_available():
    client = QClient(['http://localhost:2222', 'http://localhost:2223'])
    with pytest.raises(NoCacheAvailable):
        client.query('test_key', q=dict(select=['foo', 'bar']), load_fn=data_source,
                     load_fn_kwargs=dict(content='baz'), content_type='application/json')

    assert client.statistics['http://localhost:2222']['connect_timeout'] == 1
    assert client.statistics['http://localhost:2223']['connect_timeout'] == 1
