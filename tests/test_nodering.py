from collections import defaultdict
import random
import string
import pytest
from qclient.node_ring import NodeRing


def test_distribution():
    strings = [str(i) for i in range(60000)]
    node_weights = {'aaa': 1, 'bbb': 2, 'ccc': 1, 'ddd': 1, 'eee': 1}

    ring = NodeRing(node_weights.keys(), node_weights)
    distribution = defaultdict(list)
    for s in strings:
        distribution[ring.get_node(s)].append(s)

    assert 9000 < len(distribution['aaa']) < 11000
    assert 18000 < len(distribution['bbb']) < 22000
    assert 9000 < len(distribution['ccc']) < 11000
    assert 9000 < len(distribution['ddd']) < 11000
    assert 9000 < len(distribution['eee']) < 11000

    # Drop a node, the new distributions should be super sets of the previous.
    # Existing nodes should not move.
    ring.remove_node('aaa')

    removed_distribution = defaultdict(list)
    for s in strings:
        removed_distribution[ring.get_node(s)].append(s)

    assert 22000 < len(removed_distribution['bbb']) < 26000
    assert 11000 < len(removed_distribution['ccc']) < 13000
    assert 11000 < len(removed_distribution['ddd']) < 13000
    assert 11000 < len(removed_distribution['eee']) < 13000

    assert set(distribution['bbb']) <= set(removed_distribution['bbb'])
    assert set(distribution['ccc']) <= set(removed_distribution['ccc'])
    assert set(distribution['ddd']) <= set(removed_distribution['ddd'])
    assert set(distribution['eee']) <= set(removed_distribution['eee'])

    # Add the node back again and check that the distribution goes back to previous
    ring.add_node('aaa', weight=1)

    added_distribution = defaultdict(list)
    for s in strings:
        added_distribution[ring.get_node(s)].append(s)

    assert distribution['aaa'] == added_distribution['aaa']
    assert distribution['bbb'] == added_distribution['bbb']
    assert distribution['ccc'] == added_distribution['ccc']
    assert distribution['ddd'] == added_distribution['ddd']
    assert distribution['eee'] == added_distribution['eee']


def test_requires_at_least_one_node_at_creation():
    with pytest.raises(AssertionError):
        NodeRing([])


def test_get_node_no_nodes_available():
    ring = NodeRing(['12345'])
    ring.remove_node('12345')

    assert ring.get_node('12345') is None
