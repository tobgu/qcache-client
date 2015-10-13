from collections import defaultdict
import random
import string
from qclient.node_ring import NodeRing


def test_distribution():
    random_strings = [''.join(random.choice(string.ascii_uppercase) for _ in range(6)) for _ in range(60000)]
    node_weights = {'aaa': 1, 'bbb': 2, 'ccc': 1, 'ddd': 1, 'eee': 1}

    ring = NodeRing(node_weights.keys(), node_weights)
    distribution = defaultdict(lambda: 1)
    for s in random_strings:
        distribution[ring.get_node(s)] += 1

    assert 9000 < distribution['aaa'] < 11000
    assert 18000 < distribution['bbb'] < 22000
    assert 9000 < distribution['ccc'] < 11000
    assert 9000 < distribution['ddd'] < 11000
    assert 9000 < distribution['eee'] < 11000

    # Drop a node
    ring.remove_node('aaa')

    removed_distribution = defaultdict(lambda: 1)
    for s in random_strings:
        removed_distribution[ring.get_node(s)] += 1

    assert 22000 < removed_distribution['bbb'] < 26000
    assert 11000 < removed_distribution['ccc'] < 13000
    assert 11000 < removed_distribution['ddd'] < 13000
    assert 11000 < removed_distribution['eee'] < 13000

    # Add the node back again and check that the distribution goes back to previous
    ring.add_node('aaa', weight=1)

    added_distribution = defaultdict(lambda: 1)
    for s in random_strings:
        added_distribution[ring.get_node(s)] += 1

    assert distribution['aaa'] == added_distribution['aaa']
    assert distribution['bbb'] == added_distribution['bbb']
    assert distribution['ccc'] == added_distribution['ccc']
    assert distribution['ddd'] == added_distribution['ddd']
    assert distribution['eee'] == added_distribution['eee']
