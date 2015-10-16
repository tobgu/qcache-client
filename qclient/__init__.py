import json
import requests
from qclient.node_ring import NodeRing

__version__ = "0.1.0"

RETRY_COUNT = 3


class QClient(object):
    def __init__(self, node_list):
        self.node_ring = NodeRing([node + '/' if not node.endswith('/') else node for node in node_list])
        self.unreachable_nodes = []

    def query(self, key, q, load_fn, load_fn_kwargs=None, content_type='text/csv', accept='application/json'):
        json_q = json.dumps(q)
        for _ in range(RETRY_COUNT):
            node = self.node_ring.get_node(key)

            # TODO: Connection error
            key_url = node + 'dataset/' + key

            print key_url
            response = requests.get(key_url, params={'q': json_q}, headers={'Accept': accept})
            if response.status_code == 200:
                return response.content

            if response.status_code == 404:
                response = requests.post(key_url, headers={'Content-type': content_type},
                                         data=load_fn(**load_fn_kwargs))
                if response.status_code != 201:
                    raise Exception('Unable to create dataset, status code {status_code}'.format(
                        status_code=response.status_code))
            elif response.status_code == 400:
                print response.content
                # TODO
                pass
            elif response.status_code == 406:
                print response.content
                # TODO
                pass
            elif response.status_code == 415:
                print response.content
                # TODO
                pass
            else:
                # TODO
                pass