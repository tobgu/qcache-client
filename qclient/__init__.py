from contextlib import contextmanager
import json
import requests
from requests.exceptions import ConnectionError, ConnectTimeout, ReadTimeout, RequestException
from qclient.node_ring import NodeRing
from collections import defaultdict

__version__ = "0.5.1"


class QClientException(Exception):
    """
    Base exception for all other exceptions raised in QClient
    """
    pass


class NoCacheAvailable(QClientException):
    """
    Raised when no qcaches are deemed reachable from the currnent node.
    """
    pass


class TooManyConsecutiveErrors(QClientException):
    """
    Raised to when operations have been retried too many times. This is done to avoid overloading
    resources with failing requests. If raised it's probably an indicator of some misconfiguration
    or network problem.
    """
    pass


class UnexpectedServerResponse(QClientException):
    """
    Raised when the QCache server responded with an HTTP code that could not be interpreted.
    """
    pass


class MalformedQueryException(QClientException):
    """
    Raised when the server was unable to process the query because of errors in syntax or semantics.
    """
    pass


class UnsupportedAcceptType(QClientException):
    """
    Raised when the server cannot produce a response of the requested type.
    """
    pass


def _node_statisticts():
    return dict(connect_timeout=0,
                connection_error=0,
                read_timeout=0,
                unknown_error=0,
                resurrections=0,
                retry_error=0)


class QueryResult(object):
    """
    Returned upon successful query response.

    :param content: A byte string containing the body received from the server.
    :param unsliced_result_len: contains the complete result length. If no slicing/pagination is applied this will equal the number of records returned.
    :param encoding: Content-Encoding as set by the server
    """
    def __init__(self, response):
        self.content = response.content
        self.unsliced_result_len = int(response.headers['X-QCache-unsliced-length'])
        self.encoding = response.headers.get('Content-Encoding')
        self.statistics = get_request_statistics(response)

    def add_stats(self, stats):
        self.statistics.update(stats)

    def __repr__(self):
        return "{class_name}(content={content}, unsliced_result_len={unsliced_result_len}, encoding={encoding})".format(
            class_name=self.__class__.__name__,
            content=self.content,
            unsliced_result_len=self.unsliced_result_len,
            encoding=self.encoding)

    __str__ = __repr__


def get_request_statistics(response, prefix="get_"):
    stats_header = response.headers.get('X-QCache-stats')
    if stats_header:
        return {prefix + k.strip(): float(v) for k, v in dict(t.split('=') for t in stats_header.split(',')).items()}

    return {}


class QClient(object):
    """
    Main client class.

    Basic example:

    >>> client = QClient(node_list=('http://host1:9401', 'http://host2:9401', 'http://host3:9401'))
    >>> result = client.get('someKey', {'select': ['col1', 'col2', 'col3'], 'where': ['<', 'col', 1]})

    :param node_list: List or other iterables with addresses to qcache servers.
                      Eg. ['http://host1:9401', 'http://host2:9401']
    :param connect_timeout: Number of seconds to wait until connection timeout occurs.
    :param read_timeout: Number of seconds to wait until read timeout occurs.
    :param verify: If https is used controls if the host certificate should be verified.
    :param auth: Tuple (username, password), used for basic auth.
    :param consecutive_error_count_limit: Number of times to retry operations before giving up.
    :param trust_env: Whether to pick up proxies etc. from environment variables etc. Setting this
                      to False will cut request latency by 5-6ms because of less processing required in
                      the requests library.
    """

    def __init__(self,
                 node_list,
                 connect_timeout=1.0,
                 read_timeout=2.0,
                 verify=True,
                 cert=None,
                 auth=None,
                 consecutive_error_count_limit=10,
                 trust_env=True):
        self.node_ring = NodeRing(node_list)

        self.session = requests.session()
        self.session.cert = cert
        self.session.verify = verify
        self.session.auth = auth
        self.session.timeout = (connect_timeout, read_timeout)
        self.session.trust_env = trust_env

        self.failing_nodes = set()
        self.check_interval = 10
        self.check_attempt_count = 0
        self.consecutive_error_count = 0
        self.consecutive_error_count_limit = consecutive_error_count_limit
        self.statistics = None
        self._clear_statistics()

    def _clear_statistics(self):
        self.statistics = defaultdict(_node_statisticts)

    def _node_for_key(self, key):
        node = self.node_ring.get_node(key)
        if not node:
            # Check all caches in unreachable nodes, if none exist. Fail!
            self._test_dropped_nodes()
            node = self.node_ring.get_node(key)
            if not node:
                raise NoCacheAvailable('No QCaches reachable')

        return node

    def _test_dropped_nodes(self):
        # Test all nodes that are currently on the fail list. Any node that responds
        # gets reinserted into the node ring. A more selective strategy may be required
        # in the future but keep it simple for now.
        for node in list(self.failing_nodes):
            status_url = self._status_url(node)
            try:
                response = self.session.get(status_url)
                if response.status_code == 200:
                    self.node_ring.add_node(node)
                    self.failing_nodes.remove(node)
                    self.statistics[node]['resurrections'] += 1
            except RequestException:
                self.statistics[node]['retry_error'] += 1

    def _drop_node(self, node):
        self.node_ring.remove_node(node)
        self.failing_nodes.add(node)

    def _check_dropped_nodes(self):
        if self.check_attempt_count % self.check_interval == 0:
            self._test_dropped_nodes()

        self.check_attempt_count += 1

    @contextmanager
    def _connection_error_manager(self, node):
        try:
            yield
            self.consecutive_error_count = 0
        except ConnectTimeout:
            self.statistics[node]['connect_timeout'] += 1
            self._drop_node(node)
            self.consecutive_error_count += 1
        except ConnectionError:
            self.statistics[node]['connection_error'] += 1
            self._drop_node(node)
            self.consecutive_error_count += 1
        except ReadTimeout:
            self.statistics[node]['read_timeout'] += 1
            self._drop_node(node)
            self.consecutive_error_count += 1
        finally:
            if self.consecutive_error_count >= self.consecutive_error_count_limit:
                self.consecutive_error_count = 0
                raise TooManyConsecutiveErrors('Too many errors occurred while trying operation: {stat}'.format(
                    stat=dict(self.statistics)))

    @staticmethod
    def _status_url(node):
        new_node = node if node.endswith('/') else node + '/'
        return new_node + 'qcache/status'

    @staticmethod
    def _key_url(node, key):
        new_node = node if node.endswith('/') else node + '/'
        return new_node + 'qcache/dataset/' + key

    def get_statistics(self):
        statistics = self.statistics
        self._clear_statistics()
        return statistics

    def get(self, key, q, accept='application/json', post_query=False, query_headers=None):
        """
        Execute query and return result.

        :param key: Key for the table to query.
        :param q: Dict with the query as described in the QCache documentation
        :param accept: Response type, application/json and text/csv are supported
        :param post_query: If set the query will be executed using a POST rather than GET. Good for very large queries.
        :param query_headers: dict with additional headers to include when issuing query.
                              Key - header name
                              Value - header value
        :returns QueryResult: Contains the result of the query.
        :raises MalformedQueryException:
        :raises UnsupportedAcceptType:
        :raises UnexpectedServerResponse:
        :raises TooManyConsecutiveErrors:
        :raises NoCacheAvailable:
        """
        self._check_dropped_nodes()
        json_q = json.dumps(q)

        headers = {'Accept': accept}
        if query_headers:
            headers.update(query_headers)

        while True:
            node = self._node_for_key(key)
            key_url = self._key_url(node, key)
            with self._connection_error_manager(node):
                if post_query:
                    headers['Content-Type'] = 'application/json'
                    response = self.session.post(key_url + '/q', data=json_q, headers=headers)
                else:
                    response = self.session.get(key_url, params={'q': json_q}, headers=headers)

                if response.status_code == 200:
                    return QueryResult(response)

                if response.status_code == 404:
                    return None

                if response.status_code == 400:
                    raise MalformedQueryException('Malformed query "{json_q}", server response "{server_response}"'.format(
                        json_q=json_q, server_response=response.content))
                elif response.status_code == 406:
                    raise UnsupportedAcceptType('Accept type "{accept}" is not supported'.format(accept))
                else:
                    raise UnexpectedServerResponse('Unable to query dataset, status code {status_code}, content "{content}'.format(
                        status_code=response.status_code, content=response.content))

    def post(self, key, content, content_type='text/csv', post_headers=None):
        """
        Post table data to QCache for key.

        :param key: Key to store the table under
        :param content: Byte string with content encoded either as CSV or JSON.
        :param content_type: application/json or text/csv depending on uploaded content
        :param post_headers: dict with additional headers to include.
                             Key - header name
                             Value - header value
        :return: None
        :raises MalformedQueryException:
        :raises UnsupportedAcceptType:
        :raises UnexpectedServerResponse:
        :raises TooManyConsecutiveErrors:
        :raises NoCacheAvailable:
        """
        self._check_dropped_nodes()

        while True:
            node = self._node_for_key(key)
            key_url = self._key_url(node, key)
            headers = {'Content-type': content_type}
            if post_headers:
                headers.update(post_headers)

            with self._connection_error_manager(node):
                # Allow for a longer read timeout when posting data since it generally
                # takes longer than queries since there is more data to parse.
                timeout = (self.session.timeout[0], 10 * self.session.timeout[1])
                response = self.session.post(key_url, headers=headers, data=content, timeout=timeout)
                if response.status_code == 201:
                    return get_request_statistics(response, prefix="insert_")

                self.statistics[node]['unknown_error'] += 1
                raise UnexpectedServerResponse('Unable to create dataset, status code {status_code}, content "{content}"'.format(
                    status_code=response.status_code, content=response.content))

    def query(self, key, q, load_fn, load_fn_kwargs=None, content_type='text/csv', accept='application/json',
              post_headers=None, post_query=False, query_headers=None):
        """
        Convenience method to query for data. If the requested key is not available in the QCache a call will
        be made to :load_fn: providing :load_fn_kwargs: as key value args. :load_fn: should return the data to
        insert into QCache. Once the data has been pushed to QCache the query in executed again against the newly
        created table.

        :param key: Key for the table to query.
        :param q: Dict with the query as described in the QCache documentation
        :param load_fn: Function called to fetch data if not present in QCache.
        :param load_fn_kwargs: Key-value arguments to load_fn
        :param content_type: application/json or text/csv depending on uploaded content
        :param accept: Response type, application/json and text/csv are supported
        :param post_headers: dict with additional headers to include when pushing data to the caches.
                             Key - header name
                             Value - header value
        :param post_query: If set the query will be executed using a POST rather than GET. Good for very large queries.
        :param query_headers: dict with additional headers to include when issuing query.
                              Key - header name
                              Value - header value
        :return: QueryResult: Contains the result of the query.
        :raises MalformedQueryException:
        :raises UnsupportedAcceptType:
        :raises UnexpectedServerResponse:
        :raises TooManyConsecutiveErrors:
        :raises NoCacheAvailable:
        """
        content = None
        try_count = 0
        post_stats = {}
        while True:
            result = self.get(key, q, accept, post_query, query_headers)
            if result is not None:
                result.add_stats(post_stats)
                return result

            try_count += 1
            if try_count > self.consecutive_error_count_limit:
                raise TooManyConsecutiveErrors(
                    'Unable to query dataset after {try_count} tries, this is probably a sign of problems'.format(try_count=try_count))

            if content is None:
                kwargs = load_fn_kwargs or {}
                content = load_fn(**kwargs)

            post_stats = self.post(key, content, content_type=content_type, post_headers=post_headers)

    def delete(self, key):
        """
        Delete table stored under key from QCache.

        NOTE: If more than one QCache node is used there is no guarantee that the table is completely removed
              since it may be stored in multiple location depending events.

              Example:
              A small installation with two Qaches, qc1 and qc2. Table t1 is stored on qc1. qc1 disappears for unknown
              reason. t1 is then stored and read from t2 instead. Later t1 comes back. Data is now read from qc1
              instead. A delete would in this case be issued against qc1, after the delete qc2 would still hold a
              copy of t1.

        :param key: Key for the table to delete
        :raises UnexpectedServerResponse:
        :raises TooManyConsecutiveErrors:
        :raises NoCacheAvailable:
        :return: None
        """
        while True:
            node = self._node_for_key(key)
            key_url = self._key_url(node, key)
            with self._connection_error_manager(node):
                self.session.delete(key_url)
                return
