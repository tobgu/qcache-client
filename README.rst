=============
QCache-client
=============

.. _QCache: https://github.com/tobgu/qcache

Python client library for QCache_. Uses consistent hashing to distribute data over multiple nodes.

Installation
============
::

    pip install qcache-client

Documentation
=============

Documentation is close to non-existent right now. Please see the tests in test_qclient.py for examples of how to use it.

Development
===========

To run the all tests run::

    invoke test


TODO
====
- Fetch server side statistics
- Documentation
- Support for other python versions than 2.7
- Support chunked transfer
- Support TLS, also with self signed certificates