=============
QCache-client
=============

.. image:: https://travis-ci.org/tobgu/qcache-client.png?branch=master
    :target: https://travis-ci.org/tobgu/qcache-client

.. image:: https://badge.fury.io/py/qcache-client.svg
    :target: https://badge.fury.io/py/qcache-client

.. image:: http://codecov.io/github/tobgu/qcache-client/coverage.svg?branch=master
    :target: http://codecov.io/github/tobgu/qcache-client?branch=master


.. _QCache: https://github.com/tobgu/qcache

Python client library for QCache_. Uses consistent hashing to distribute data over multiple nodes.

Installation
============
::

    pip install qcache-client

Documentation
=============

Documentation is close to non-existent right now. Please see the tests in test_qclient.py for examples of how to use it.


Contributing
============
Want to contribute? That's great!

If you experience problems please log them on GitHub. If you want to contribute code,
please fork the code and submit a pull request.

If you intend to implement major features or make major changes please raise an issue
so that we can discuss it first.

Running tests
=============
.. code::

   pip install -r dev-requirements.txt
   invoke test

TODO
====
- Fetch server side statistics
- Documentation
- Support for other python versions than 2.7
- Support chunked transfer
