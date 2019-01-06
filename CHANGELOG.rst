Changelog
=========

0.5.1 (2019-01-06)
------------------
* Include response content in cases of unexpected responses for easier debugging.
* Expose "trust_env" as a client constructor parameter to improve performance.

0.5.0 (2017-01-08)
------------------
* Support TLS client certificate verification

0.4.2 (2016-12-18)
------------------
* Check dropped nodes also on get, not only post.
* Make connection stats public and resettable.

0.4.1 (2016-11-11)
------------------
* Include content encoding in the result object.

0.4.0 (2016-09-18)
------------------
* Support for custom headers when running queries. This allows use of the pandas filter engine
  introduced in QCache 0.6.1.

0.3.2 (2016-04-10)
------------------
* Support Python 3.4 and 3.5.

0.3.1 (2016-01-16)
------------------
* Include CHANGELOG in release.

0.3.0 (2015-12-23)
------------------
* Possible to query using POST instead of GET. Good for very large queries.
* Additional circuit breakers to avoid infinite repetition of requests in case of errors.

0.2.1 (2015-12-14)
------------------
* SSL and basic auth support
* Possible to add custom headers when posting data, type information for example

0.2.0 (2015-12-06)
------------------
* Report the unsliced result length as part of the result, nice for pagination for example
* Use connection pooling

0.1.0 (2015-10-25)
------------------
* First release that actually does something sensible.

0.0.1 (2015-10-15)
------------------
* First release on PyPI.
