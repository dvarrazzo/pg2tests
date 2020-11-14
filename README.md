Psycopg2 tests to port to psycopg3
==================================

This repos contains a copy of [the test suite of
psycopg2](https://github.com/psycopg/psycopg2/tree/master/tests), where the
tests are being removed as they get [adapted to
psycopg3](https://github.com/psycopg/psycopg3/tree/master/tests).

The number of tests currently in this test suite is close to

    grep 'def test' *.py | wc -l

The goal of this repository is to disappear.
