#!/usr/bin/env python
# -*- coding: utf-8 -*-

# test_async.py - unit test for asynchronous API
#
# Copyright (C) 2010-2019 Jan Urbański  <wulczer@wulczer.org>
# Copyright (C) 2020 The Psycopg Team
#
# psycopg2 is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# In addition, as a special exception, the copyright holders give
# permission to link this program with the OpenSSL library (or with
# modified versions of OpenSSL that use the same license as OpenSSL),
# and distribute linked combinations including the two.
#
# You must obey the GNU Lesser General Public License in all respects for
# all of the code used other than OpenSSL.
#
# psycopg2 is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
# License for more details.

import time
import unittest

import psycopg2
import psycopg2.errors
from psycopg2 import extensions as ext

from .testutils import ConnectingTestCase, skip_before_postgres, slow


class PollableStub(object):
    """A 'pollable' wrapper allowing analysis of the `poll()` calls."""

    def __init__(self, pollable):
        self.pollable = pollable
        self.polls = []

    def fileno(self):
        return self.pollable.fileno()

    def poll(self):
        rv = self.pollable.poll()
        self.polls.append(rv)
        return rv


class AsyncTests(ConnectingTestCase):
    def setUp(self):
        ConnectingTestCase.setUp(self)

        self.sync_conn = self.conn
        self.conn = self.connect(async_=True)

        self.wait(self.conn)

        curs = self.conn.cursor()
        curs.execute(
            """
            CREATE TEMPORARY TABLE table1 (
              id int PRIMARY KEY
            )"""
        )
        self.wait(curs)

    def test_async_named_cursor(self):
        self.assertRaises(psycopg2.ProgrammingError, self.conn.cursor, "name")

    def test_set_parameters_while_async(self):
        cur = self.conn.cursor()

        cur.execute("select 'c'")
        self.assertTrue(self.conn.isexecuting())

        # getting transaction status works
        self.assertEquals(
            self.conn.info.transaction_status, ext.TRANSACTION_STATUS_ACTIVE
        )
        self.assertTrue(self.conn.isexecuting())

        # setting connection encoding should fail
        self.assertRaises(
            psycopg2.ProgrammingError, self.conn.set_client_encoding, "LATIN1"
        )

        # same for transaction isolation
        self.assertRaises(psycopg2.ProgrammingError, self.conn.set_isolation_level, 1)

    def test_reset_while_async(self):
        cur = self.conn.cursor()
        cur.execute("select 'c'")
        self.assertTrue(self.conn.isexecuting())

        # a reset should fail
        self.assertRaises(psycopg2.ProgrammingError, self.conn.reset)

    def test_lobject_while_async(self):
        # large objects should be prohibited
        self.assertRaises(psycopg2.ProgrammingError, self.conn.lobject)

    def test_async_scroll(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            insert into table1 values (1);
            insert into table1 values (2);
            insert into table1 values (3);
        """
        )
        self.wait(cur)
        cur.execute("select id from table1 order by id")

        # scroll should fail if a query is underway
        self.assertRaises(psycopg2.ProgrammingError, cur.scroll, 1)
        self.assertTrue(self.conn.isexecuting())

        # but after it's done it should work
        self.wait(cur)
        cur.scroll(1)
        self.assertEquals(cur.fetchall(), [(2,), (3,)])

        cur = self.conn.cursor()
        cur.execute("select id from table1 order by id")
        self.wait(cur)

        cur2 = self.conn.cursor()
        self.assertRaises(psycopg2.ProgrammingError, cur2.scroll, 1)

        self.assertRaises(psycopg2.ProgrammingError, cur.scroll, 4)

        cur = self.conn.cursor()
        cur.execute("select id from table1 order by id")
        self.wait(cur)
        cur.scroll(2)
        cur.scroll(-1)
        self.assertEquals(cur.fetchall(), [(2,), (3,)])

    def test_scroll(self):
        cur = self.sync_conn.cursor()
        cur.execute("create table table1 (id int)")
        cur.execute(
            """
            insert into table1 values (1);
            insert into table1 values (2);
            insert into table1 values (3);
        """
        )
        cur.execute("select id from table1 order by id")
        cur.scroll(2)
        cur.scroll(-1)
        self.assertEquals(cur.fetchall(), [(2,), (3,)])

    def test_async_dont_read_all(self):
        cur = self.conn.cursor()
        cur.execute("select repeat('a', 10000); select repeat('b', 10000)")

        # fetch the result
        self.wait(cur)

        # it should be the result of the second query
        self.assertEquals(cur.fetchone()[0], "b" * 10000)

    def test_async_subclass(self):
        class MyConn(ext.connection):
            def __init__(self, dsn, async_=0):
                ext.connection.__init__(self, dsn, async_=async_)

        conn = self.connect(connection_factory=MyConn, async_=True)
        self.assert_(isinstance(conn, MyConn))
        self.assert_(conn.async_)
        conn.close()

    def test_sync_poll(self):
        cur = self.sync_conn.cursor()
        cur.execute("select 1")
        # polling with a sync query works
        cur.connection.poll()
        self.assertEquals(cur.fetchone()[0], 1)

    @slow
    @skip_before_postgres(9, 0)
    def test_non_block_after_notification(self):
        from select import select

        cur = self.conn.cursor()
        cur.execute(
            """
            select 1;
            do $$
                begin
                    raise notice 'hello';
                end
            $$ language plpgsql;
            select pg_sleep(1);
            """
        )

        polls = 0
        while True:
            state = self.conn.poll()
            if state == psycopg2.extensions.POLL_OK:
                break
            elif state == psycopg2.extensions.POLL_READ:
                select([self.conn], [], [], 0.1)
            elif state == psycopg2.extensions.POLL_WRITE:
                select([], [self.conn], [], 0.1)
            else:
                raise Exception("Unexpected result from poll: %r", state)
            polls += 1

        self.assert_(polls >= 8, polls)

    def test_poll_noop(self):
        self.conn.poll()

    @skip_before_postgres(9, 0)
    def test_poll_conn_for_notification(self):
        with self.conn.cursor() as cur:
            cur.execute("listen test")
            self.wait(cur)

        with self.sync_conn.cursor() as cur:
            cur.execute("notify test, 'hello'")
            self.sync_conn.commit()

        for i in range(10):
            self.conn.poll()

            if self.conn.notifies:
                n = self.conn.notifies.pop()
                self.assertEqual(n.channel, "test")
                self.assertEqual(n.payload, "hello")
                break
            time.sleep(0.1)
        else:
            self.fail("No notification received")


def test_suite():
    return unittest.TestLoader().loadTestsFromName(__name__)


if __name__ == "__main__":
    unittest.main()
