#!/usr/bin/env python

# test_cursor.py - unit test for cursor attributes
#
# Copyright (C) 2010-2019 Daniele Varrazzo  <daniele.varrazzo@gmail.com>
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
import psycopg2
import psycopg2.extensions
import unittest
from .testutils import (
    ConnectingTestCase,
    skip_before_postgres,
    skip_if_no_superuser,
    skip_if_windows,
)

import psycopg2.extras


class CursorTests(ConnectingTestCase):
    def _create_withhold_table(self):
        curs = self.conn.cursor()
        try:
            curs.execute("drop table withhold")
        except psycopg2.ProgrammingError:
            self.conn.rollback()
        curs.execute("create table withhold (data int)")
        for i in (10, 20, 30):
            curs.execute("insert into withhold values (%s)", (i,))
        curs.close()

    def test_withhold(self):
        self.assertRaises(psycopg2.ProgrammingError, self.conn.cursor, withhold=True)

        self._create_withhold_table()
        curs = self.conn.cursor("W")
        self.assertEqual(curs.withhold, False)
        curs.withhold = True
        self.assertEqual(curs.withhold, True)
        curs.execute("select data from withhold order by data")
        self.conn.commit()
        self.assertEqual(curs.fetchall(), [(10,), (20,), (30,)])
        curs.close()

        curs = self.conn.cursor("W", withhold=True)
        self.assertEqual(curs.withhold, True)
        curs.execute("select data from withhold order by data")
        self.conn.commit()
        self.assertEqual(curs.fetchall(), [(10,), (20,), (30,)])

        curs = self.conn.cursor()
        curs.execute("drop table withhold")
        self.conn.commit()

    def test_withhold_no_begin(self):
        self._create_withhold_table()
        curs = self.conn.cursor("w", withhold=True)
        curs.execute("select data from withhold order by data")
        self.assertEqual(curs.fetchone(), (10,))
        self.assertEqual(self.conn.status, psycopg2.extensions.STATUS_BEGIN)
        self.assertEqual(
            self.conn.info.transaction_status,
            psycopg2.extensions.TRANSACTION_STATUS_INTRANS,
        )

        self.conn.commit()
        self.assertEqual(self.conn.status, psycopg2.extensions.STATUS_READY)
        self.assertEqual(
            self.conn.info.transaction_status,
            psycopg2.extensions.TRANSACTION_STATUS_IDLE,
        )

        self.assertEqual(curs.fetchone(), (20,))
        self.assertEqual(self.conn.status, psycopg2.extensions.STATUS_READY)
        self.assertEqual(
            self.conn.info.transaction_status,
            psycopg2.extensions.TRANSACTION_STATUS_IDLE,
        )

        curs.close()
        self.assertEqual(self.conn.status, psycopg2.extensions.STATUS_READY)
        self.assertEqual(
            self.conn.info.transaction_status,
            psycopg2.extensions.TRANSACTION_STATUS_IDLE,
        )

    def test_withhold_autocommit(self):
        self._create_withhold_table()
        self.conn.commit()
        self.conn.autocommit = True
        curs = self.conn.cursor("w", withhold=True)
        curs.execute("select data from withhold order by data")

        self.assertEqual(curs.fetchone(), (10,))
        self.assertEqual(self.conn.status, psycopg2.extensions.STATUS_READY)
        self.assertEqual(
            self.conn.info.transaction_status,
            psycopg2.extensions.TRANSACTION_STATUS_IDLE,
        )

        self.conn.commit()
        self.assertEqual(self.conn.status, psycopg2.extensions.STATUS_READY)
        self.assertEqual(
            self.conn.info.transaction_status,
            psycopg2.extensions.TRANSACTION_STATUS_IDLE,
        )

        curs.close()
        self.assertEqual(self.conn.status, psycopg2.extensions.STATUS_READY)
        self.assertEqual(
            self.conn.info.transaction_status,
            psycopg2.extensions.TRANSACTION_STATUS_IDLE,
        )

    @skip_if_no_superuser
    @skip_if_windows
    @skip_before_postgres(8, 4)
    def test_external_close_sync(self):
        # If a "victim" connection is closed by a "control" connection
        # behind psycopg2's back, psycopg2 always handles it correctly:
        # raise OperationalError, set conn.closed to 2. This reproduces
        # issue #443, a race between control_conn closing victim_conn and
        # psycopg2 noticing.
        control_conn = self.conn
        connect_func = self.connect

        def wait_func(conn):
            pass

        self._test_external_close(control_conn, connect_func, wait_func)

    @skip_if_no_superuser
    @skip_if_windows
    @skip_before_postgres(8, 4)
    def test_external_close_async(self):
        # Issue #443 is in the async code too. Since the fix is duplicated,
        # so is the test.
        control_conn = self.conn

        def connect_func():
            return self.connect(async_=True)

        wait_func = psycopg2.extras.wait_select
        self._test_external_close(control_conn, connect_func, wait_func)

    def _test_external_close(self, control_conn, connect_func, wait_func):
        # The short sleep before using victim_conn the second time makes it
        # much more likely to lose the race and see the bug. Repeating the
        # test several times makes it even more likely.
        for i in range(10):
            victim_conn = connect_func()
            wait_func(victim_conn)

            with victim_conn.cursor() as cur:
                cur.execute("select pg_backend_pid()")
                wait_func(victim_conn)
                pid1 = cur.fetchall()[0][0]

            with control_conn.cursor() as cur:
                cur.execute("select pg_terminate_backend(%s)", (pid1,))

            time.sleep(0.001)

            def f():
                with victim_conn.cursor() as cur:
                    cur.execute("select 1")
                    wait_func(victim_conn)

            self.assertRaises(psycopg2.OperationalError, f)

            self.assertEqual(victim_conn.closed, 2)


def test_suite():
    return unittest.TestLoader().loadTestsFromName(__name__)


if __name__ == "__main__":
    unittest.main()
