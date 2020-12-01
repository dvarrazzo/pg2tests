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
    slow,
    skip_if_no_superuser,
    skip_if_windows,
)

import psycopg2.extras


class CursorTests(ConnectingTestCase):
    def test_null_name(self):
        curs = self.conn.cursor(None)
        self.assertEqual(curs.name, None)

    def test_invalid_name(self):
        curs = self.conn.cursor()
        curs.execute("create temp table invname (data int);")
        for i in (10, 20, 30):
            curs.execute("insert into invname values (%s)", (i,))
        curs.close()

        curs = self.conn.cursor(r'1-2-3 \ "test"')
        curs.execute("select data from invname order by data")
        self.assertEqual(curs.fetchall(), [(10,), (20,), (30,)])

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

    def test_scrollable(self):
        self.assertRaises(psycopg2.ProgrammingError, self.conn.cursor, scrollable=True)

        curs = self.conn.cursor()
        curs.execute("create table scrollable (data int)")
        curs.executemany(
            "insert into scrollable values (%s)", [(i,) for i in range(100)]
        )
        curs.close()

        for t in range(2):
            if not t:
                curs = self.conn.cursor("S")
                self.assertEqual(curs.scrollable, None)
                curs.scrollable = True
            else:
                curs = self.conn.cursor("S", scrollable=True)

            self.assertEqual(curs.scrollable, True)
            curs.itersize = 10

            # complex enough to make postgres cursors declare without
            # scroll/no scroll to fail
            curs.execute(
                """
                select x.data
                from scrollable x
                join scrollable y on x.data = y.data
                order by y.data"""
            )
            for i, (n,) in enumerate(curs):
                self.assertEqual(i, n)

            curs.scroll(-1)
            for i in range(99, -1, -1):
                curs.scroll(-1)
                self.assertEqual(i, curs.fetchone()[0])
                curs.scroll(-1)

            curs.close()

    def test_not_scrollable(self):
        self.assertRaises(psycopg2.ProgrammingError, self.conn.cursor, scrollable=False)

        curs = self.conn.cursor()
        curs.execute("create table scrollable (data int)")
        curs.executemany(
            "insert into scrollable values (%s)", [(i,) for i in range(100)]
        )
        curs.close()

        curs = self.conn.cursor("S")  # default scrollability
        curs.execute("select * from scrollable")
        self.assertEqual(curs.scrollable, None)
        curs.scroll(2)
        try:
            curs.scroll(-1)
        except psycopg2.OperationalError:
            return self.skipTest("can't evaluate non-scrollable cursor")
        curs.close()

        curs = self.conn.cursor("S", scrollable=False)
        self.assertEqual(curs.scrollable, False)
        curs.execute("select * from scrollable")
        curs.scroll(2)
        self.assertRaises(psycopg2.OperationalError, curs.scroll, -1)

    @slow
    @skip_before_postgres(8, 2)
    def test_iter_named_cursor_efficient(self):
        curs = self.conn.cursor("tmp")
        # if these records are fetched in the same roundtrip their
        # timestamp will not be influenced by the pause in Python world.
        curs.execute("""select clock_timestamp() from generate_series(1,2)""")
        i = iter(curs)
        t1 = next(i)[0]
        time.sleep(0.2)
        t2 = next(i)[0]
        self.assert_(
            (t2 - t1).microseconds * 1e-6 < 0.1,
            "named cursor records fetched in 2 roundtrips (delta: %s)" % (t2 - t1),
        )

    @skip_before_postgres(8, 0)
    def test_iter_named_cursor_default_itersize(self):
        curs = self.conn.cursor("tmp")
        curs.execute("select generate_series(1,50)")
        rv = [(r[0], curs.rownumber) for r in curs]
        # everything swallowed in one gulp
        self.assertEqual(rv, [(i, i) for i in range(1, 51)])

    @skip_before_postgres(8, 0)
    def test_iter_named_cursor_itersize(self):
        curs = self.conn.cursor("tmp")
        curs.itersize = 30
        curs.execute("select generate_series(1,50)")
        rv = [(r[0], curs.rownumber) for r in curs]
        # everything swallowed in two gulps
        self.assertEqual(rv, [(i, ((i - 1) % 30) + 1) for i in range(1, 51)])

    @skip_before_postgres(8, 0)
    def test_iter_named_cursor_rownumber(self):
        curs = self.conn.cursor("tmp")
        # note: this fails if itersize < dataset: internally we check
        # rownumber == rowcount to detect when to read anoter page, so we
        # would need an extra attribute to have a monotonic rownumber.
        curs.itersize = 20
        curs.execute("select generate_series(1,10)")
        for i, rec in enumerate(curs):
            self.assertEqual(i + 1, curs.rownumber)

    @skip_before_postgres(8, 0)
    def test_named_cursor_stealing(self):
        # you can use a named cursor to iterate on a refcursor created
        # somewhere else
        cur1 = self.conn.cursor()
        cur1.execute(
            "DECLARE test CURSOR WITHOUT HOLD " " FOR SELECT generate_series(1,7)"
        )

        cur2 = self.conn.cursor("test")
        # can call fetch without execute
        self.assertEqual((1,), cur2.fetchone())
        self.assertEqual([(2,), (3,), (4,)], cur2.fetchmany(3))
        self.assertEqual([(5,), (6,), (7,)], cur2.fetchall())

    @skip_before_postgres(8, 2)
    def test_named_noop_close(self):
        cur = self.conn.cursor("test")
        cur.close()

    @skip_before_postgres(8, 2)
    def test_stolen_named_cursor_close(self):
        cur1 = self.conn.cursor()
        cur1.execute(
            "DECLARE test CURSOR WITHOUT HOLD " " FOR SELECT generate_series(1,7)"
        )
        cur2 = self.conn.cursor("test")
        cur2.close()

        cur1.execute(
            "DECLARE test CURSOR WITHOUT HOLD " " FOR SELECT generate_series(1,7)"
        )
        cur2 = self.conn.cursor("test")
        cur2.close()

    @skip_before_postgres(8, 0)
    def test_scroll(self):
        cur = self.conn.cursor()
        cur.execute("select generate_series(0,9)")
        cur.scroll(2)
        self.assertEqual(cur.fetchone(), (2,))
        cur.scroll(2)
        self.assertEqual(cur.fetchone(), (5,))
        cur.scroll(2, mode="relative")
        self.assertEqual(cur.fetchone(), (8,))
        cur.scroll(-1)
        self.assertEqual(cur.fetchone(), (8,))
        cur.scroll(-2)
        self.assertEqual(cur.fetchone(), (7,))
        cur.scroll(2, mode="absolute")
        self.assertEqual(cur.fetchone(), (2,))

        # on the boundary
        cur.scroll(0, mode="absolute")
        self.assertEqual(cur.fetchone(), (0,))
        self.assertRaises(
            (IndexError, psycopg2.ProgrammingError),
            cur.scroll,
            -1,
            mode="absolute",
        )
        cur.scroll(0, mode="absolute")
        self.assertRaises((IndexError, psycopg2.ProgrammingError), cur.scroll, -1)

        cur.scroll(9, mode="absolute")
        self.assertEqual(cur.fetchone(), (9,))
        self.assertRaises(
            (IndexError, psycopg2.ProgrammingError),
            cur.scroll,
            10,
            mode="absolute",
        )
        cur.scroll(9, mode="absolute")
        self.assertRaises((IndexError, psycopg2.ProgrammingError), cur.scroll, 1)

    @skip_before_postgres(8, 0)
    def test_scroll_named(self):
        cur = self.conn.cursor("tmp", scrollable=True)
        cur.execute("select generate_series(0,9)")
        cur.scroll(2)
        self.assertEqual(cur.fetchone(), (2,))
        cur.scroll(2)
        self.assertEqual(cur.fetchone(), (5,))
        cur.scroll(2, mode="relative")
        self.assertEqual(cur.fetchone(), (8,))
        cur.scroll(9, mode="absolute")
        self.assertEqual(cur.fetchone(), (9,))

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
