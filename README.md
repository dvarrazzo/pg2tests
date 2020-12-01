Psycopg2 tests to port to psycopg3
==================================

This repos contains a copy of [the test suite of psycopg2][test2], where the tests
are being removed as they get [adapted to psycopg3][test3].

The number of tests currently in this test suite is close to:

    grep 'def test' *.py | wc -l

Visual progress is tracked [in a spreadsheet][sheet] online.

![tests to go](https://docs.google.com/spreadsheets/d/e/2PACX-1vSIjr8UbGoF1BE9fOJkanoUWe7-aDjm0HvpdxJDA0PEddpU0bGVtQkSSjtdEr2MRzAFawNXBfgn5MyK/pubchart?oid=889461505&amp;format=image "tests to go")

[test2]: https://github.com/psycopg/psycopg2/tree/master/tests
[test3]: https://github.com/psycopg/psycopg3/tree/master/tests
[sheet]: https://docs.google.com/spreadsheets/d/1Jh8XdAJyoVgyl3AiSRqJhEw1Gv1U1CUR3dBTuai3oks/edit?usp=sharing


Project goals
-------------

The goal of this repository is to disappear. 👋
