#! /usr/bin/env python

import unittest
import sql

class SQLTestCase(unittest.TestCase):
    def test_second(self):
        print sql.duration(0) == ''

    def test_seconds(self):
        assert sql.duration(42) == '42 s'

    def test_minute(self):
        assert sql.duration(60) == '01 min'

    def test_minutes(self):
        assert sql.duration(61) == '01 min 01 s'

    def test_hour(self):
        assert sql.duration(60 * 60) == '1 h 00 min'

    def test_hours(self):
        assert sql.duration(60 * 60 + 1) == '1 h 00 min 01 s'

if __name__ == '__main__':
    unittest.main()
