import unittest
from signal_control.http_api import util


class MyTestCase(unittest.TestCase):
    def test_something(self):
        print(util.gen_uuid())
        self.assertTrue(True)
