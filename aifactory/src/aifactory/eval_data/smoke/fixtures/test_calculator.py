import unittest
from calculator import add


class Addition(unittest.TestCase):
    def test_add(self):
        for a, b in ((2, 3), (-3, 1), (0, 0), (17, -24)):
            self.assertEqual(add(a, b), a + b)
