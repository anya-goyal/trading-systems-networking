"""Tests for exchange1.fix (FIX 4.2 SOH framing and checksum)."""

import unittest
from copy import copy

from exchange1.fix import SOH, build_fix, compute_checksum, parse_fix_message, try_pop_message


class TestChecksum(unittest.TestCase):
    def test_known_checksum(self):
        data = b"8=FIX.4.2\x0135=D\x01"
        self.assertEqual(compute_checksum(data), sum(data) % 256)


class TestBuildAndPop(unittest.TestCase):
    def test_roundtrip(self):
        fields = [
            ("8", "FIX.4.2"),
            ("35", "D"),
            ("49", "IOG"),
            ("56", "EXCH1"),
            ("11", "O1"),
            ("55", "AAPL"),
            ("54", "1"),
            ("40", "2"),
            ("38", "10"),
            ("44", "150.25"),
        ]
        msg = build_fix(fields)
        self.assertTrue(msg.endswith(SOH))
        buf = bytearray(msg)
        parsed = try_pop_message(buf)
        self.assertIsNotNone(parsed)
        self.assertEqual(len(buf), 0)
        d = parse_fix_message(parsed)
        self.assertEqual(d["35"], "D")
        self.assertEqual(d["11"], "O1")
        self.assertEqual(d["44"], "150.25")

    def test_two_messages_back_to_back(self):
        m1 = build_fix([("8", "FIX.4.2"), ("35", "0"), ("49", "A"), ("56", "B")])
        m2 = build_fix([("8", "FIX.4.2"), ("35", "1"), ("49", "C"), ("56", "D")])
        buf = bytearray(m1 + m2)
        a = try_pop_message(buf)
        b = try_pop_message(buf)
        self.assertIsNotNone(a)
        self.assertIsNotNone(b)
        self.assertEqual(len(buf), 0)
        self.assertEqual(parse_fix_message(a)["35"], "0")
        self.assertEqual(parse_fix_message(b)["35"], "1")

    def test_bad_checksum_not_consumed(self):
        msg = build_fix([("8", "FIX.4.2"), ("35", "D"), ("49", "IOG"), ("56", "EXCH1")])
        corrupted = bytearray(msg)
        idx = corrupted.rfind(b"10=")
        self.assertNotEqual(idx, -1)
        corrupted[idx + 5] = ord(b"9") if corrupted[idx + 5] != ord(b"9") else ord(b"0")
        buf = bytearray(corrupted)
        before = copy(buf)
        out = try_pop_message(buf)
        self.assertIsNone(out)
        self.assertEqual(buf, before)


if __name__ == "__main__":
    unittest.main()
