"""Tests for snapshot line parsing (no TCP)."""

import unittest

from exchange1.snapshot_client import parse_snapshot_text


class TestParseSnapshotText(unittest.TestCase):
    def test_empty_book(self):
        text = "SNAPSHOT_SEQ=42\nEND_SNAPSHOT\n"
        out = parse_snapshot_text(text)
        self.assertEqual(out["snapshot_seq"], 42)
        self.assertEqual(out["symbols"], {})

    def test_bids_asks_sorting(self):
        text = (
            "SNAPSHOT_SEQ=1\n"
            "2|100|A|101|AAPL|EQ|B|10|1500000\n"
            "3|101|A|102|AAPL|EQ|B|5|1502500\n"
            "4|102|A|103|AAPL|EQ|S|8|1502600\n"
            "5|103|A|104|AAPL|EQ|S|3|1502400\n"
            "END_SNAPSHOT\n"
        )
        out = parse_snapshot_text(text)
        self.assertEqual(out["snapshot_seq"], 1)
        book = out["symbols"]["AAPL"]
        self.assertEqual(len(book["bids"]), 2)
        self.assertEqual(len(book["asks"]), 2)
        # bids: descending price
        self.assertEqual(book["bids"][0]["price"], 150.25)
        self.assertEqual(book["bids"][1]["price"], 150.0)
        # asks: ascending price
        self.assertEqual(book["asks"][0]["price"], 150.24)
        self.assertEqual(book["asks"][1]["price"], 150.26)


if __name__ == "__main__":
    unittest.main()
