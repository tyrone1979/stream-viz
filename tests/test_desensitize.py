"""The desensitisation scan must fire on identifiers and not on measurements."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))

from desensitize_check import find_term, is_json_number_value  # noqa: E402


class NumberLiteralTests(unittest.TestCase):
    def test_measurement_in_value_position_is_not_an_identifier(self):
        for line in ('        "min": 4711.0,',
                     '        "frame_dropped": 4711,',
                     '    "sent": [4711]',
                     '    "sent": 4711}'):
            self.assertIsNone(find_term(line, "4711"),
                              msg=f"false positive on {line!r}")

    def test_identifier_contexts_still_fire(self):
        for line in ('# built at 4711所 in 2019',
                     '"owner": "4711所"',
                     'value 4711 attached to the hull',
                     '7204711 is a different number but 4711 is in it',
                     '总装4711单位'):
            self.assertIsNotNone(find_term(line, "4711"),
                                 msg=f"missed identifier in {line!r}")

    def test_no_6_is_not_a_json_value(self):
        # A bare number in prose has no JSON value separator in front of it.
        self.assertFalse(is_json_number_value("we measured 4711 samples", 13, 16))
        self.assertTrue(is_json_number_value('  "n": 4711,', 7, 10))


class FindTermTests(unittest.TestCase):
    def test_first_non_numeric_occurrence_is_reported(self):
        line = '  "min": 4711.0, "label": "4711所"'
        at = find_term(line, "4711")
        self.assertIsNotNone(at)
        self.assertIn("4711所", line[at:])

    def test_textual_terms_are_untouched_by_the_numeric_rule(self):
        self.assertIsNotNone(find_term("the 47110 Hz channel", "47110"))
        self.assertIsNone(find_term("nothing here", "4711"))


if __name__ == "__main__":
    unittest.main()
