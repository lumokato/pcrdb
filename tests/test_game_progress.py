import os
from unittest import TestCase
from unittest.mock import patch

from pcrdb.game_progress import (
    exp_to_knight_level,
    get_talent_quest_total,
    load_game_progress,
)


class GameProgressTests(TestCase):
    def test_loads_current_master_limits(self):
        progress = load_game_progress()

        self.assertEqual(progress.master_version, 202607231835)
        self.assertEqual(len(progress.knight_rank_total_exp), 521)
        self.assertEqual(progress.knight_rank_total_exp[-1], 53769077)
        self.assertEqual(progress.talent_quest_counts, (80, 80, 80, 80, 80))

    def test_converts_exp_at_rank_boundaries(self):
        cases = {
            None: "0",
            0: "0",
            1: "1",
            53235: "2",
            10647076: "201",
            10758152: "202",
            40844290: "428",
            53769077: "521",
            53769078: "521+",
        }

        for total_exp, expected in cases.items():
            with self.subTest(total_exp=total_exp):
                self.assertEqual(exp_to_knight_level(total_exp), expected)

    def test_uses_master_talent_total_by_default(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(get_talent_quest_total(), 400)

    def test_allows_positive_talent_total_override(self):
        with patch.dict(os.environ, {"TALENT_QUEST_TOTAL": "405"}, clear=True):
            self.assertEqual(get_talent_quest_total(), 405)

    def test_rejects_invalid_talent_total_override(self):
        for value in ("", "0", "invalid"):
            with self.subTest(value=value):
                with patch.dict(os.environ, {"TALENT_QUEST_TOTAL": value}, clear=True):
                    with self.assertRaisesRegex(ValueError, "positive integer"):
                        get_talent_quest_total()
