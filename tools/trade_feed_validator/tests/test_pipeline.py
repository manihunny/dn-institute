import contextlib
import csv
import io
import tempfile
import unittest
from pathlib import Path

from pipeline import main

SAMPLE = Path(__file__).resolve().parent.parent / "sample_feed.csv"


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


class PipelineTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def run_main(self, *args):
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = main([*map(str, args), "--out-dir", str(self.out)])
        return code, stdout.getvalue(), stderr.getvalue()

    def test_sample_feed_end_to_end(self):
        code, stdout, _ = self.run_main(SAMPLE, "--feed-date", "2026-01-01")
        self.assertEqual(code, 0)
        self.assertIn("naive 705000 -> clean 375000", stdout)

        clean = read_csv(self.out / "clean_trades.csv")
        self.assertEqual([r["event_id"] for r in clean], ["evt_001", "evt_002", "evt_004", "evt_006"])
        self.assertEqual(clean[0]["block_time"], "2026-01-01T09:14:02+00:00")

        quarantine = {r["event_id"]: r for r in read_csv(self.out / "quarantine.csv")}
        self.assertEqual(
            {k: (v["disposition"], v["reason"]) for k, v in quarantine.items()},
            {
                "evt_003": ("duplicate", "duplicate_trade"),
                "evt_005": ("dead_letter", "missing_field"),
                "evt_007": ("duplicate", "duplicate_trade"),
                "evt_008": ("dead_letter", "ingested_before_block_time"),
            },
        )
        # the raw record is kept verbatim so a repair job can replay it
        self.assertEqual(quarantine["evt_005"]["raw_block_time"], "null")
        self.assertEqual(quarantine["evt_005"]["raw_amount"], "30000")

    def test_quality_gate_fails_when_dead_letter_rate_is_too_high(self):
        code, _, stderr = self.run_main(SAMPLE, "--feed-date", "2026-01-01", "--max-dead-letter-rate", "0.1")
        self.assertEqual(code, 2)
        self.assertIn("quality gate failed", stderr)
        self.assertFalse((self.out / "clean_trades.csv").exists())
        self.assertTrue((self.out / "quarantine.csv").exists())

    def test_failed_quality_gate_removes_stale_clean_output(self):
        self.assertEqual(self.run_main(SAMPLE, "--feed-date", "2026-01-01")[0], 0)
        self.assertTrue((self.out / "clean_trades.csv").exists())
        code, _, _ = self.run_main(SAMPLE, "--feed-date", "2026-01-01", "--max-dead-letter-rate", "0.1")
        self.assertEqual(code, 2)
        self.assertFalse((self.out / "clean_trades.csv").exists())

    def test_invalid_thresholds_are_rejected(self):
        options = ("--dedup-horizon-hours", "--clock-skew-seconds", "--max-lag-seconds", "--allowed-lateness-seconds")
        for option in options:
            for value in ("-1", "nan", "inf"):
                with self.subTest(option=option, value=value), self.assertRaises(SystemExit):
                    self.run_main(SAMPLE, "--feed-date", "2026-01-01", option, value)

    def test_failed_run_removes_stale_outputs(self):
        self.assertEqual(self.run_main(SAMPLE, "--feed-date", "2026-01-01")[0], 0)
        broken = self.out / "broken.csv"
        broken.write_text("event_id,tx_hash,amount\nevt_1,0xaa1,10\n", encoding="utf-8")
        self.assertEqual(self.run_main(broken, "--feed-date", "2026-01-01")[0], 1)
        self.assertFalse((self.out / "clean_trades.csv").exists())
        self.assertFalse((self.out / "quarantine.csv").exists())

    def test_invalid_utf8_fails_with_exit_code_1(self):
        feed = self.out / "latin1.csv"
        feed.write_bytes(SAMPLE.read_bytes().replace("…".encode("utf-8"), b"\xe9"))
        code, _, stderr = self.run_main(feed, "--feed-date", "2026-01-01")
        self.assertEqual(code, 1)
        self.assertIn("error:", stderr)

    def test_quality_gate_rejects_invalid_threshold(self):
        for value in ("1.5", "-0.1", "nan"):
            with self.subTest(value=value), self.assertRaises(SystemExit):
                self.run_main(SAMPLE, "--feed-date", "2026-01-01", "--max-dead-letter-rate", value)

    def test_quality_gate_passes_under_threshold(self):
        code, _, _ = self.run_main(SAMPLE, "--feed-date", "2026-01-01", "--max-dead-letter-rate", "0.25")
        self.assertEqual(code, 0)

    def test_quarantine_keeps_every_raw_field_for_replay(self):
        feed = self.out / "feed.csv"
        feed.write_text(
            "event_id,tx_hash,block_time,wallet,side,amount,ingested_at,venue\n"
            "evt_1,0xaa1,09:00:00,0xd4,BUY,10,09:00:01,dex\n"
            "evt_2,0xaa2,09:00:05,0xd4,BUY,10,09:00:06,dex,a|b,c\n",
            encoding="utf-8",
        )
        code, _, _ = self.run_main(feed, "--feed-date", "2026-01-01")
        self.assertEqual(code, 0)
        [row] = read_csv(self.out / "quarantine.csv")
        self.assertEqual((row["event_id"], row["reason"]), ("evt_2", "malformed_row"))
        self.assertEqual((row["raw_venue"], row["raw_extra"]), ("dex", '["a|b", "c"]'))

    def test_extreme_amount_does_not_crash_reporting(self):
        feed = self.out / "huge.csv"
        feed.write_text(SAMPLE.read_text(encoding="utf-8") + "evt_009,0xaa7,10:20:00,0xF6,BUY,1e999999999,10:20:02\n",
                        encoding="utf-8")
        code, stdout, _ = self.run_main(feed, "--feed-date", "2026-01-01")
        self.assertEqual(code, 0)
        self.assertIn("naive 705000", stdout)
        reasons = {r["event_id"]: r["reason"] for r in read_csv(self.out / "quarantine.csv")}
        self.assertEqual(reasons["evt_009"], "invalid_amount")

    def test_broken_schema_fails_fast(self):
        broken = self.out / "broken.csv"
        broken.write_text("event_id,tx_hash,amount\nevt_1,0xaa1,10\n", encoding="utf-8")
        code, _, stderr = self.run_main(broken, "--feed-date", "2026-01-01")
        self.assertEqual(code, 1)
        self.assertIn("missing required columns", stderr)
        self.assertFalse((self.out / "clean_trades.csv").exists())


if __name__ == "__main__":
    unittest.main()
