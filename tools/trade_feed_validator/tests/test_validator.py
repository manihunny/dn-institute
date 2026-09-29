import csv
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from validator import (
    Config,
    Disposition,
    Flag,
    Reason,
    SchemaError,
    check_columns,
    validate,
)

SAMPLE = Path(__file__).resolve().parent.parent / "sample_feed.csv"
FEED_DATE = date(2026, 1, 1)
CONFIG = Config(feed_date=FEED_DATE)

WALLET = "0x" + "d4" * 20
TX_A = "0x" + "a1" * 32
TX_B = "0x" + "b2" * 32


def record(**overrides):
    base = {
        "event_id": "evt_1",
        "tx_hash": TX_A,
        "block_time": "09:00:00",
        "wallet": WALLET,
        "side": "BUY",
        "amount": "100",
        "ingested_at": "09:00:03",
    }
    base.update(overrides)
    return base


def outcomes_by_event(report):
    return {o.event_id: o for o in report.outcomes}


def load_sample():
    with SAMPLE.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


class SampleFeedTest(unittest.TestCase):
    """Every issue in the challenge sample is caught and handled the documented way."""

    @classmethod
    def setUpClass(cls):
        cls.report = validate(load_sample(), CONFIG)
        cls.by_event = outcomes_by_event(cls.report)

    def assertOutcome(self, event_id, disposition, reason=None):
        outcome = self.by_event[event_id]
        self.assertIs(outcome.disposition, disposition, outcome.detail)
        self.assertEqual(outcome.reason, reason)

    def test_redelivery_under_new_event_id_is_dropped(self):
        # rows 2-3: same tx 0xaa2, second copy arrives 2m38s later as evt_003
        self.assertOutcome("evt_002", Disposition.ACCEPTED)
        self.assertOutcome("evt_003", Disposition.DUPLICATE, Reason.DUPLICATE_TRADE)
        self.assertIn("evt_002", self.by_event["evt_003"].detail)

    def test_double_emission_in_same_second_is_dropped(self):
        # rows 6-7: same tx 0xaa5, identical ingested_at, different event_id
        self.assertOutcome("evt_006", Disposition.ACCEPTED)
        self.assertOutcome("evt_007", Disposition.DUPLICATE, Reason.DUPLICATE_TRADE)

    def test_missing_block_time_is_dead_lettered(self):
        # row 5: evt_005 has block_time = null
        self.assertOutcome("evt_005", Disposition.DEAD_LETTER, Reason.MISSING_FIELD)
        self.assertIn("block_time", self.by_event["evt_005"].detail)

    def test_ingested_before_block_time_is_dead_lettered(self):
        # row 8: ingested 09:59:50 for a trade in a block at 10:10:00
        self.assertOutcome("evt_008", Disposition.DEAD_LETTER, Reason.INGESTED_BEFORE_BLOCK)

    def test_clean_rows_pass_without_flags(self):
        for event_id in ("evt_001", "evt_002", "evt_004", "evt_006"):
            self.assertOutcome(event_id, Disposition.ACCEPTED)
            self.assertEqual(self.by_event[event_id].flags, ())

    def test_every_row_has_exactly_one_outcome(self):
        self.assertEqual(len(self.report.outcomes), 8)
        self.assertEqual(
            [o.disposition.value for o in self.report.outcomes],
            ["accepted", "accepted", "duplicate", "accepted", "dead_letter", "accepted", "duplicate", "dead_letter"],
        )

    def test_clean_volume_and_wallet_activity(self):
        trades = self.report.trades
        self.assertEqual([t.event_id for t in trades], ["evt_001", "evt_002", "evt_004", "evt_006"])
        self.assertEqual(sum(t.amount for t in trades), Decimal(375000))
        per_wallet = {}
        for trade in trades:
            per_wallet[trade.wallet] = per_wallet.get(trade.wallet, 0) + 1
        self.assertEqual(per_wallet, {"0xd4…": 2, "0xe5…": 1, "0xf6…": 1})


class RecordChecksTest(unittest.TestCase):
    def single(self, config=CONFIG, **overrides):
        return validate([record(**overrides)], config).outcomes[0]

    def assertDeadLetter(self, outcome, reason):
        self.assertIs(outcome.disposition, Disposition.DEAD_LETTER, outcome.detail)
        self.assertIs(outcome.reason, reason)

    def test_valid_record_is_accepted_and_normalised(self):
        outcome = self.single(side=" buy ", wallet=WALLET.upper().replace("0X", "0x"), amount="1.5e2")
        self.assertIs(outcome.disposition, Disposition.ACCEPTED)
        self.assertEqual(outcome.trade.side, "BUY")
        self.assertEqual(outcome.trade.wallet, WALLET)
        self.assertEqual(outcome.trade.amount, Decimal(150))
        self.assertEqual(outcome.trade.block_time, datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc))

    def test_null_tokens_are_missing(self):
        for token in ("", "  ", "null", "NULL", "None", "NaN", "n/a"):
            with self.subTest(token=token):
                self.assertDeadLetter(self.single(block_time=token), Reason.MISSING_FIELD)

    def test_absent_value_is_missing(self):
        raw = record()
        raw["wallet"] = None  # csv.DictReader yields None for short rows
        self.assertDeadLetter(validate([raw], CONFIG).outcomes[0], Reason.MISSING_FIELD)

    def test_invalid_amounts(self):
        for amount in ("abc", "0", "-5", "Infinity", "sNaN", "1,000"):
            with self.subTest(amount=amount):
                self.assertDeadLetter(self.single(amount=amount), Reason.INVALID_AMOUNT)

    def test_invalid_side(self):
        for side in ("HOLD", "B", "long"):
            with self.subTest(side=side):
                self.assertDeadLetter(self.single(side=side), Reason.INVALID_SIDE)

    def test_invalid_timestamps(self):
        for value in ("25:00:00", "yesterday", "2026-13-01T00:00:00"):
            with self.subTest(value=value):
                self.assertDeadLetter(self.single(block_time=value), Reason.INVALID_TIMESTAMP)

    def test_time_only_timestamp_needs_feed_date(self):
        self.assertDeadLetter(self.single(config=Config()), Reason.INVALID_TIMESTAMP)

    def test_iso_timestamps_with_offsets_are_normalised_to_utc(self):
        outcome = self.single(config=Config(), block_time="2026-01-01T11:00:00+02:00",
                              ingested_at="2026-01-01T09:00:02Z")
        self.assertIs(outcome.disposition, Disposition.ACCEPTED, outcome.detail)
        self.assertEqual(outcome.trade.block_time, datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc))

    def test_clock_skew_tolerance_boundary(self):
        config = Config(feed_date=FEED_DATE, clock_skew_tolerance=timedelta(seconds=2))
        self.assertIs(self.single(config, ingested_at="08:59:58").disposition, Disposition.ACCEPTED)
        self.assertDeadLetter(self.single(config, ingested_at="08:59:57"), Reason.INGESTED_BEFORE_BLOCK)

    def test_strict_identifiers(self):
        strict = Config(feed_date=FEED_DATE, strict_identifiers=True)
        self.assertIs(self.single(strict).disposition, Disposition.ACCEPTED)
        self.assertDeadLetter(self.single(strict, wallet="0xD4…"), Reason.INVALID_IDENTIFIER)
        self.assertDeadLetter(self.single(strict, tx_hash="0xaa1"), Reason.INVALID_IDENTIFIER)

    def test_invalid_log_index(self):
        for value in ("x", "-1"):
            with self.subTest(value=value):
                self.assertDeadLetter(self.single(log_index=value), Reason.INVALID_IDENTIFIER)

    def test_missing_required_column_fails_the_whole_feed(self):
        with self.assertRaises(SchemaError):
            check_columns(["event_id", "tx_hash", "wallet", "side", "amount", "ingested_at"])
        check_columns(list(record()))


class CrossRecordChecksTest(unittest.TestCase):
    def test_exact_redelivery_with_same_event_id(self):
        report = validate([record(), record(ingested_at="09:03:00")], CONFIG)
        self.assertIs(report.outcomes[1].disposition, Disposition.DUPLICATE)
        self.assertIs(report.outcomes[1].reason, Reason.DUPLICATE_EVENT_ID)

    def test_event_id_reused_for_different_trade(self):
        report = validate([record(), record(amount="999")], CONFIG)
        self.assertIs(report.outcomes[1].disposition, Disposition.DEAD_LETTER)
        self.assertIs(report.outcomes[1].reason, Reason.CONFLICTING_EVENT_ID)

    def test_same_transaction_with_two_block_times(self):
        report = validate([record(), record(event_id="evt_2", side="SELL", block_time="09:00:12",
                                            ingested_at="09:00:15")], CONFIG)
        self.assertIs(report.outcomes[1].reason, Reason.TX_BLOCK_TIME_CONFLICT)

    def test_address_case_does_not_hide_a_duplicate(self):
        report = validate([record(), record(event_id="evt_2", wallet=WALLET.upper().replace("0X", "0x"))], CONFIG)
        self.assertIs(report.outcomes[1].reason, Reason.DUPLICATE_TRADE)

    def test_log_index_keeps_identical_fills_in_one_transaction(self):
        report = validate([record(log_index="3"), record(event_id="evt_2", log_index="7")], CONFIG)
        self.assertEqual([o.disposition for o in report.outcomes], [Disposition.ACCEPTED] * 2)

    def test_different_trades_in_same_transaction_are_kept(self):
        report = validate([record(), record(event_id="evt_2", side="SELL", amount="40")], CONFIG)
        self.assertEqual([o.disposition for o in report.outcomes], [Disposition.ACCEPTED] * 2)

    def test_rejected_record_does_not_poison_dedup_state(self):
        # A broken first copy must not make the later valid copy look like a duplicate.
        report = validate([record(block_time="null"), record(event_id="evt_2")], CONFIG)
        self.assertIs(report.outcomes[0].disposition, Disposition.DEAD_LETTER)
        self.assertIs(report.outcomes[1].disposition, Disposition.ACCEPTED)

    def test_output_is_in_block_time_order_not_arrival_order(self):
        rows = [
            record(event_id="late", tx_hash=TX_B, block_time="09:10:00", ingested_at="09:10:02"),
            record(event_id="early", block_time="09:00:00", ingested_at="09:12:00"),
        ]
        report = validate(rows, CONFIG)
        self.assertEqual([t.event_id for t in report.trades], ["early", "late"])

    def test_late_arrival_and_high_lag_are_flagged(self):
        rows = [
            record(event_id="now", tx_hash=TX_B, block_time="10:00:00", ingested_at="10:00:02"),
            record(event_id="old", block_time="09:00:00", ingested_at="10:00:05"),
        ]
        old = validate(rows, CONFIG).outcomes[1]
        self.assertIs(old.disposition, Disposition.ACCEPTED)
        self.assertEqual(set(old.flags), {Flag.LATE_ARRIVAL, Flag.HIGH_INGESTION_LAG})


if __name__ == "__main__":
    unittest.main()
