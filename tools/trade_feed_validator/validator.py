"""Validation core for raw trade events coming from a blockchain indexer.

Every raw record ends up with exactly one outcome:

* accepted     - safe to load into the analytics table (possibly with warning flags);
* duplicate    - the same trade is already accepted, the record is dropped;
* dead_letter  - the record cannot be trusted as-is and is kept for repair and replay.

The validator is streaming: it keeps only the state needed for deduplication and
consistency checks, so the same code works for a CSV file and for a live consumer.
"""

from __future__ import annotations

import heapq
import itertools
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Iterable, Mapping, Optional, Sequence, Tuple

REQUIRED_COLUMNS = ("event_id", "tx_hash", "block_time", "wallet", "side", "amount", "ingested_at")
OPTIONAL_COLUMNS = ("log_index",)
NULL_TOKENS = frozenset({"", "null", "none", "nan", "n/a"})
SIDES = frozenset({"BUY", "SELL"})
# On-chain amounts are uint256, so anything larger cannot be a real transfer.
# The bound also keeps absurd exponents like 1e999999999 out of aggregates.
MAX_AMOUNT = Decimal(2**256 - 1)
EVM_ADDRESS = re.compile(r"^0x[0-9a-f]{40}$")
EVM_TX_HASH = re.compile(r"^0x[0-9a-f]{64}$")


class Disposition(str, Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    DEAD_LETTER = "dead_letter"


class Reason(str, Enum):
    MALFORMED_ROW = "malformed_row"
    MISSING_FIELD = "missing_field"
    INVALID_TIMESTAMP = "invalid_timestamp"
    INVALID_SIDE = "invalid_side"
    INVALID_AMOUNT = "invalid_amount"
    INVALID_IDENTIFIER = "invalid_identifier"
    INGESTED_BEFORE_BLOCK = "ingested_before_block_time"
    DUPLICATE_EVENT_ID = "duplicate_event_id"
    CONFLICTING_EVENT_ID = "conflicting_event_id"
    DUPLICATE_TRADE = "duplicate_trade"
    CONFLICTING_TRADE = "conflicting_trade"
    TX_BLOCK_TIME_CONFLICT = "tx_block_time_conflict"
    OUTSIDE_DEDUP_HORIZON = "outside_dedup_horizon"


class Flag(str, Enum):
    """Warnings on accepted trades: the data is usable, but downstream must know."""

    LATE_ARRIVAL = "late_arrival"
    HIGH_INGESTION_LAG = "high_ingestion_lag"


class SchemaError(ValueError):
    """The feed as a whole is unusable (e.g. a required column is missing)."""


class RecordError(ValueError):
    def __init__(self, reason: Reason, detail: str):
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class Config:
    # Date to attach to time-only timestamps like "09:14:02"; ISO datetimes ignore it.
    feed_date: Optional[date] = None
    # ingested_at may trail block_time only by NTP-scale clock skew, never by minutes.
    clock_skew_tolerance: timedelta = timedelta(seconds=2)
    max_ingestion_lag: timedelta = timedelta(minutes=5)
    # Trades older than (newest block_time seen - allowed_lateness) land in windows
    # that downstream jobs may have already closed.
    allowed_lateness: timedelta = timedelta(minutes=5)
    # Enforce full EVM identifiers. Off by default only because the sample feed
    # uses shortened addresses and hashes.
    strict_identifiers: bool = False
    # Dedup state is kept only for trades within this distance of the newest
    # block_time, so a long-running consumer uses bounded memory. Older records
    # cannot be checked for duplicates and are dead-lettered. None keeps everything.
    dedup_horizon: Optional[timedelta] = timedelta(hours=24)


@dataclass(frozen=True)
class Trade:
    event_id: str
    tx_hash: str
    log_index: Optional[int]
    block_time: datetime
    wallet: str
    side: str
    amount: Decimal
    ingested_at: datetime

    @property
    def natural_key(self) -> tuple:
        # (tx_hash, log_index) identifies the emitted log on its own. Without it two
        # identical fills inside one transaction are indistinguishable from a
        # redelivery, so they collapse into one trade.
        if self.log_index is not None:
            return (self.tx_hash, self.log_index)
        return (self.tx_hash, None, self.wallet, self.side, self.amount)

    @property
    def payload(self) -> tuple:
        return (self.tx_hash, self.log_index, self.block_time, self.wallet, self.side, self.amount)


@dataclass(frozen=True)
class Outcome:
    row: int
    raw: Mapping[str, Optional[str]]
    disposition: Disposition
    trade: Optional[Trade] = None
    reason: Optional[Reason] = None
    detail: str = ""
    flags: Tuple[Flag, ...] = ()

    @property
    def event_id(self) -> Optional[str]:
        if self.trade is not None:
            return self.trade.event_id
        return _clean(self.raw.get("event_id"))


def check_columns(columns: Optional[Sequence[str]]) -> None:
    columns = list(columns or ())
    missing = [c for c in REQUIRED_COLUMNS if c not in columns]
    if missing:
        raise SchemaError(f"feed is missing required columns: {', '.join(missing)}")
    # DictReader keeps only the last of repeated names, silently dropping the others
    repeated = sorted({c for c in columns if columns.count(c) > 1})
    if repeated:
        raise SchemaError(f"feed has repeated columns: {', '.join(repeated)}")


def _clean(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return None if value.lower() in NULL_TOKENS else value


def parse_timestamp(value: str, name: str, feed_date: Optional[date]) -> datetime:
    try:
        # Python < 3.11 does not understand the "Z" suffix
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            clock = time.fromisoformat(value)
        except ValueError:
            raise RecordError(Reason.INVALID_TIMESTAMP, f"{name}={value!r} is not an ISO 8601 timestamp") from None
        if feed_date is None:
            raise RecordError(Reason.INVALID_TIMESTAMP, f"{name}={value!r} has no date and no feed date is configured")
        parsed = datetime.combine(feed_date, clock)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_record(raw: Mapping[str, Optional[str]], config: Config) -> Trade:
    """Stateless checks: types, nulls, enums, ranges and per-record invariants."""
    # csv.DictReader puts surplus fields under the None key: the row does not match
    # the header, so any field may be shifted and none of them can be trusted.
    if raw.get(None):
        raise RecordError(Reason.MALFORMED_ROW, f"{len(raw[None])} more field(s) than the header")

    values = {column: _clean(raw.get(column)) for column in REQUIRED_COLUMNS + OPTIONAL_COLUMNS}

    missing = [c for c in REQUIRED_COLUMNS if values[c] is None]
    if missing:
        raise RecordError(Reason.MISSING_FIELD, f"null or empty: {', '.join(missing)}")

    side = values["side"].upper()
    if side not in SIDES:
        raise RecordError(Reason.INVALID_SIDE, f"side={values['side']!r} is not one of {sorted(SIDES)}")

    try:
        amount = Decimal(values["amount"])
    except InvalidOperation:
        raise RecordError(Reason.INVALID_AMOUNT, f"amount={values['amount']!r} is not a number") from None
    if not amount.is_finite() or amount <= 0:
        raise RecordError(Reason.INVALID_AMOUNT, f"amount={values['amount']!r} must be a finite positive number")
    if amount > MAX_AMOUNT:
        raise RecordError(Reason.INVALID_AMOUNT, f"amount={values['amount']!r} exceeds the uint256 range")

    # EVM hex identifiers are case-insensitive (mixed case is only a checksum),
    # so "0xAB.." and "0xab.." must not become two different wallets.
    tx_hash = values["tx_hash"].lower()
    wallet = values["wallet"].lower()
    if config.strict_identifiers:
        if not EVM_TX_HASH.match(tx_hash):
            raise RecordError(Reason.INVALID_IDENTIFIER, f"tx_hash={values['tx_hash']!r} is not a 32-byte hex hash")
        if not EVM_ADDRESS.match(wallet):
            raise RecordError(Reason.INVALID_IDENTIFIER, f"wallet={values['wallet']!r} is not a 20-byte hex address")

    log_index = None
    if values["log_index"] is not None:
        try:
            log_index = int(values["log_index"])
        except ValueError:
            raise RecordError(Reason.INVALID_IDENTIFIER, f"log_index={values['log_index']!r} is not an integer") from None
        if log_index < 0:
            raise RecordError(Reason.INVALID_IDENTIFIER, f"log_index={log_index} is negative")

    block_time = parse_timestamp(values["block_time"], "block_time", config.feed_date)
    ingested_at = parse_timestamp(values["ingested_at"], "ingested_at", config.feed_date)
    if ingested_at < block_time - config.clock_skew_tolerance:
        raise RecordError(
            Reason.INGESTED_BEFORE_BLOCK,
            f"ingested_at {ingested_at:%H:%M:%S} is {block_time - ingested_at} before block_time {block_time:%H:%M:%S}",
        )

    return Trade(
        event_id=values["event_id"],
        tx_hash=tx_hash,
        log_index=log_index,
        block_time=block_time,
        wallet=wallet,
        side=side,
        amount=amount,
        ingested_at=ingested_at,
    )


class FeedValidator:
    """Stateful checks across records: deduplication and cross-record consistency."""

    def __init__(self, config: Config = Config()):
        self.config = config
        self._by_event_id: dict = {}
        self._by_natural_key: dict = {}
        self._tx_block_time: dict = {}
        self._watermark: Optional[datetime] = None
        self._expiry: list = []  # heap of (block_time, sequence, trade)
        self._sequence = itertools.count()

    def process(self, row: int, raw: Mapping[str, Optional[str]]) -> Outcome:
        try:
            trade = parse_record(raw, self.config)
        except RecordError as err:
            return Outcome(row, raw, Disposition.DEAD_LETTER, reason=err.reason, detail=err.detail)

        horizon = self.config.dedup_horizon
        if horizon is not None and self._watermark is not None and trade.block_time < self._watermark - horizon:
            return Outcome(row, raw, Disposition.DEAD_LETTER, trade, Reason.OUTSIDE_DEDUP_HORIZON,
                           f"block_time is more than {horizon} older than the newest trade, duplicates cannot be ruled out")

        # Checked before deduplication: a copy of a known trade with another
        # block_time is a reorg or a corrupted timestamp, not a harmless duplicate.
        # One transaction lives in exactly one block, so a human or a chain lookup decides.
        known_time = self._tx_block_time.get(trade.tx_hash)
        if known_time is not None and known_time != trade.block_time:
            return Outcome(row, raw, Disposition.DEAD_LETTER, trade, Reason.TX_BLOCK_TIME_CONFLICT,
                           f"{trade.tx_hash} was already accepted with block_time {known_time:%Y-%m-%d %H:%M:%S}")

        # Rejected records never reach the state below, so a broken record cannot
        # cause a later valid copy of the same trade to be dropped as a duplicate.
        seen = self._by_event_id.get(trade.event_id)
        if seen is not None:
            if seen.payload == trade.payload:
                return Outcome(row, raw, Disposition.DUPLICATE, trade, Reason.DUPLICATE_EVENT_ID,
                               f"redelivery of {seen.event_id}")
            return Outcome(row, raw, Disposition.DEAD_LETTER, trade, Reason.CONFLICTING_EVENT_ID,
                           f"event_id {trade.event_id} was already accepted with a different payload")

        seen = self._by_natural_key.get(trade.natural_key)
        if seen is not None:
            if seen.payload == trade.payload:
                # Reserve the duplicate's event_id too, so it cannot later be reused
                # for a different trade and end up in both quarantine and clean output.
                self._by_event_id[trade.event_id] = trade
                heapq.heappush(self._expiry, (trade.block_time, next(self._sequence), trade))
                return Outcome(row, raw, Disposition.DUPLICATE, trade, Reason.DUPLICATE_TRADE,
                               f"same trade as {seen.event_id} under a new event_id")
            return Outcome(row, raw, Disposition.DEAD_LETTER, trade, Reason.CONFLICTING_TRADE,
                           f"same log as {seen.event_id} but a different payload")

        flags = []
        if trade.ingested_at - trade.block_time > self.config.max_ingestion_lag:
            flags.append(Flag.HIGH_INGESTION_LAG)
        if self._watermark is not None and trade.block_time < self._watermark - self.config.allowed_lateness:
            flags.append(Flag.LATE_ARRIVAL)

        self._by_event_id[trade.event_id] = trade
        self._by_natural_key[trade.natural_key] = trade
        self._tx_block_time[trade.tx_hash] = trade.block_time
        self._watermark = max(self._watermark or trade.block_time, trade.block_time)
        heapq.heappush(self._expiry, (trade.block_time, next(self._sequence), trade))
        self._evict()
        return Outcome(row, raw, Disposition.ACCEPTED, trade, flags=tuple(flags))

    def _evict(self) -> None:
        if self.config.dedup_horizon is None:
            return
        cutoff = self._watermark - self.config.dedup_horizon
        while self._expiry and self._expiry[0][0] < cutoff:
            trade = heapq.heappop(self._expiry)[2]
            for index, key in ((self._by_event_id, trade.event_id), (self._by_natural_key, trade.natural_key)):
                if index.get(key) is trade:
                    del index[key]
            if self._tx_block_time.get(trade.tx_hash) == trade.block_time:
                del self._tx_block_time[trade.tx_hash]

    @property
    def state_size(self) -> int:
        return len(self._by_event_id)


@dataclass(frozen=True)
class Report:
    outcomes: Tuple[Outcome, ...]

    @property
    def accepted(self) -> list:
        # Arrival order is not chain order: analytics get trades in block_time order.
        return sorted(
            self.with_disposition(Disposition.ACCEPTED),
            key=lambda o: (o.trade.block_time, o.trade.tx_hash, o.trade.log_index or 0, o.trade.event_id),
        )

    @property
    def trades(self) -> list:
        return [o.trade for o in self.accepted]

    def with_disposition(self, disposition: Disposition) -> list:
        return [o for o in self.outcomes if o.disposition is disposition]

    def reason_counts(self) -> Counter:
        return Counter(o.reason.value for o in self.outcomes if o.reason is not None)


def validate(rows: Iterable[Mapping[str, Optional[str]]], config: Config = Config()) -> Report:
    validator = FeedValidator(config)
    return Report(tuple(validator.process(row, raw) for row, raw in enumerate(rows, start=1)))
