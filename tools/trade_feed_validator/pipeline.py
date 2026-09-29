"""Replacement for the `for event in feed: insert(parse(event))` pipeline.

Reads a raw CSV feed, validates it and writes two files instead of one table:

* clean_trades.csv - deduplicated, validated trades in block_time order;
* quarantine.csv   - every dropped or dead-lettered record with a reason code,
                     so nothing disappears silently and dead letters can be replayed.

Exit codes: 0 - ok, 1 - the feed is unreadable or has a broken schema,
2 - the dead-letter rate exceeded --max-dead-letter-rate (quality gate).
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from validator import (
    Config,
    Disposition,
    Report,
    SchemaError,
    Trade,
    check_columns,
    validate,
)

CLEAN_COLUMNS = ("event_id", "tx_hash", "log_index", "block_time", "wallet", "side", "amount", "ingested_at", "flags")
QUARANTINE_COLUMNS = ("row", "event_id", "disposition", "reason", "detail")


def read_feed(path: Path) -> Tuple[List[dict], List[str]]:
    # utf-8-sig tolerates a BOM from spreadsheet exports
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        check_columns(reader.fieldnames)
        return list(reader), list(reader.fieldnames)


def write_clean(path: Path, report: Report) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CLEAN_COLUMNS)
        writer.writeheader()
        for outcome in report.accepted:
            trade = outcome.trade
            writer.writerow({
                "event_id": trade.event_id,
                "tx_hash": trade.tx_hash,
                "log_index": "" if trade.log_index is None else trade.log_index,
                "block_time": trade.block_time.isoformat(),
                "wallet": trade.wallet,
                "side": trade.side,
                "amount": trade.amount,
                "ingested_at": trade.ingested_at.isoformat(),
                "flags": ";".join(f.value for f in outcome.flags),
            })


def write_quarantine(path: Path, report: Report, raw_columns: List[str]) -> None:
    # Every input column is kept, known or not, plus surplus fields of malformed
    # rows, so a dead letter can be replayed exactly as it arrived.
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=list(QUARANTINE_COLUMNS) + [f"raw_{c}" for c in raw_columns] + ["raw_extra"]
        )
        writer.writeheader()
        for outcome in report.outcomes:
            if outcome.disposition is Disposition.ACCEPTED:
                continue
            row = {
                "row": outcome.row,
                "event_id": outcome.event_id,
                "disposition": outcome.disposition.value,
                "reason": outcome.reason.value,
                "detail": outcome.detail,
            }
            row.update({f"raw_{c}": outcome.raw.get(c) for c in raw_columns})
            row["raw_extra"] = "|".join(outcome.raw.get(None) or ())
            writer.writerow(row)


def naive_volume(rows: Iterable[dict]) -> Decimal:
    """Volume as the original insert-everything pipeline would report it."""
    total = Decimal(0)
    for raw in rows:
        try:
            total += Decimal((raw.get("amount") or "").strip())
        except InvalidOperation:
            continue
    return total


def summarize(trades: Iterable[Trade]) -> dict:
    volume = defaultdict(Decimal)
    per_wallet = defaultdict(int)
    count = 0
    for trade in trades:
        count += 1
        volume[trade.side] += trade.amount
        per_wallet[trade.wallet] += 1
    return {
        "trades": count,
        "volume": volume["BUY"] + volume["SELL"],
        "buy_volume": volume["BUY"],
        "sell_volume": volume["SELL"],
        "trades_per_wallet": dict(sorted(per_wallet.items())),
    }


def print_report(rows: List[dict], report: Report) -> None:
    accepted = report.accepted
    duplicates = report.with_disposition(Disposition.DUPLICATE)
    dead = report.with_disposition(Disposition.DEAD_LETTER)
    clean = summarize(report.trades)

    print(f"rows read:      {len(rows)}")
    print(f"accepted:       {len(accepted)}")
    print(f"duplicates:     {len(duplicates)}")
    print(f"dead-lettered:  {len(dead)}")
    for outcome in duplicates + dead:
        print(f"  row {outcome.row} {outcome.event_id}: {outcome.disposition.value}/{outcome.reason.value} - {outcome.detail}")
    for outcome in accepted:
        if outcome.flags:
            print(f"  row {outcome.row} {outcome.event_id}: accepted with flags {', '.join(f.value for f in outcome.flags)}")
    print(f"volume:         naive {naive_volume(rows)} -> clean {clean['volume']} "
          f"(buy {clean['buy_volume']}, sell {clean['sell_volume']})")
    print(f"trades/wallet:  {clean['trades_per_wallet']}")


def run(input_path: Path, out_dir: Path, config: Config, max_dead_letter_rate: Optional[float] = None) -> int:
    try:
        rows, columns = read_feed(input_path)
    except (OSError, SchemaError, csv.Error) as err:
        print(f"error: {err}", file=sys.stderr)
        return 1

    report = validate(rows, config)
    print_report(rows, report)
    out_dir.mkdir(parents=True, exist_ok=True)
    # The quarantine is written even when the gate fails: it is what people debug with.
    write_quarantine(out_dir / "quarantine.csv", report, columns)

    rate = len(report.with_disposition(Disposition.DEAD_LETTER)) / len(rows) if rows else 0.0
    if max_dead_letter_rate is not None and rate > max_dead_letter_rate:
        # A spike of broken records usually means the upstream indexer is broken;
        # stop and page someone instead of publishing half of the day.
        print(f"quality gate failed: dead-letter rate {rate:.1%} > {max_dead_letter_rate:.1%}, "
              f"clean output not published", file=sys.stderr)
        return 2

    write_clean(out_dir / "clean_trades.csv", report)
    print(f"written:        {out_dir / 'clean_trades.csv'}, {out_dir / 'quarantine.csv'}")
    return 0


def rate_fraction(value: str) -> float:
    rate = float(value)
    if not 0 <= rate <= 1:  # also rejects NaN, which would silently disable the gate
        raise argparse.ArgumentTypeError(f"{value!r} is not a fraction between 0 and 1")
    return rate


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("feed", type=Path, help="raw feed CSV")
    parser.add_argument("--out-dir", type=Path, default=Path("out"), help="where to write results (default: out)")
    parser.add_argument("--feed-date", type=date.fromisoformat,
                        help="UTC date (YYYY-MM-DD) for feeds whose timestamps are time-only")
    parser.add_argument("--clock-skew-seconds", type=float, default=2,
                        help="how far ingested_at may precede block_time (default: 2)")
    parser.add_argument("--max-lag-seconds", type=float, default=300,
                        help="flag trades ingested later than this after block_time (default: 300)")
    parser.add_argument("--allowed-lateness-seconds", type=float, default=300,
                        help="flag trades older than the newest block_time minus this (default: 300)")
    parser.add_argument("--dedup-horizon-hours", type=float, default=24,
                        help="keep dedup state for this long by block_time, 0 keeps it forever (default: 24)")
    parser.add_argument("--strict-identifiers", action="store_true",
                        help="require full 20-byte addresses and 32-byte tx hashes")
    parser.add_argument("--max-dead-letter-rate", type=rate_fraction,
                        help="fail with exit code 2 if the share of dead-lettered rows exceeds this (0..1)")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    config = Config(
        feed_date=args.feed_date,
        clock_skew_tolerance=timedelta(seconds=args.clock_skew_seconds),
        max_ingestion_lag=timedelta(seconds=args.max_lag_seconds),
        allowed_lateness=timedelta(seconds=args.allowed_lateness_seconds),
        strict_identifiers=args.strict_identifiers,
        dedup_horizon=timedelta(hours=args.dedup_horizon_hours) if args.dedup_horizon_hours > 0 else None,
    )
    return run(args.feed, args.out_dir, config, args.max_dead_letter_rate)


if __name__ == "__main__":
    sys.exit(main())
