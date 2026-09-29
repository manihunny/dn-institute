# Trade feed validator

A small, dependency-free validator that sits between a blockchain indexer and the analytics table. It replaces the current pipeline

```
for event in feed_stream:
    row = parse(event)
    insert_into(analytics_table, row)
```

with one that gives every raw event exactly one outcome:

| Outcome | Meaning | Where it goes |
|---|---|---|
| `accepted` | Valid, unique trade. May carry warning flags (`late_arrival`, `high_ingestion_lag`). | `clean_trades.csv`, sorted by `block_time` |
| `duplicate` | The same trade was already accepted. Nothing is lost by dropping it. | `quarantine.csv` (audit trail) |
| `dead_letter` | The record cannot be trusted as-is but may be real. Kept verbatim for repair and replay. | `quarantine.csv` |

Nothing disappears silently: `rows read = accepted + duplicate + dead_letter`, and every non-accepted row has a machine-readable reason code.

## Quick start

Requires Python 3.9+ and nothing else (standard library only; tested on Python 3.14).

```bash
cd tools/trade_feed_validator

# no dependencies to install

# run the pipeline on the sample feed (its timestamps are time-only, so a date is required)
python pipeline.py sample_feed.csv --feed-date 2026-01-01 --out-dir out

# same run with a quality gate: exit code 2 if more than 10% of rows are dead-lettered
python pipeline.py sample_feed.csv --feed-date 2026-01-01 --max-dead-letter-rate 0.1

# run the tests
python -m unittest discover -s tests -v
```

Output on the sample feed:

```
rows read:      8
accepted:       4
duplicates:     2
dead-lettered:  2
  row 3 evt_003: duplicate/duplicate_trade - same trade as evt_002 under a new event_id
  row 7 evt_007: duplicate/duplicate_trade - same trade as evt_006 under a new event_id
  row 5 evt_005: dead_letter/missing_field - null or empty: block_time
  row 8 evt_008: dead_letter/ingested_before_block_time - ingested_at 09:59:50 is 0:10:10 before block_time 10:10:00
volume:         naive 705000 -> clean 375000 (buy 330000, sell 45000)
trades/wallet:  {'0xd4…': 2, '0xe5…': 1, '0xf6…': 1}
written:        out/clean_trades.csv, out/quarantine.csv
```

Run `python pipeline.py --help` for all options (clock-skew tolerance, lag and lateness thresholds, strict identifier checks).

## Files

| File | Purpose |
|---|---|
| `sample_feed.csv` | The raw feed from the challenge, verbatim (`null` kept as a literal token, as it arrives). |
| `validator.py` | Validation core: stateless record checks (`parse_record`) and stateful cross-record checks (`FeedValidator`). Streaming, so it works for a file or a live consumer. |
| `pipeline.py` | CLI: reads CSV, runs the validator, writes `clean_trades.csv` / `quarantine.csv`, prints a summary, enforces the quality gate. |
| `tests/` | `unittest` suite: one test per sample issue, plus edge cases that real feeds produce. |

## Data-quality issues in the sample

| # | Issue | Rows | Handling |
|---|---|---|---|
| 1 | **Redelivery under a new `event_id`.** `evt_003` is the same trade as `evt_002` (tx `0xaa2`, same wallet, side, amount, block time) delivered again 2m38s later with a fresh `event_id`. Typical indexer retry after a timeout. Deduplicating on `event_id` does not catch it. | 2, 3 | Drop `evt_003` as `duplicate_trade`, keep the first copy. |
| 2 | **Double emission.** `evt_007` repeats `evt_006` (tx `0xaa5`) with the *same* `ingested_at`, i.e. the indexer emitted one trade twice in the same batch (fan-out bug, two consumers on one partition, etc.). Same fix as #1, different root cause, worth its own alert. | 6, 7 | Drop `evt_007` as `duplicate_trade`. |
| 3 | **Missing `block_time`.** `evt_005` has `block_time = null`. The trade cannot be placed in time. A naive parser either fails, stores `NULL`, or stores the literal string `"null"`. | 5 | Dead-letter (`missing_field`). See [Handling evt_005](#handling-evt_005). |
| 4 | **Impossible timestamps.** `evt_008` was "ingested" at 09:59:50, 10m10s *before* its block was produced at 10:10:00. Causality is violated, so at least one of the two timestamps is wrong (collector clock skew, a mislabeled/reorged block, or a field swap). This is far beyond NTP-scale skew. | 8 | Dead-letter (`ingested_before_block_time`). A 2 s tolerance (`--clock-skew-seconds`) absorbs honest clock skew. |
| 5 | **Arrival order is not ingestion or chain order.** Row 8 arrives last but claims to have been ingested before rows 6 and 7, and a feed that promises "arrival order, not chain order" can deliver any block late. The current pipeline inserts in arrival order, so anything that relies on insert order (running totals, "last price", windows closed on arrival) is wrong. | 6-8 (and any late event) | Output is sorted by `(block_time, tx_hash, log_index, event_id)`. Trades older than the watermark minus `allowed_lateness` are flagged `late_arrival` so downstream can recompute already-published windows; ingestion lag over 5 min is flagged `high_ingestion_lag`. |
| 6 | **Feed contract gaps** (not bad values, but they make some errors undetectable). Timestamps are time-only (the date must come from outside; midnight-crossing batches become ambiguous). No `log_index`, so two genuinely identical fills in one transaction cannot be told apart from a redelivery. No price or token/pool column, so VWAP cannot be computed from this feed at all. Identifiers are shortened. | all | Time-only values require an explicit `--feed-date`. If a `log_index` column is present it becomes part of the dedup key. `--strict-identifiers` enforces 20-byte addresses and 32-byte hashes on real feeds (off by default only because the sample is shortened). |

Additional checks that do not fire on the sample but do on real feeds (all covered by tests): other null tokens (`""`, `NULL`, `None`, `NaN`, `n/a`), non-numeric / zero / negative / infinite amounts, unknown `side`, unparseable timestamps, ISO timestamps with time-zone offsets (normalised to UTC), mixed-case addresses (`0xAB…` and `0xab…` are the same wallet), the same `event_id` reused for a different trade (`conflicting_event_id`), one transaction reported with two block times (`tx_block_time_conflict`, a reorg signal), missing columns (whole feed rejected with exit code 1).

## What each issue corrupts downstream

Raw feed vs. validated feed:

| Metric | Naive pipeline | Validated | In quarantine, pending repair |
|---|---|---|---|
| Trades | 8 | 4 | 2 dead letters (+2 dropped duplicates) |
| Volume | 705,000 | 375,000 | 120,000 (`evt_005` 30,000 + `evt_008` 90,000) |
| Buy / sell volume | 540,000 / 165,000 | 330,000 / 45,000 | 0 / 120,000 |
| Trades per wallet `0xD4` / `0xE5` / `0xF6` | 3 / 2 / 3 | 2 / 1 / 1 | 0 / 1 / 1 |

- **Duplicates (#1, #2)** inflate volume by 210,000 (+56% over the true 375,000 in the validated set) and trade count 1.5x. Buy-side volume is inflated specifically, so buy/sell imbalance and "net flow" indicators point the wrong way. VWAP double-weights the duplicated trades' prices. Wallet activity and clustering see `0xD4` and `0xF6` as more active than they are, which is exactly the kind of signal (repeated same-size trades in a short window) that wash-trading detectors key on, so duplicates create **false positives** for manipulation.
- **Missing time (#3)** makes the table internally inconsistent: the trade is counted in daily totals but falls out of every time-bucketed query (`WHERE block_time BETWEEN …`), so the sum of hourly volumes no longer equals daily volume. Coercing it to epoch 0 or to ingestion time creates a phantom trade in the wrong bucket. Dropping it silently understates sell volume by 30,000 (40% of wallet `0xE5`'s activity).
- **Impossible timestamps (#4)** put 90,000 of volume into an unknown bucket (10:10 by block time, 09:59 by ingestion), produce negative ingestion latency (breaking SLA dashboards), and let a watermark-based aggregator treat the event as on-time for a window it does not belong to. It also hides behaviour: with `evt_008`, wallet `0xF6` buys 90,000 and sells 90,000 seven minutes later, a round trip with zero net position. Without it, `0xF6` looks like a buyer holding 90,000. A round trip of equal size is itself a pattern worth surveillance, so this record must be repaired, not forgotten.
- **Ordering (#5)** breaks anything that assumes monotonic time in insert order: running sums, "last trade price", candle close prices, time-to-next-trade features, and incremental aggregates that finalise a window when a newer event arrives.
- **Contract gaps (#6)** make a wrong feed indistinguishable from a right one: without `log_index` the validator must assume identical fills in one transaction are duplicates (it may under-count real multi-fill transactions), and without a date the same file loaded twice on different days produces different trades.

## Handling evt_005

**Dead-letter it**, not drop and not backfill.

- **Dropping** is silent data loss: 30,000 of sell volume vanishes and nobody can tell afterwards. The row is otherwise healthy (valid tx hash, wallet, side, amount), so the trade almost certainly happened.
- **Backfilling from `ingested_at`** (09:58:30 minus the typical 3-4 s lag ≈ 09:58:26) looks tempting, but it writes a guess into the analytics table as if it were a fact. Lag is not stable: the same sample contains a 2m38s redelivery and a record with a negative lag, and during indexer outages or re-syncs lag grows to hours. A guessed timestamp can move the trade into the wrong candle or even the wrong day, and there is no marker left to find it later.
- **Dead-lettering** keeps the raw record with reason `missing_field`. The missing value is recoverable *deterministically*: `block_time` is a property of the block that contains tx `0xaa4`, so a repair job fetches the receipt from a node or a second indexer, fills in the true timestamp and replays the record. Replay is safe because deduplication is keyed on the trade, not on the delivery.

What would change the answer:

- **An RPC node / archive API is available inline** → backfill inside the pipeline from the chain (authoritative, not a guess) and only dead-letter if the lookup fails.
- **Consumers only need daily totals** and the feed date is certain → impute from `ingested_at` with an `is_imputed` flag, since any timestamp within the day gives the same answer.
- **Missing timestamps spike** (e.g. more than 1% of a batch) → it is an upstream outage, not a bad record. Fail the batch (`--max-dead-letter-rate`) rather than publishing a day with a hole in it.
- **The transaction is not found on-chain** during repair → the event is a phantom; drop it permanently and alert on the indexer.

## Catching this class of problems automatically

*(141 words)*

Treat the feed as a versioned data contract enforced at ingestion, not a stream to trust. The contract declares types, nullability, enums, ranges, the natural key (`tx_hash`, `log_index`) and invariants: `ingested_at >= block_time`, one `block_time` per transaction. Every record passes this gate; failures go to a dead-letter queue with reason codes and a replay path, never into analytics. Loads are idempotent (upsert/merge on the natural key), so retries and replays cannot double-count. The gate emits per-batch metrics (duplicate rate, null rate, DLQ rate, lag percentiles, out-of-order share), alerts on deviations from their baseline and blocks publication when thresholds are breached. A daily reconciliation compares trade counts and volume per block range against an independent source, such as an RPC node or second indexer, catching what per-record rules cannot. Each incident found downstream becomes a new contract rule and a regression fixture.
