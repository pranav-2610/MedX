# Raw event archive and replay

The raw archive records trusted observations for reproducible demos, debugging,
and later evaluation. It is infrastructure only: it does not add clinical
meaning, modify `VitalEvent`, or archive rejected/DLQ records.

```text
simulator
   ↓
Kafka patient_vitals
   ↓
ingestion validation + data quality
   ├──→ state/window
   └──→ raw_events/YYYY-MM-DD/part-*.parquet
```

Then:

```text
raw_events
   ↓
ReplayProducer
   ↓
patient_vitals_replay
   ↓
same ingestion validation + data quality + state/window + archive path
```

## Archive format and durability

`RawEventArchive` writes local PyArrow Parquet, partitioned by the **UTC date
of `VitalEvent.event_time`**. A partition looks like:

```text
raw_events/
  2026-09-20/
    part-000001.parquet
    part-000002.parquet
```

Every row preserves the ten canonical `VitalEvent` fields exactly. Additional
archive metadata is stored outside that contract: `ingested_at`, Kafka topic,
partition, offset, `archive_sequence`, and `archived_at`.

Within one archive directory, an already archived canonical `event_id` is not
written again. This makes a Kafka retry after a successful staging write safe
across a consumer restart. The archive builds that local identity index from
its Parquet files at startup; it is intentionally a local-demo durability aid,
not a distributed exactly-once protocol.

`archive_sequence` records the order in which this archive writer processed
accepted records; replay uses it before Kafka location fields. It preserves
per-patient ordering and the writer's recorded interleaving. Kafka does not
provide a global ordering across independent topic partitions, so the archive
does not claim to recreate one from `event_time` alone.

Writes are batched by configurable `batch_size` (default 100). Before an
accepted event can be acknowledged to Kafka, it is atomically written to a
valid per-date staging Parquet file. Full batches, and all pending batches on
clean shutdown, atomically become `part-*.parquet`. Existing finalized parts
are never overwritten. If the archive callback fails, the ingestion consumer
does not commit the source offset and does not update state/window.

The completed Parquet file is flushed to the operating system before and after
its atomic replacement. This protects normal process interruption; it is not a
claim of power-loss durability for every filesystem, because portable Python
does not provide a cross-platform directory-metadata sync primitive.

## Live recording

Start Redpanda, then start the archive-aware consumer:

```powershell
docker compose up -d
$env:MEDX_ARCHIVE_DIRECTORY = "raw_events"
$env:MEDX_ARCHIVE_BATCH_SIZE = "100"
python run_consumer.py
```

In another terminal, publish source events:

```powershell
$env:MEDX_MAX_EVENTS = "100"
python run_producer.py
```

Stop the consumer with Ctrl+C to finalize pending batches. Inspect the resulting
Parquet partitions under `raw_events/`.

## Replay

`run_replay.py` reads stored rows, reconstructs and Pydantic-validates the
original `VitalEvent`, and sends it through the existing `VitalEventProducer`.
It never generates new vitals or new event IDs.

```powershell
$env:MEDX_ARCHIVE_DIRECTORY = "raw_events"
$env:MEDX_REPLAY_KAFKA_TOPIC = "patient_vitals_replay"
$env:MEDX_REPLAY_MAX_EVENTS = "100"
$env:MEDX_REPLAY_INTERVAL_SECONDS = "0.1"
python run_replay.py
```

Optional filters: `MEDX_REPLAY_PATIENT_IDS` as comma-separated IDs,
`MEDX_REPLAY_START_DATE`/`MEDX_REPLAY_END_DATE` as UTC `YYYY-MM-DD`, and
`MEDX_REPLAY_MAX_EVENTS`. `MEDX_REPLAY_INTERVAL_SECONDS=0` replays as fast as
the producer permits.

Consume replay through the same path, using a separate group and input topic:

```powershell
$env:MEDX_KAFKA_TOPIC = "patient_vitals_replay"
$env:MEDX_KAFKA_CONSUMER_GROUP = "medx-vital-replay-ingestion"
$env:MEDX_ARCHIVE_DIRECTORY = "raw_events_replay"
python run_consumer.py
```

Use a separate replay topic and consumer process/group. Exact event IDs are
intentionally retained, so feeding replay records into an already-running
consumer instance that remembers those IDs correctly triggers existing duplicate
protection; no special replay bypass exists. A different output archive avoids
mixing replay output with the source recording.

DLQ records are never archived because only the consumer's successful trusted
callback invokes `ArchiveAndStateProcessor`.
