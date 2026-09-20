# MedX streaming foundation

This repository's supported foundation is the Python vitals streaming path:

```text
validated profiles → synthetic VitalEvent source → Kafka/Redpanda →
ingestion validation/DLQ → local Parquet archive → local state + event-time window
```

The older FastAPI prototype in `main.py` and its adjacent simulator/detector
files are not part of this streaming path and are not the entry point for the
current foundation.

## Prerequisites

- Python 3.11 or newer
- Docker Desktop with Compose, for a live Redpanda run

Install dependencies from the repository root:

```powershell
python -m pip install -r requirements.txt
```

Run the offline test suite:

```powershell
python -m unittest discover -s tests -v
```

## Live demo

Start the local single-broker Redpanda and Console:

```powershell
docker compose up -d
```

In one terminal, start the archive-aware consumer:

```powershell
$env:MEDX_ARCHIVE_DIRECTORY = "raw_events"
$env:MEDX_ARCHIVE_BATCH_SIZE = "100"
python run_consumer.py
```

In another terminal, publish a finite deterministic demo:

```powershell
$env:MEDX_SIMULATOR_SEED = "7"
$env:MEDX_SCENARIO_DEMO = "1"
$env:MEDX_MAX_EVENTS = "100"
python run_producer.py
```

Use Ctrl+C to stop the consumer cleanly and flush its partial archive batch.
Redpanda Console is available at `http://localhost:8080`.

## Replay

Replay the source archive without changing event IDs or measurements:

```powershell
$env:MEDX_ARCHIVE_DIRECTORY = "raw_events"
$env:MEDX_REPLAY_KAFKA_TOPIC = "patient_vitals_replay"
$env:MEDX_REPLAY_MAX_EVENTS = "100"
python run_replay.py
```

Consume it through a separate group and write it to a separate archive
directory to avoid intentionally replaying IDs into the live consumer's local
duplicate cache:

```powershell
$env:MEDX_KAFKA_TOPIC = "patient_vitals_replay"
$env:MEDX_KAFKA_CONSUMER_GROUP = "medx-vital-replay-ingestion"
$env:MEDX_ARCHIVE_DIRECTORY = "raw_events_replay"
python run_consumer.py
```

Configuration defaults and operational details are documented in `docs/`.
The Compose setup is a single-broker development environment; it is not a
production deployment or a distributed exactly-once design.
