# Ingestion data-quality contract

The consumer has two explicit outcomes for `patient_vitals` records:

```text
trusted VitalEvent + ingested_at  -> downstream callback
rejected source record            -> patient_vitals_dlq
```

The `VitalEvent` Pydantic contract remains the sole definition of a valid
observation. The ingestion layer adds only integrity checks that depend on
external context: Kafka key agreement, loaded patient-profile membership, and a
bounded in-memory duplicate-event guard.

## Rejection categories

| Category | Examples | Result |
| --- | --- | --- |
| `malformed_message` | Corrupt JSON or invalid UTF-8 | DLQ and source offset commit |
| `incompatible_payload` | Missing or non-byte Kafka value | DLQ and source offset commit |
| `schema_validation` | Missing field, wrong type, naive timestamp, invalid vital, non-finite number, invalid BP pair | DLQ and source offset commit |
| `key_mismatch` | Kafka key is invalid UTF-8 or disagrees with `patient_id` | DLQ and source offset commit |
| `unknown_patient` | Valid event has no loaded profile | DLQ and source offset commit |
| `duplicate_event` | `event_id` was accepted previously by this running consumer | DLQ and source offset commit |
| `unexpected_processing_error` | Unexpected ingestion failure | DLQ attempt; commit only after DLQ delivery |

Every rejected record becomes a `DeadLetterRecord` on `patient_vitals_dlq`.
It contains category, safe reason code, ingestion time, source topic/partition/
offset, and base64-encoded original value/key for controlled investigation. Raw
payloads are **not** written to normal application logs. DLQ access and
retention must be restricted because those records may contain patient data.

The source offset is committed only after a broker-confirmed DLQ publication.
If DLQ delivery fails or times out, the source offset remains uncommitted for retry. In local tests
and custom embedded usage, a logging-only sink may be injected; the normal
broker-backed consumer uses Kafka DLQ publishing.

## Duplicate policy

The consumer remembers the most recent 10,000 accepted `event_id` values by
default (`ConsumerConfig.deduplication_capacity`). A duplicate seen by that
consumer is rejected to the DLQ and is never passed downstream. The raw archive
also prevents re-writing an `event_id` already present in its local Parquet
dataset, which makes a retry after archive persistence idempotent across a
process restart. Neither mechanism is a distributed exactly-once guarantee;
separate consumers and a replacement archive location do not share identity
state.

## Reproducing a rejection

Run the normal producer and consumer from the Block 3/4 documentation. To
observe the DLQ, open Redpanda Console at `http://localhost:8080` and inspect
`patient_vitals_dlq` after sending an intentionally malformed record using a
Kafka client. The consumer logs only category and Kafka location; inspect the
DLQ record through authorized tooling for the retained source payload.
