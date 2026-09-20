# Ingestion consumer

`VitalIngestionConsumer` reads the `patient_vitals` Kafka/Redpanda topic in a
configurable consumer group (default `medx-vital-ingestion`). It is the trusted
boundary before later stateful processing.

For every message, it decodes UTF-8 JSON and calls `VitalEvent.model_validate`.
That reuses the Block 0 schema rather than duplicating validation rules. A valid
event is wrapped in `IngestedVitalEvent` with an independent UTC `ingested_at`.
`event.event_time` remains the source measurement time; `ingested_at` is when
this service accepted it.

## Failure policy

Malformed JSON, invalid UTF-8, missing/non-byte payloads, and schema-invalid
events are rejected to the Kafka dead-letter topic `patient_vitals_dlq`. They
are counted in `consumer.metrics.rejected`, retained in a `DeadLetterRecord`,
and committed only after that DLQ publication succeeds. This makes failure
observable without allowing a poison payload to block the partition forever.
Normal logs contain category and Kafka location, not raw payload content.

When a Kafka key is present, it must be UTF-8 and match `event.patient_id`; a
mismatch is rejected so the producer's keyed per-patient routing cannot be
silently contradicted by its payload. Keyless records remain valid for future
compatible sources because patient identity is canonical in `VitalEvent`.

Accepted records are committed only after the downstream-boundary callback
returns successfully. If that callback fails, the offset is deliberately left
uncommitted for a later retry. The consumer does not implement ordering,
full durable deduplication, state, or clinical logic; it retains event ID, patient ID,
measurement time, sequence number, and receipt time for the next block.

## Configuration and local verification

Configuration defaults are portable and can be overridden with:

```text
MEDX_KAFKA_BOOTSTRAP_SERVERS
MEDX_KAFKA_TOPIC
MEDX_KAFKA_CONSUMER_GROUP
MEDX_KAFKA_AUTO_OFFSET_RESET
MEDX_KAFKA_DLQ_TOPIC
```

With the Block 3 Redpanda Compose setup running, start the consumer in one
terminal:

```powershell
python run_consumer.py
```

Then publish events in another terminal:

```powershell
$env:MEDX_MAX_EVENTS = 20; python run_producer.py
```

The consumer prints accepted `IngestedVitalEvent` envelopes. Verify that each
contains a complete `event`, preserved `event_time`, and a distinct later
`ingested_at`. Redpanda Console at `http://localhost:8080` can independently
show the source records in `patient_vitals`.

See [the data-quality contract](data_quality.md) for rejection categories, DLQ
records, duplicate behavior, and safe debugging guidance.
