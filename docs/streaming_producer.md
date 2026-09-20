# Streaming producer

`VitalEventProducer` is the broker boundary between `VitalStreamSimulator` and
Kafka/Redpanda. It publishes exactly one complete canonical `VitalEvent` as a
UTF-8 JSON message to the `patient_vitals` topic.

The JSON value comes directly from `VitalEvent.model_dump_json()`. It preserves
the Block 0 names and types, including a UTC ISO 8601 `event_time`; no producer-
specific event schema exists. The UTF-8 Kafka key is `patient_id`. Kafka's keyed
partitioner consequently routes one patient's events consistently while many
patients coexist in the same physical topic.

## Configuration

`ProducerConfig` defaults to `localhost:19092`, topic `patient_vitals`, and
client ID `medx-vital-producer`. Override the runnable script with:

```text
MEDX_KAFKA_BOOTSTRAP_SERVERS
MEDX_KAFKA_TOPIC
MEDX_KAFKA_CLIENT_ID
MEDX_SIMULATOR_SEED
MEDX_MAX_EVENTS
```

The producer uses acknowledgements from all in-sync replicas and idempotent
production. It processes delivery callbacks after every publish, raises a
`ProducerDeliveryError` for delivery failures, and flushes during shutdown.

## Local end-to-end verification

1. Install dependencies: `python -m pip install -r requirements.txt`.
2. Start Redpanda and Console: `docker compose up -d`.
3. Publish a finite demo run:
   - PowerShell: `$env:MEDX_MAX_EVENTS = 20; python run_producer.py`
   - bash/zsh: `MEDX_MAX_EVENTS=20 python run_producer.py`
4. Open `http://localhost:8080`, select `patient_vitals`, and inspect records.
   Each value contains all five vitals; the record key is the patient ID.
5. Omit `MEDX_MAX_EVENTS` to run continuously. Stop with Ctrl+C; the producer
   flushes queued events during shutdown.

The Compose file exposes Redpanda's Kafka API to the host at `localhost:19092`
and the console at `localhost:8080`. It follows Redpanda's single-broker Docker
pattern, including separate internal and external Kafka addresses.

## Test strategy

Unit tests inject a Kafka-client test double. They verify serialized messages,
topic/key routing, delivery failure propagation, lifecycle flushing, and the
simulator-to-producer path without requiring a running broker. The Compose flow
above is the real broker verification path.
