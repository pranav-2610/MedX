# Per-patient state store

The patient state store gives later clinical-processing blocks a current,
bounded history of trusted vital observations. It is deliberately a local,
in-memory structure:

```text
dict[patient_id -> deque[VitalEvent]]
```

Each deque contains only one patient's validated `VitalEvent` objects in their
**arrival order**. `event_time` and `sequence_number` are retained unchanged;
this block does not reorder, score, interpret, or reject events.
The ingestion and data-quality boundary performs those admission decisions
before calling `store.update(ingested_event.event)`. If a retained event is
retried after a later callback step fails, `update` recognises its `event_id`
and leaves the local history unchanged.

## API

```python
store = PatientStateStore(max_events_per_patient=60)
updated = store.update(event)
recent = store.get(event.patient_id)
```

`update(event)` creates a patient's deque when needed, appends the event, and
returns a snapshot. `get(patient_id)` returns a tuple snapshot or `None` for no
state. At capacity, `deque(maxlen=N)` removes the oldest event automatically.
Default capacity is 60 events per patient.

Snapshots and stored events are defensively copied, so callers cannot mutate a
returned object and corrupt internal state. The store intentionally allows a
new ID when used directly in a unit test; the normal pipeline prevents unknown
patients before this layer.

This is not a database, Kafka consumer, profile store, durable restart-safe
state store, event-time window, or clinical detector. The next clinical block
can consume `get(patient_id)` to perform its own analysis without changing the
ingestion contract.
