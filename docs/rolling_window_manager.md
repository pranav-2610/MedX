# Rolling window manager

`PatientStateStore` retains a bounded **event-count** history. The separate
`RollingWindowManager` answers a different technical question: which retained
observations fall into this patient's current **event-time** horizon?

```text
PatientStateStore.update(event)       -> bounded recent history
RollingWindowManager.update(event)    -> bounded-by-time analysis window
```

## Time semantics

Construct it with a configurable `timedelta` `W`:

```python
windows = RollingWindowManager(window_duration=timedelta(minutes=30))
window = windows.update(event)
```

For each patient, the anchor is the largest `event.event_time` observed so far.
The current window contains exactly the events satisfying:

```text
anchor_time - W < event.event_time <= anchor_time
```

Therefore an event exactly `W` old is excluded. The manager uses
`event_time` only; `ingested_at`, wall-clock time, and Kafka arrival time have
no role. When a newer observation advances an anchor, expired observations are
removed. A delayed event cannot move the anchor backward; it is retained only
when it falls inside the existing horizon. The output preserves source arrival
order rather than applying a clinical or sequence-number ordering decision.

`get_window(patient_id)` returns a defensive tuple snapshot or `None` for an
unseen patient. The component owns independent `deque` instances per patient,
and it copies events on input/output to prevent public mutation of its state.

This is a temporal abstraction only. It does not validate raw data, use Kafka,
look up profiles, persist state, score risk, or infer clinical meaning. It
ignores a retry of an event ID that is already retained in its local window;
later clinical analysis can consume the returned window directly.
