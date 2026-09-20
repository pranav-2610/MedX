# VitalEvent ingestion contract

`VitalEvent` is the only canonical observation event for the vitals ingestion
pipeline. Every producer, broker message, consumer, validator, store, and
patient-state component must use these exact field names and meanings.

```text
VitalEvent(
  event_id, patient_id, event_time,
  heart_rate, spo2, respiratory_rate, systolic_bp, diastolic_bp,
  sequence_number, source
)
```

The Pydantic implementation is in `models.py`. It accepts no extra fields.

## Fields and units

| Field | Type | Requirement and unit |
| --- | --- | --- |
| `event_id` | UUID | Required unique identifier for this individual observation event. The producer generates it once; transport retries and archive replay intentionally preserve it so idempotency layers can recognise the same observation. This contract does not prescribe a distributed ID system. |
| `patient_id` | string | Required, non-empty stable patient identifier. |
| `event_time` | timezone-aware datetime | Required observation time, encoded on the wire as ISO 8601/RFC 3339 with an offset (for example `2026-09-19T10:15:00Z`). It is normalized to UTC by the model. It is never the application receive/ingest time. |
| `heart_rate` | float | Required, beats per minute (bpm), inclusive range 0-300. |
| `spo2` | float | Required, peripheral oxygen saturation, percent (%), inclusive range 0-100. |
| `respiratory_rate` | float | Required, breaths per minute (breaths/min), inclusive range 0-120. |
| `systolic_bp` | float | Required, systolic arterial blood pressure, millimetres of mercury (mmHg), inclusive range 0-300. |
| `diastolic_bp` | float | Required, diastolic arterial blood pressure, mmHg, inclusive range 0-200 and not greater than `systolic_bp`. |
| `sequence_number` | integer | Required positive per-patient source sequence. A larger value represents a later observation in the source's logical patient order. The current ingestion boundary preserves it but does not enforce cross-event monotonicity, because delayed and out-of-order source delivery is accepted. |
| `source` | string | Required, non-empty origin label, such as `simulator`, `bedside_monitor`, or a future namespaced source identifier. It is intentionally extensible and not an enum. |

## Missing, invalid, and delayed data

All five physiological measurements are mandatory. A `VitalEvent` represents a
complete vital observation set, so partial events are not valid contract
instances.

- A valid measurement is a finite numeric value within the documented range.
- An explicitly missing measurement is represented by `null` at the producer
  boundary and is rejected with an explicit missing-measurement validation
  error. An omitted measurement is likewise rejected as a required field.
- An invalid measurement (wrong type, non-finite number, out-of-range number,
  or an inconsistent BP pair) is rejected; it is never coerced into a valid
  value.

A producer that cannot provide all five measurements must emit no `VitalEvent`
for that observation. A future quality/telemetry contract may record the
missingness separately, but it must not overload this event or invent a partial
vitals format.

`event_time` deliberately records when the source observed the vitals. A later
ingestion block may separately record receive time and use per-patient
`sequence_number` plus event time to handle delayed or out-of-order arrival.
This contract does not reorder events.
