# Static patient profile contract

`PatientProfile` is the static clinical context for one patient. It is loaded
once at application startup from `data/patients.json`; it is not a live event
and is never embedded in `VitalEvent`.

```json
{
  "profiles": [
    {
      "patient_id": "P001",
      "age": 67,
      "relevant_history": ["hypertension"],
      "current_medications": ["amlodipine"],
      "labs": {"creatinine_mg_dl": 1.0}
    }
  ]
}
```

Every profile has a non-empty `patient_id`, integer age from 0 through 130,
arrays for relevant history and current medications, and a laboratory-value
object. Lists may be empty when no information is available. Lab keys include
their units (for example `creatinine_mg_dl`) and values must be finite numbers.

`PatientProfileRepository.from_json_file(path)` validates the entire document
before returning a repository. It fails fast with `ProfileLoadError` for unreadable
or malformed JSON, an invalid profile, an unexpected top-level shape, or a
duplicate `patient_id`; no partial dataset is returned. Downstream components
use `get(patient_id)` for an optional lookup or `require(patient_id)` when a
profile must be present. Patient identifiers use the same non-empty string
contract as `VitalEvent.patient_id`.
