from datetime import timezone
from pathlib import Path
import sys
import unittest
from uuid import UUID

from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from models import VitalEvent


def valid_payload() -> dict[str, object]:
    return {
        "event_id": "4d5e6f70-1234-4abc-8def-1234567890ab",
        "patient_id": "P001",
        "event_time": "2026-09-19T10:15:00+05:30",
        "heart_rate": 82.0,
        "spo2": 97.0,
        "respiratory_rate": 16.0,
        "systolic_bp": 125.0,
        "diastolic_bp": 78.0,
        "sequence_number": 1,
        "source": "simulator",
    }


class VitalEventContractTests(unittest.TestCase):
    def test_valid_event_is_accepted_and_normalized_to_utc(self) -> None:
        event = VitalEvent.model_validate(valid_payload())

        self.assertEqual(event.patient_id, "P001")
        self.assertEqual(event.event_time.tzinfo, timezone.utc)
        self.assertEqual(event.event_time.isoformat(), "2026-09-19T04:45:00+00:00")

    def test_missing_required_measurement_is_rejected(self) -> None:
        payload = valid_payload()
        del payload["spo2"]

        with self.assertRaises(ValidationError):
            VitalEvent.model_validate(payload)

    def test_explicitly_missing_measurement_is_rejected(self) -> None:
        payload = valid_payload()
        payload["spo2"] = None

        with self.assertRaisesRegex(ValidationError, "explicitly missing"):
            VitalEvent.model_validate(payload)

    def test_invalid_field_type_is_rejected(self) -> None:
        payload = valid_payload()
        payload["heart_rate"] = "82"

        with self.assertRaises(ValidationError):
            VitalEvent.model_validate(payload)

    def test_invalid_physiological_values_are_rejected(self) -> None:
        payload = valid_payload()
        payload["spo2"] = 101.0

        with self.assertRaises(ValidationError):
            VitalEvent.model_validate(payload)

        payload = valid_payload()
        payload["diastolic_bp"] = 130.0
        payload["systolic_bp"] = 120.0

        with self.assertRaises(ValidationError):
            VitalEvent.model_validate(payload)

    def test_naive_timestamp_is_rejected(self) -> None:
        payload = valid_payload()
        payload["event_time"] = "2026-09-19T10:15:00"

        with self.assertRaisesRegex(ValidationError, "timezone offset"):
            VitalEvent.model_validate(payload)

    def test_canonical_field_names_and_types_are_preserved(self) -> None:
        self.assertEqual(
            list(VitalEvent.model_fields),
            [
                "event_id",
                "patient_id",
                "event_time",
                "heart_rate",
                "spo2",
                "respiratory_rate",
                "systolic_bp",
                "diastolic_bp",
                "sequence_number",
                "source",
            ],
        )
        event = VitalEvent.model_validate(valid_payload())
        self.assertIsInstance(event.event_id, UUID)
        self.assertIsInstance(event.event_time.tzinfo, timezone)
        self.assertIsInstance(event.heart_rate, float)
        self.assertIsInstance(event.sequence_number, int)


if __name__ == "__main__":
    unittest.main()
