import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from patient_profiles import PatientProfileRepository, ProfileLoadError


PROFILE_PATH = Path(__file__).resolve().parents[1] / "data" / "patients.json"


class PatientProfileRepositoryTests(unittest.TestCase):
    def write_document(self, document: object) -> Path:
        handle = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        with handle:
            json.dump(document, handle)
        self.addCleanup(Path(handle.name).unlink)
        return Path(handle.name)

    def test_multiple_profiles_load_and_lookup_by_patient_id(self) -> None:
        repository = PatientProfileRepository.from_json_file(PROFILE_PATH)

        self.assertEqual(len(repository), 4)
        profile = repository.require("P003")
        self.assertEqual(profile.patient_id, "P003")
        self.assertEqual(profile.age, 78)
        self.assertIn("chronic kidney disease", profile.relevant_history)
        self.assertEqual(repository.get("unknown"), None)

    def test_invalid_profile_data_fails_without_partial_repository(self) -> None:
        path = self.write_document(
            {
                "profiles": [
                    {
                        "patient_id": "P001",
                        "age": 40,
                        "relevant_history": [],
                        "current_medications": [],
                        "labs": {},
                    },
                    {
                        "patient_id": "P002",
                        "age": -1,
                        "relevant_history": [],
                        "current_medications": [],
                        "labs": {},
                    },
                ]
            }
        )

        with self.assertRaisesRegex(ProfileLoadError, "invalid profile at index 1"):
            PatientProfileRepository.from_json_file(path)

    def test_duplicate_patient_ids_are_rejected(self) -> None:
        profile = {
            "patient_id": "P001",
            "age": 40,
            "relevant_history": [],
            "current_medications": [],
            "labs": {},
        }
        path = self.write_document({"profiles": [profile, profile]})

        with self.assertRaisesRegex(ProfileLoadError, "duplicate profile"):
            PatientProfileRepository.from_json_file(path)

    def test_malformed_json_is_rejected(self) -> None:
        handle = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        with handle:
            handle.write("{not valid JSON")
        self.addCleanup(Path(handle.name).unlink)

        with self.assertRaisesRegex(ProfileLoadError, "malformed JSON"):
            PatientProfileRepository.from_json_file(handle.name)

    def test_require_raises_for_unknown_patient(self) -> None:
        repository = PatientProfileRepository.from_json_file(PROFILE_PATH)

        with self.assertRaisesRegex(KeyError, "no static profile"):
            repository.require("P999")


if __name__ == "__main__":
    unittest.main()
