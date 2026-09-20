from fastapi import FastAPI
from pathlib import Path

from simulator import PatientSimulator
from state import PatientStateEngine
from detector import DeteriorationDetector
from alerts import should_alert
from patient_profiles import PatientProfileRepository


app = FastAPI(
    title="Sentinel Clinical Deterioration Copilot"
)


state_engine = PatientStateEngine()

detector = DeteriorationDetector()

# Static context is validated and loaded once as the application starts.
profile_repository = PatientProfileRepository.from_json_file(
    Path(__file__).parent / "data" / "patients.json"
)


patients = [

    PatientSimulator("P001", "stable"),

    PatientSimulator("P002", "noise"),

    PatientSimulator("P003", "deteriorating"),

    PatientSimulator("P004", "stable_abnormal")
]


@app.get("/")
def root():

    return {
        "system": "MedX",
        "status": "running"
    }


@app.post("/tick")
def tick():

    results = []

    for simulator in patients:

        reading = simulator.step()

        state = state_engine.update(reading)

        detection = detector.calculate(state)

        state["risk"] = detection["score"]

        state["risk_level"] = (
            "HIGH"
            if detection["score"] >= 70
            else
            "MEDIUM"
            if detection["score"] >= 40
            else
            "WATCH"
            if detection["score"] >= 20
            else
            "STABLE"
        )

        alert = should_alert(
            state,
            detection
        )

        if alert:
            state["active_alert"] = True

            results.append({
                "patient_id": simulator.patient_id,
                "alert": True,
                "detection": detection
            })

        else:
            results.append({
                "patient_id": simulator.patient_id,
                "alert": False,
                "detection": detection
            })
    return results


@app.get("/patients")
def patients_state():
    return state_engine.get_all()
