from collections import deque


class PatientStateEngine:

    def __init__(self, max_history=60):
        self.states = {}
        self.max_history = max_history

    def update(self, reading):
        patient_id = reading["patient_id"]

        if patient_id not in self.states:
            self.states[patient_id] = {
                "patient_id": patient_id,

                "readings": deque(maxlen=self.max_history),

                "baseline": {
                    "heart_rate": reading["heart_rate"],
                    "spo2": reading["spo2"],
                    "respiratory_rate": reading["respiratory_rate"],
                    "systolic_bp": reading["systolic_bp"]
                },

                "latest": reading,

                "risk": 0,

                "risk_level": "STABLE",

                "active_alert": False,

                "last_alert_time": None
            }

        state = self.states[patient_id]

        state["readings"].append(reading)

        state["latest"] = reading

        return state

    def get(self, patient_id):
        return self.states.get(patient_id)

    def get_all(self):
        return list(self.states.values())