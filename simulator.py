import random
import time


class PatientSimulator:

    def __init__(self, patient_id, scenario="stable"):
        self.patient_id = patient_id
        self.scenario = scenario

        self.t = 0

        self.hr = 82
        self.spo2 = 97
        self.rr = 16
        self.sbp = 125
        self.dbp = 78

    def step(self):

        self.t += 1

        if self.scenario == "stable":

            self.hr += random.gauss(0, 1)
            self.spo2 += random.gauss(0, 0.2)
            self.rr += random.gauss(0, 0.3)
            self.sbp += random.gauss(0, 1)

        elif self.scenario == "noise":

            self.hr += random.gauss(0, 3)

            # Everything else remains stable
            self.spo2 += random.gauss(0, 0.2)
            self.rr += random.gauss(0, 0.3)
            self.sbp += random.gauss(0, 1)

        elif self.scenario == "deteriorating":

            self.hr += 0.8 + random.gauss(0, 0.5)

            self.spo2 -= 0.35 + random.gauss(0, 0.1)

            self.rr += 0.5 + random.gauss(0, 0.15)

            self.sbp -= 1.5 + random.gauss(0, 0.5)

        elif self.scenario == "stable_abnormal":

            self.hr = 105 + random.gauss(0, 2)

            self.spo2 = 94 + random.gauss(0, 0.3)

            self.rr = 20 + random.gauss(0, 0.5)

            self.sbp = 120 + random.gauss(0, 2)

        return {
            "timestamp": time.time(),
            "patient_id": self.patient_id,

            "heart_rate": round(self.hr, 1),
            "spo2": round(self.spo2, 1),
            "respiratory_rate": round(self.rr, 1),

            "systolic_bp": round(self.sbp, 1),
            "diastolic_bp": round(self.dbp, 1)
        }
    