import numpy as np


class DeteriorationDetector:

    def calculate(self, state):

        readings = list(state["readings"])

        if len(readings) < 5:
            return {
                "score": 0,
                "signals": [],
                "trajectory": "INSUFFICIENT_DATA"
            }

        recent = readings[-5:]

        signals = []

        # -----------------------
        # Heart Rate
        # -----------------------

        hr_values = [
            r["heart_rate"]
            for r in recent
        ]

        hr_slope = np.polyfit(
            range(len(hr_values)),
            hr_values,
            1
        )[0]

        if hr_slope > 1.0:
            signals.append("heart_rate_rising")

        # -----------------------
        # SpO2
        # -----------------------

        spo2_values = [
            r["spo2"]
            for r in recent
        ]

        spo2_slope = np.polyfit(
            range(len(spo2_values)),
            spo2_values,
            1
        )[0]

        if spo2_slope < -0.25:
            signals.append("spo2_falling")

        # -----------------------
        # Respiratory Rate
        # -----------------------

        rr_values = [
            r["respiratory_rate"]
            for r in recent
        ]

        rr_slope = np.polyfit(
            range(len(rr_values)),
            rr_values,
            1
        )[0]

        if rr_slope > 0.4:
            signals.append("respiratory_rate_rising")

        # -----------------------
        # Blood Pressure
        # -----------------------

        bp_values = [
            r["systolic_bp"]
            for r in recent
        ]

        bp_slope = np.polyfit(
            range(len(bp_values)),
            bp_values,
            1
        )[0]

        if bp_slope < -1.0:
            signals.append("blood_pressure_falling")

        # -----------------------
        # Multi-signal Score
        # -----------------------

        signal_count = len(signals)

        score = signal_count * 20

        if signal_count >= 3:
            score += 20

        if signal_count == 4:
            score += 10

        score = min(score, 100)

        # -----------------------
        # Trajectory
        # -----------------------

        if score >= 70:
            trajectory = "RAPIDLY_WORSENING"

        elif score >= 40:
            trajectory = "WORSENING"

        elif score >= 20:
            trajectory = "EMERGING"

        else:
            trajectory = "STABLE"

        return {
            "score": score,
            "signals": signals,
            "trajectory": trajectory
        }