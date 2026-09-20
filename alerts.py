import time 

alert_cooldown = 60

def should_alert(state, detection) : 
    now = time.time()
    if detection["score"] < 70:
        return False

    if state["active_alert"]:
        return False

    if state["last_alert_time"]:
        if now - state["last_alert_time"] < alert_cooldown:
            return False

    return True