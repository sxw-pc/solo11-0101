from datetime import timedelta


def corrected_time(observed, anchors):
    prior = [offset for time, offset in anchors if time <= observed]
    if not prior:
        return None
    return observed - timedelta(seconds=prior[-1])


def local_hour(corrected, zone):
    return corrected.astimezone(zone).replace(minute=0, second=0, microsecond=0)
