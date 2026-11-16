from datetime import timedelta


def corrected_time(observed, anchors):
    prior = [offset for time, offset in anchors if time <= observed]
    if not prior:
        return None
    return observed - timedelta(seconds=prior[-1])


def local_hour(corrected, zone):
    return corrected.astimezone(zone).replace(minute=0, second=0, microsecond=0)


def _intervals_intersect(a_start, a_end, b_start, b_end):
    return a_start < b_end and b_start < a_end


def _intersection(a_start, a_end, b_start, b_end):
    lo = max(a_start, b_start)
    hi = min(a_end, b_end)
    if lo < hi:
        return lo, hi
    return None


def drift_bounds(observed, anchors, max_drift_seconds):
    """UTC envelope [earliest, latest] of an observed device clock time.

    The device clock is corrected with the most recent anchor whose observed
    time is not later than the event; when that anchor is itself uncertain by
    up to ``max_drift_seconds`` (the accepted spread of anchor offsets), the
    true UTC instant may lie anywhere inside the returned half-open window.
    Anchors at or before ``observed`` are required, mirroring corrected_time.
    """
    prior = [(time, offset) for time, offset in anchors if time <= observed]
    if not prior:
        return None
    anchor_time, offset = prior[-1]
    nominal = observed - timedelta(seconds=offset)
    spread = timedelta(seconds=max_drift_seconds)
    return nominal - spread, nominal + spread


def anchor_spread_ok(anchors, max_drift_seconds):
    if len(anchors) < 2:
        return True
    offsets = [offset for _, offset in anchors]
    return max(offsets) - min(offsets) <= max_drift_seconds


def maintenance_portion(window_start, window_end, maintenance):
    """Total length of [window_start, window_end) covered by [m_start, m_end)
    maintenance windows. Overlapping windows are unioned first so their
    overlap is never deducted twice."""
    hits = [seg for seg in maintenance if _intervals_intersect(window_start, window_end, *seg)]
    if not hits:
        return timedelta(0)
    hits.sort()
    merged = []
    cur_lo, cur_hi = hits[0]
    for lo, hi in hits[1:]:
        if lo <= cur_hi:
            if hi > cur_hi:
                cur_hi = hi
        else:
            merged.append((cur_lo, cur_hi))
            cur_lo, cur_hi = lo, hi
    merged.append((cur_lo, cur_hi))
    total = timedelta(0)
    for lo, hi in merged:
        cut = _intersection(window_start, window_end, lo, hi)
        if cut:
            total += cut[1] - cut[0]
    return total


def touched_local_hours(earliest, latest, zone):
    """Every station-local hour that any instant in [earliest, latest) could
    fall into. The uncertain window is allocated across all of them instead
    of being bucketed at a single guessed time."""
    hours = []
    cursor = local_hour(earliest, zone)
    last = local_hour(latest - timedelta(microseconds=1), zone) if latest > earliest else local_hour(latest, zone)
    while cursor <= last:
        hours.append(cursor)
        cursor += timedelta(hours=1)
    return hours


def hour_utc_span(hour_local, zone):
    """UTC [start, end) of a station-local hour bucket."""
    start = hour_local.astimezone(zone.utc)
    return start, start + timedelta(hours=1)


def gap_windows(earliest, latest, maintenance, extra_cuts=()):
    """Split an uncertainty window into (start, end, inside_maintenance)
    half-open segments at maintenance boundaries, local-hour boundaries and
    any extra cut points."""
    cuts = {earliest, latest}
    for point in extra_cuts:
        if earliest < point < latest:
            cuts.add(point)
    for m_start, m_end in maintenance:
        if earliest < m_start < latest:
            cuts.add(m_start)
        if earliest < m_end < latest:
            cuts.add(m_end)
    ordered = sorted(cuts)
    segments = []
    for lo, hi in zip(ordered, ordered[1:]):
        if hi <= lo:
            continue
        mid = lo + (hi - lo) / 2
        inside = any(m_start <= mid < m_end for m_start, m_end in maintenance)
        segments.append((lo, hi, inside))
    return segments
