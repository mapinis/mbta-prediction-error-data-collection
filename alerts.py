"""
alerts.py

Holds the current alerts and the map of
"""

import threading
from datetime import datetime
from typing import Dict, Set, NamedTuple
from itertools import product


class AlertEntity(NamedTuple):
    """
    NamedTuple to hold alert entities
    (what the alerts apply to)

    None means that this alert does not filter by this entity
    """

    route: str | None
    direction: int | None
    stop: str | None
    trip: str | None


_alerts_lock = threading.Lock()

# map of entities (route, direction, stop, trip) to set of alert_ids
_entity_to_alerts_map: Dict[AlertEntity, Set[str]] = {}

# removal events do not have the attributes, so this keeps track of those
_alerts_to_entities_map: Dict[
    str, tuple[list[AlertEntity], int, list[tuple[datetime, datetime]]]
] = {}


def add_alert(
    alert_id: str,
    severity: int,
    entities: list[AlertEntity],
    active_periods: list[tuple[datetime, datetime]],
):
    """
    Thread-safe alert add
    """

    with _alerts_lock:
        _alerts_to_entities_map[alert_id] = (entities, severity, active_periods)
        for entity in entities:
            _entity_to_alerts_map.setdefault(entity, set()).add(alert_id)


def remove_alert(alert_id: str):
    """
    Thread-safe alert remove
    """

    with _alerts_lock:
        if alert_id in _alerts_to_entities_map:
            entities, *_ = _alerts_to_entities_map.pop(alert_id)
            for entity in entities:
                _entity_to_alerts_map[entity].discard(alert_id)
                if not _entity_to_alerts_map[entity]:
                    _entity_to_alerts_map.pop(entity)


def check_alerts(entities: AlertEntity, time: datetime) -> Set[tuple[str, int]]:
    """
    Thread-safe alert check
    Returns union set of all (alert_id, severity) pairs that apply to these
    entities and are currently within an active period
    """

    alert_ids: set[str] = set()
    with _alerts_lock:
        for mask in product([True, False], repeat=4):
            key = AlertEntity(*(v if use else None for use, v in zip(mask, entities)))
            alert_ids |= _entity_to_alerts_map.get(key, set())

        return {
            (alert_id, _alerts_to_entities_map[alert_id][1])
            for alert_id in alert_ids
            if any(
                start <= time <= end
                for start, end in _alerts_to_entities_map[alert_id][2]
            )
        }
