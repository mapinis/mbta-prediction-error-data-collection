"""
alerts.py

Holds the current alerts and the map of
"""

import threading
from typing import Dict, Tuple, Set, NamedTuple
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


# map of entities (route, direction, stop, trip) to set of (alert_id, severity) tuples
_entity_to_alerts_map: Dict[AlertEntity, Set[Tuple[str, int]]] = {}
_entity_to_alerts_map_lock = threading.Lock()

# removal events do not have the attributes, so this keeps track of those
# int is severity
_alerts_to_entities_map: Dict[str, Tuple[list[AlertEntity], int]] = {}
# does not need a lock, only used by alerts_stream
# thread when adding or removing alerts


def add_alert(alert_id: str, severity: int, entities: list[AlertEntity]):
    """
    Thread-safe alert add
    """

    with _entity_to_alerts_map_lock:
        _alerts_to_entities_map[alert_id] = (entities, severity)
        for entity in entities:
            _entity_to_alerts_map.setdefault(entity, set()).add((alert_id, severity))


def remove_alert(alert_id: str):
    """
    Thread-safe alert remove
    """

    with _entity_to_alerts_map_lock:
        if alert_id in _alerts_to_entities_map:
            entities, sev = _alerts_to_entities_map.pop(alert_id)
            for entity in entities:
                _entity_to_alerts_map[entity].discard((alert_id, sev))
                if not _entity_to_alerts_map[entity]:
                    _entity_to_alerts_map.pop(entity)


def check_alerts(entities: AlertEntity) -> Set[Tuple[str, int]]:
    """
    Thread-safe alert check
    Returns union set of all (alert, severity) pairs
    That these entities may apply to
    """

    alert_tuples = set()
    with _entity_to_alerts_map_lock:
        for mask in product([True, False], repeat=4):
            key = AlertEntity(*(v if use else None for use, v in zip(mask, entities)))
            alert_tuples |= _entity_to_alerts_map.get(key, set())

    return alert_tuples
