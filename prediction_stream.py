"""
prediction_stream.py

Listen to and handle the prediction stream

open_prediction_stream is meant to be run in its own thread
"""

from datetime import datetime
import json
import logging
import sqlite3
import os

import requests

from alerts import AlertEntities, check_alerts

logger = logging.getLogger()

MBTA_API_KEY = os.environ["MBTA_API_KEY"]


def update_trips(conn: sqlite3.Connection, trip_id: str):
    """
    Checks if trip_id is in trips table, gets and saves trip info if not
    """

    cursor = conn.execute("SELECT trip_id FROM trips WHERE trip_id=?", (trip_id,))

    if cursor.fetchone() is None:
        # missing, get trip data
        logger.info("New trip id: %s", trip_id)

        trip_response = requests.get(
            f"https://api-v3.mbta.com/trips/{trip_id}",
            headers={"x-api-key": MBTA_API_KEY},
            timeout=(1, 2),
        )

        trip_response.raise_for_status()

        trip_data = json.loads(trip_response.text)

        conn.execute(
            "INSERT INTO trips VALUES(?, ?, ?, ?, ?)",
            (
                trip_id,
                trip_data["relationships"]["route"]["data"]["id"],
                trip_data["attributes"]["headsign"],
                trip_data["attributes"]["direction_id"],
                datetime.now().isoformat(),
            ),
        )


def handle_prediction_update(conn: sqlite3.Connection, prediction_update: dict):
    """
    Handles a single prediction update
    """

    # get all the values needed from the update dictionary
    recorded_at = datetime.now().isoformat()
    prediction_id = prediction_update["id"]

    trip_id = prediction_update["relationships"]["trip"]["data"]["id"]
    stop_id = prediction_update["relationships"]["stop"]["data"]["id"]
    direction_id = prediction_update["attributes"]["direction_id"]
    route_id = prediction_update["relationships"]["route"]["data"]["id"]

    vehicle_id = prediction_update["relationships"]["vehicle"]["data"]["id"]

    predicted_arrival_time = prediction_update["attributes"]["arrival_time"]
    schedule_relationship = (
        prediction_update["attributes"]["schedule_relationship"] or "SCHEDULED"
    )

    stop_sequence = prediction_update["attributes"]["stop_sequence"]

    logger.info("New prediction for trip %s", trip_id)

    # update trips as needed
    # TODO threadpool executor
    update_trips(conn, trip_id)

    # get active alerts for this prediction
    alert_tuples = check_alerts(
        AlertEntities(*(route_id, direction_id, stop_id, trip_id))
    )

    active_alert_ids = json.dumps(sorted([a[0] for a in alert_tuples]))
    max_severity = max(alert_tuples, key=lambda a: a[1])[1]

    conn.execute(
        "INSERT INTO prediction_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            prediction_id,
            trip_id,
            route_id,
            direction_id,
            stop_id,
            stop_sequence,
            vehicle_id,
            predicted_arrival_time,
            schedule_relationship,
            recorded_at,
            active_alert_ids,
            max_severity,
        ),
    )


def handle_arrival(conn: sqlite3.Connection, prediction_removal: dict):
    """
    Handle arrival of train to stop via prediction removal
    """

    resolved_at = datetime.now().isoformat()
    prediction_id = prediction_removal["id"]

    res = conn.execute(
        """
SELECT trip_id, route_id, direction_id, stop_id, schedule_relationship
FROM prediction_snapshots
WHERE prediction_id = ?
ORDER BY recorded_at DESC
    """,
        (prediction_id),
    )

    data = res.fetchone()

    if not data:
        logger.error(
            "Prediction %s removed without any prediction snapshots recorded",
            prediction_id,
        )
        return

    # match schedule_resolution to resolution_type
    match data[4]:
        case "SCHEDULED":
            resolution_type = "arrived"
        case "ADDED":
            resolution_type = "arrived"
        case "CANCELLED":
            resolution_type = "cancelled"
        case "SKIPPED":
            resolution_type = "skipped"
        case _:
            resolution_type = "other"

    conn.execute(
        "INSERT INTO arrivals VALUES(?, ?, ?, ?, ?, ?, ?)",
        (
            prediction_id,
            data[0],
            data[1],
            data[2],
            data[3],
            resolved_at,
            resolution_type,
        ),
    )


def open_prediction_stream():
    """
    Open the prediction stream and start listening for events

    Handles reset/add/update/remove events
    """
