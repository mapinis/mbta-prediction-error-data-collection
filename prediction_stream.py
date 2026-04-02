"""
prediction_stream.py

Listen to and handle the prediction stream

open is meant to be run in its own thread
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sqlite3
import os

import httpx
from httpx_sse import connect_sse

from alerts import AlertEntity, check_alerts

logger = logging.getLogger(__name__)
update_trips_executor = ThreadPoolExecutor(
    max_workers=4, thread_name_prefix="update_trips_"
)


def update_trips(db_path: Path, trip_id: str):
    """
    Checks if trip_id is in trips table, gets and saves trip info if not

    Should lazily run in its own thread
    """

    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute("SELECT trip_id FROM trips WHERE trip_id=?", (trip_id,))

        if cursor.fetchone() is None:
            # missing, get trip data
            logger.info("New trip id: %s", trip_id)

            trip_response = httpx.get(
                f"https://api-v3.mbta.com/trips/{trip_id}",
                headers={"x-api-key": os.environ["MBTA_API_KEY"]},
                timeout=5,  # 5 seconds
            )

            trip_data = trip_response.raise_for_status().json()

            # insert or ignore to prevent race condition of two concurrent update_trip threads with same trip_id
            conn.execute(
                "INSERT OR IGNORE INTO trips VALUES(?, ?, ?, ?, ?)",
                (
                    trip_id,
                    trip_data["data"]["relationships"]["route"]["data"]["id"],
                    trip_data["data"]["attributes"]["headsign"],
                    trip_data["data"]["attributes"]["direction_id"],
                    datetime.now(timezone.utc).isoformat(),
                ),
            )


class PredictionStream:
    """
    Class to handle prediction streams
    """

    def __init__(self, db_path: Path):
        self._db_path = db_path

        if not self._db_path.is_file():
            raise ValueError(f"DB path {db_path} not a file that exists")

    def _handle_prediction_update(
        self, conn: sqlite3.Connection, prediction_update: dict
    ):
        """
        Handles a single prediction update
        """

        # get all the values needed from the update dictionary
        prediction_time = datetime.now(timezone.utc)

        recorded_at = prediction_time.isoformat()
        prediction_id = prediction_update["id"]

        trip_id = prediction_update["relationships"]["trip"]["data"]["id"]
        stop_id = prediction_update["relationships"]["stop"]["data"]["id"]
        direction_id = prediction_update["attributes"]["direction_id"]
        route_id = prediction_update["relationships"]["route"]["data"]["id"]

        logger.info("New prediction for trip %s", trip_id)

        # if the prediction is for the start of a route, arrival_time may be null
        # in this case, fall back to departure_time (which will only be null at last stop)
        raw_time = (
            prediction_update["attributes"]["arrival_time"]
            or prediction_update["attributes"]["departure_time"]
        )

        if raw_time is None:
            logger.warning(
                "No arrival or departure time for prediction %s, skipping",
                prediction_id,
            )
            return

        predicted_time = (
            datetime.fromisoformat(raw_time).astimezone(timezone.utc).isoformat()
        )

        schedule_relationship = (
            prediction_update["attributes"]["schedule_relationship"] or "SCHEDULED"
        )

        stop_sequence = prediction_update["attributes"]["stop_sequence"]

        # update trips as needed
        update_trips_executor.submit(update_trips, self._db_path, trip_id)

        # get active alerts for this prediction
        alert_tuples = check_alerts(
            AlertEntity(*(route_id, direction_id, stop_id, trip_id)), prediction_time
        )

        active_alert_ids = sorted([a[0] for a in alert_tuples])
        max_severity = max(alert_tuples, key=lambda a: a[1], default=["", None])[1]

        conn.execute(
            """INSERT INTO prediction_snapshots
            (prediction_id, trip_id, route_id, direction_id, stop_id,
            stop_sequence, predicted_time, schedule_relationship,
            recorded_at, active_alert_ids, max_alert_severity)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                prediction_id,
                trip_id,
                route_id,
                direction_id,
                stop_id,
                stop_sequence,
                predicted_time,
                schedule_relationship,
                recorded_at,
                json.dumps(active_alert_ids) if active_alert_ids else None,
                max_severity,
            ),
        )

    def _handle_arrival(self, conn: sqlite3.Connection, prediction_removal: dict):
        """
        Handle arrival of train to stop via prediction removal
        """

        resolved_at = datetime.now(timezone.utc).isoformat()
        prediction_id = prediction_removal["id"]

        res = conn.execute(
            """SELECT trip_id, route_id, direction_id, stop_id, schedule_relationship
            FROM prediction_snapshots
            WHERE prediction_id = ?
            ORDER BY recorded_at DESC
            LIMIT 1""",
            (prediction_id,),
        )

        data = res.fetchone()

        if not data:
            logger.warning(
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

        logger.info(
            "Logging arrival for trip %s, prediction %s", data[0], prediction_id
        )

        conn.execute(
            "INSERT OR IGNORE INTO arrivals VALUES (?, ?, ?, ?, ?, ?, ?)",
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

    def open(self):
        """
        Open the prediction stream and start listening for events

        Handles reset/add/update/remove events
        """

        # create DB connection
        with sqlite3.connect(self._db_path) as conn:
            # create httpx client
            with httpx.Client(timeout=httpx.Timeout(5.0, read=None)) as client:
                # connect
                with connect_sse(
                    client,
                    "GET",
                    "https://api-v3.mbta.com/predictions/?filter[route]=Orange,Red,Blue,Green-B,Green-C,Green-D,Green-E",
                    headers={
                        "Accept": "text/event-stream",
                        "x-api-key": os.environ["MBTA_API_KEY"],
                    },
                ) as event_source:

                    event_source.response.raise_for_status()

                    for sse in event_source.iter_sse():

                        match sse.event:
                            case "reset":
                                # data should be list of resources
                                pred_updates = sse.json()
                                for pred_update in pred_updates:
                                    self._handle_prediction_update(conn, pred_update)
                            case "add" | "update":
                                # data is singular resource
                                pred_update = sse.json()
                                self._handle_prediction_update(conn, pred_update)
                            case "remove":
                                # data is id
                                self._handle_arrival(conn, sse.json())
                            case _:
                                logger.warning(
                                    "Received event with unexpected type (%s), data: %s",
                                    sse.event,
                                    sse.data,
                                )

                        # commit after handling event
                        conn.commit()
