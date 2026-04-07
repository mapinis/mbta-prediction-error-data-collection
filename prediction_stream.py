"""
prediction_stream.py

Listen to and handle the prediction stream

open is meant to be run in its own thread
"""

from concurrent.futures import ThreadPoolExecutor  # pylint: disable=no-name-in-module
from datetime import datetime, timezone
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
                "INSERT OR IGNORE INTO trips VALUES (?, ?, ?, ?, ?)",
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

    def _handle_prediction_add(
        self, conn: sqlite3.Connection, prediction_add: dict, prediction_time: datetime
    ):
        """
        Handle a single prediction add
        prediction_add dict is resource shape

        Checks for new trip
        Adds row to prediction_trips then just calls the update code
        """

        prediction_id = prediction_add["id"]
        trip_id = prediction_add["relationships"]["trip"]["data"]["id"]

        logger.info("New prediction %s for trip %s", prediction_id, trip_id)

        # update trips as needed (if new trip)
        update_trips_executor.submit(update_trips, self._db_path, trip_id)

        # update prediction_trips mapping table
        conn.execute(
            "INSERT OR IGNORE INTO prediction_trips VALUES (?, ?)",
            (prediction_id, trip_id),
        )

        self._handle_prediction_update(conn, prediction_add, prediction_time)

    def _handle_prediction_update(
        self,
        conn: sqlite3.Connection,
        prediction_update: dict,
        prediction_time: datetime,
    ):
        """
        Handles a single prediction update
        """

        # get all the values needed from the update dictionary
        recorded_at = prediction_time.isoformat()
        prediction_id = prediction_update["id"]

        trip_id = prediction_update["relationships"]["trip"]["data"]["id"]
        stop_id = prediction_update["relationships"]["stop"]["data"]["id"]
        direction_id = prediction_update["attributes"]["direction_id"]
        route_id = prediction_update["relationships"]["route"]["data"]["id"]

        logger.info("Updated prediction %s for trip %s", prediction_id, trip_id)

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

        # get active alerts for this prediction
        alert_tuples = check_alerts(
            AlertEntity(*(route_id, direction_id, stop_id, trip_id)), prediction_time
        )

        alert_count = len(alert_tuples)
        max_severity = max(a[1] for a in alert_tuples) if alert_count > 0 else None

        conn.execute(
            """INSERT INTO prediction_snapshots
            (prediction_id, stop_id,
            stop_sequence, predicted_time, schedule_relationship,
            recorded_at, active_alert_count, max_alert_severity)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                prediction_id,
                stop_id,
                stop_sequence,
                predicted_time,
                schedule_relationship,
                recorded_at,
                alert_count,
                max_severity,
            ),
        )

    def _handle_arrival(
        self, conn: sqlite3.Connection, prediction_removal: dict, arrival_time: datetime
    ):
        """
        Handle arrival of train to stop via prediction removal
        """

        resolved_at = arrival_time.isoformat()
        prediction_id = prediction_removal["id"]

        res = conn.execute(
            """SELECT pt.trip_id, ps.stop_id, ps.schedule_relationship
            FROM prediction_snapshots ps
            JOIN prediction_trips pt ON pt.prediction_id = ps.prediction_id
            WHERE ps.prediction_id = ?
            ORDER BY ps.recorded_at DESC
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
        match data[2]:
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
            "INSERT OR IGNORE INTO arrivals VALUES (?, ?, ?, ?, ?)",
            (
                prediction_id,
                data[0],
                data[1],
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

                        event_time = datetime.now(timezone.utc)
                        json_data = sse.json()

                        match sse.event:
                            case "reset":
                                # data should be list of resources
                                for pred_update in json_data:
                                    self._handle_prediction_add(
                                        conn, pred_update, event_time
                                    )
                            case "add":
                                # data is singular resource
                                self._handle_prediction_add(conn, json_data, event_time)
                            case "update":
                                # data is singular resource
                                self._handle_prediction_update(
                                    conn, json_data, event_time
                                )
                            case "remove":
                                # data is id
                                self._handle_arrival(conn, json_data, event_time)
                            case _:
                                logger.warning(
                                    "Received event with unexpected type (%s), data: %s",
                                    sse.event,
                                    sse.data,
                                )

                        # commit after handling event
                        conn.commit()
