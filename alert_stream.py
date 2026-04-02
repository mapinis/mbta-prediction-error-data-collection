"""
alert_stream.py

Listen to and handle the alert stream

open is meant to be run in its own thread
"""

import logging
import os
from datetime import datetime, timezone

import httpx
from httpx_sse import connect_sse

from alerts import AlertEntity, add_alert, remove_alert

MBTA_API_KEY = os.environ["MBTA_API_KEY"]

logger = logging.getLogger(__name__)


class AlertStream:
    """
    Class to handle alerts streams
    """

    def _handle_add_alert(self, alert_resource_event: dict):
        """
        Handles a single alert add with multiple possible informed entities
        """

        # there may be many possible informed_entities per alert
        alert_id = alert_resource_event["id"]
        severity = alert_resource_event["attributes"]["severity"]

        alert_entities = [
            AlertEntity(
                route=ie.get("route", None),
                direction=ie.get("direction_id", None),
                stop=ie.get("stop", None),
                trip=ie.get("trip", None),
            )
            for ie in alert_resource_event["attributes"]["informed_entity"]
        ]

        periods = [
            (
                (
                    datetime.fromisoformat(p["start"])
                    if p["start"]
                    else datetime.min.replace(tzinfo=timezone.utc)
                ),
                (
                    datetime.fromisoformat(p["end"])
                    if p["end"]
                    else datetime.max.replace(tzinfo=timezone.utc)
                ),
            )
            for p in alert_resource_event["attributes"]["active_period"]
        ]

        add_alert(alert_id, severity, alert_entities, periods)

    def _handle_update_alert(self, alert_resource_event: dict):
        """
        Handles an alert update (removes old and adds new)
        """

        alert_id = alert_resource_event["id"]
        logger.info("Updating alert %s", alert_id)
        self._handle_remove_update(alert_resource_event)
        self._handle_add_alert(alert_resource_event)

    def _handle_remove_update(self, alert_id_event: dict):
        """
        Handle a single alert remove
        """

        alert_id = alert_id_event["id"]
        logger.info("Removing alert %s", alert_id)

        remove_alert(alert_id)

    def open(self):
        """
        Open the alert stream and start listening for events

        Handles reset/add/update/remove events
        """

        # create httpx client
        with httpx.Client() as client:
            # connect
            with connect_sse(
                client,
                "GET",
                "https://api-v3.mbta.com/alerts/?filter[route]=Orange,Red,Blue,Green-B,Green-C,Green-D,Green-E",
                headers={"Accept": "text/event-stream", "x-api-key": MBTA_API_KEY},
            ) as event_source:

                event_source.response.raise_for_status()

                for sse in event_source.iter_sse():

                    match sse.event:
                        case "reset":
                            # data should be list of resources
                            alert_updates = sse.json()
                            for alert_add in alert_updates:
                                self._handle_add_alert(alert_add)
                        case "add":
                            # data is singular resource
                            alert_add = sse.json()
                            self._handle_add_alert(alert_add)
                        case "update":
                            alert_update = sse.json()
                            self._handle_update_alert(alert_update)
                        case "remove":
                            # data is id
                            self._handle_remove_update(sse.json())
                        case _:
                            logger.warning(
                                "Received event with unexpected type (%s), data: %s",
                                sse.event,
                                sse.data,
                            )
