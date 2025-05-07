"""Tools for working with Google Cloud Datastore.

This module handles all Datastore operations for storing and retrieving task entries.
It provides functionality to manage task entries in Datastore with features like
filtering, ordering, and updating task information.

Typical usage example:

    db = DB("task_kind", db_secret_location="/path/to/credentials.json")
    client = db.client
    db.handle_task(client, {
        "dag_id": "my_dag",
        "run_id": "run_123",
        "airflow_task_id": "my_task"
    })
"""

from __future__ import annotations

import datetime
import json
import logging
import os
from operator import itemgetter
from typing import TYPE_CHECKING, Any, ClassVar

from google.cloud import datastore

from dataeng_container_tools.modules import BaseModule

if TYPE_CHECKING:
    from pathlib import Path

    from dataeng_container_tools import CommandLineArguments

logger = logging.getLogger("Container Tools")


class DB(BaseModule):
    """Handles all Google Cloud Datastore operations.

    This class provides methods for interacting with Google Cloud Datastore,
    including querying, creating, and updating task entries.

    Attributes:
        MODULE_NAME: Identifies the module type for logging and display.
        DEFAULT_SECRET_PATHS: Default secret file paths for Datastore credentials.
        current_task_kind: The kind of task entries this instance will handle.
        client: The Datastore client instance.
    """

    MODULE_NAME: ClassVar[str] = "DB"
    DEFAULT_SECRET_PATHS: ClassVar[dict[str, str]] = {"DB": "/vault/secrets/gcp-credentials.json"}

    def __init__(
        self,
        task_kind: str,
        gcp_secret_location: str | Path | None = None,
    ) -> None:
        """Initialize the DB module.

        Args:
            task_kind: The kind of task entries this instance will handle.
            gcp_secret_location: The location of the secret file
                associated with Datastore.
        """
        override_secret_paths = {self.MODULE_NAME: gcp_secret_location} if gcp_secret_location else None
        super().__init__(override_secret_paths=override_secret_paths, local=False)

        self.current_task_kind = task_kind

        # Get the secret path for Datastore
        secret_path = self.secret_paths[self.MODULE_NAME]
        if not secret_path.exists():
            msg = f"Datastore credentials not found at {secret_path!s}"
            raise FileNotFoundError(msg)

        # Load credentials and create client
        credentials = json.loads(secret_path.read_text())

        self.client = datastore.Client.from_service_account_info(credentials)
        logger.info("Datastore client initialized successfully")

    def get_task_entry(
        self,
        client: datastore.Client,
        filter_map: dict[str, Any],
        kind: str,
        order_task_entries_params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Query task entries based on filter criteria.

        Args:
            client: Datastore client instance.
            filter_map: Dictionary of filter criteria (key-value pairs).
            kind: The kind of entities to query.
            order_task_entries_params: Optional parameters for ordering task entries.
                Should contain 'order_by_key_list' (list of keys to order by) and
                'descending_order' (boolean) keys.

        Returns:
            List of task entries matching the filter criteria.

        Raises:
            Exception: If ordering keys are not present in entries or other query errors.
        """
        # Create and execute query with filters
        query = client.query(kind=kind)
        for key, value in filter_map.items():
            query.add_filter(key, "=", value)

        entries = list(query.fetch())

        # Apply ordering if specified and multiple entries exist
        if len(entries) > 1 and order_task_entries_params is not None:
            order_keys = order_task_entries_params["order_by_key_list"]

            # Validate that all ordering keys exist in entries
            for key in order_keys:
                for entry in entries:
                    if key not in entry:
                        msg = f"No element for key: {key} present in fetched entry. Cannot order entries by key: {key}"
                        raise ValueError(msg)

            # Sort entries based on specified keys and order
            entries = sorted(
                entries,
                key=itemgetter(*order_keys),
                reverse=order_task_entries_params["descending_order"],
            )

        return entries

    def put_snapshot_task_entry(
        self,
        client: datastore.Client,
        task_entry: datastore.Entity,
        params: dict[str, Any],
    ) -> None:
        """Store or update a task entry in Datastore.

        Args:
            client: Datastore client instance.
            task_entry: Entity which stores the actual instance of data.
            params: Dictionary containing parameters (key-value pairs) to store.
        """
        # Update task entry with provided parameters
        for key, value in params.items():
            task_entry[key] = value

        # Update modification timestamp
        task_entry["modified_at"] = datetime.datetime.now(datetime.timezone.utc)

        # Store entity in Datastore
        logger.info("Storing task entry: %s", task_entry)
        client.put(task_entry)

    def handle_task(
        self,
        client: datastore.Client,
        params: dict[str, Any],
        order_task_entries_params: dict[str, Any] | None = None,
    ) -> None:
        """Check if a task instance exists and update it or create a new one.

        Args:
            client: Datastore client instance.
            params: Dictionary containing parameters (key-value pairs) to store.
            order_task_entries_params: Optional parameters for ordering task entries
                when retrieving existing entries.
        """
        # Get commit ID from environment if available
        commit_id = os.environ.get("GITHUB_SHA", "")

        # Create filter to find existing entries
        filter_entries = {
            "dag_id": params["dag_id"],
            "run_id": params["run_id"],
            "airflow_task_id": params["airflow_task_id"],
        }

        # Check for existing entries
        existing_entries = self.get_task_entry(
            client,
            filter_entries,
            self.current_task_kind,
            order_task_entries_params,
        )

        task_key = client.key(self.current_task_kind)
        task_entry = datastore.Entity(key=task_key, exclude_from_indexes=("exception_details",))
        if existing_entries:
            task_entry.update(existing_entries[0])
        else:
            task_entry["commit_id"] = commit_id
            task_entry["created_at"] = datetime.datetime.now(datetime.timezone.utc)

        # Store the task entry
        self.put_snapshot_task_entry(client, task_entry, params)

    @classmethod
    def from_command_line_args(cls, args: CommandLineArguments, task_kind: str) -> DB:
        """Create a DB instance from command line arguments.

        Args:
            args: The parsed command line arguments object with get_secret_locations method.
            task_kind: The kind of task entries this instance will handle.

        Returns:
            An initialized DB module instance.
        """
        try:
            # Try to get the secret locations from command line arguments
            secret_locations = args.get_secret_locations()
            if secret_locations and hasattr(secret_locations, cls.MODULE_NAME):
                secret_path = getattr(secret_locations, cls.MODULE_NAME)
                if isinstance(secret_path, str):
                    gcp_secret_location = secret_path
                elif isinstance(secret_path, dict):
                    gcp_secret_location = secret_path[cls.MODULE_NAME]
        except (AttributeError, TypeError):
            # Fall back to module's defaults if command line args don't work
            pass

        return cls(
            task_kind=task_kind,
            gcp_secret_location=gcp_secret_location or None,
        )
