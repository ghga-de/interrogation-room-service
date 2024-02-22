# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Functionality to periodically deal with stale files in configured object storage."""

import logging
from datetime import timedelta

from ghga_service_commons.utils.multinode_storage import S3ObjectStorages
from ghga_service_commons.utils.utc_dates import now_as_utc
from pydantic import Field
from pydantic_settings import BaseSettings

from irs.ports.inbound.storage_inspector import StorageInspectorPort
from irs.ports.outbound.dao import StagingObjectDaoPort

log = logging.getLogger(__name__)


class StorageInspectorConfig(BaseSettings):
    """Config specific to storage inspector"""

    object_stale_after_minutes: int = Field(
        default=...,
        description="Amount of time in minutes after which an object in the staging bucket"
        + " is considered stale. If an object continues existing after this point in time,"
        + " this is an indication, that something might have gone wrong downstream.",
    )


class StagingInspector(StorageInspectorPort):
    """Checks inbox storage buckets for stale files."""

    def __init__(
        self,
        config: StorageInspectorConfig,
        staging_object_dao: StagingObjectDaoPort,
        object_storages: S3ObjectStorages,
    ):
        """Initialize with DB DAOs and storage handles."""
        self._config = config
        self._staging_object_dao = staging_object_dao
        self._object_storages = object_storages

    async def check_buckets(self):
        """Check objects in all buckets configured for the service."""
        log.debug("Checking for stale objects.")

        async for staging_object in self._staging_object_dao.find_all(mapping={}):
            stale_as_of = now_as_utc() - timedelta(
                minutes=self._config.object_stale_after_minutes
            )

            storage_alias = staging_object.storage_alias
            try:
                bucket_id, _ = self._object_storages.for_alias(
                    endpoint_alias=storage_alias
                )
            except KeyError as error:
                storage_not_configured = ValueError(
                    f"Storage alias not configured: {storage_alias}"
                )
                log.critical(storage_not_configured, extra={"alias": storage_alias})
                raise storage_not_configured from error

            if staging_object.creation_date <= stale_as_of:
                # only log for now, but this points to an underlying issue
                extra = {
                    "object_id": staging_object.object_id,
                    "file_id": staging_object.file_id,
                    "bucket_id": bucket_id,
                    "storage_alias": storage_alias,
                }

                log.error(
                    "Stale object '%s' found for file '%s' in bucket '%s' of storage '%s'.",
                    *extra.values(),
                    extra=extra,
                )
