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
from hexkit.protocols.dao import MultipleHitsFoundError, NoHitsFoundError
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

    async def _inspect_object(
        self, *, bucket_id: str, object_id: str, storage_alias: str
    ):
        """Check one specific object in a specified storage node."""
        extra = {
            "object_id": object_id,
            "bucket_id": bucket_id,
            "storage_alias": storage_alias,
        }
        try:
            staging_object = await self._staging_object_dao.find_one(
                mapping={"object_id": object_id, "storage_alias": storage_alias}
            )
        except NoHitsFoundError:
            # This can happen in one of two cases:
            # 1) Failed to send success event
            # 2) Success event sent, but failed to insert staging object entry
            log.error(
                "Object '%s' with no corresponding DB entry found in bucket '%s'"
                + " of storage '%s'.",
                *extra.values(),
                extra=extra,
            )
            return

        except MultipleHitsFoundError as error:
            # can only happen if the same object ID is generated from uuid4()
            log.critical(
                error,
                extra=extra,
            )
            return

        stale_as_of = now_as_utc() - timedelta(
            minutes=self._config.object_stale_after_minutes
        )
        if staging_object.creation_date <= stale_as_of:
            # only log for now, but this points to an underlying issue
            extra["file_id"] = staging_object.file_id
            log.error(
                "Stale object '%s' found for file '%s' in bucket '%s' of storage '%s'.",
                *[
                    extra[key]
                    for key in ("object_id", "file_id", "bucket_id", "storage_alias")
                ],
                extra=extra,
            )

    async def check_buckets(self):
        """Check objects in all buckets configured for the service."""
        for storage_alias in self._object_storages._config.object_storages:
            log.info(f"Checking for stale objects in storage {storage_alias}.")

            bucket_id, object_storage = self._object_storages.for_alias(storage_alias)

            for object_id in await object_storage.list_all_object_ids(bucket_id):
                await self._inspect_object(
                    bucket_id=bucket_id,
                    object_id=object_id,
                    storage_alias=storage_alias,
                )
