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

"""Contains models used to identify events that have already been processed"""

import hashlib

from ghga_event_schemas import pydantic_ as event_schemas
from ghga_service_commons.utils.utc_dates import UTCDatetime, now_as_utc
from pydantic import BaseModel, Field


class StagingObject(BaseModel):
    """
    Stores data mapping an object in a staging bucket to its file ID.

    This is done to allow checking for and the removal of stale objects, as only the
    file ID is available from the incoming validation event.

    Needs a timestamp, as the downstream failure path does not return a failure event
    and the CLI command checking for stale objects needs some information on when to
    consider and object in staging stale.
    """

    file_id: str
    object_id: str
    creation_date: UTCDatetime = Field(default_factory=now_as_utc)


class UploadReceivedFingerprint(BaseModel):
    """
    Stores the hash of a FileUploadReceived payload along with its generation
    timestamp. This facilitates the identification of previously encountered
    payloads and the removal of old fingerprints.
    """

    checksum: str
    creation_date: UTCDatetime = Field(default_factory=now_as_utc)

    @staticmethod
    def generate(
        payload: event_schemas.FileUploadReceived,
    ) -> "UploadReceivedFingerprint":
        """Serialize payload to json and generate hash"""
        serialized = payload.model_dump_json()
        checksum = hashlib.sha256(serialized.encode("utf-8")).hexdigest()

        return UploadReceivedFingerprint(checksum=checksum)
