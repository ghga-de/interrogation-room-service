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
    storage_alias: str
    creation_date: UTCDatetime = Field(default_factory=now_as_utc)


class InterrogationSubject(BaseModel):
    """Necessary data for core functionality from incoming payload-"""

    file_id: str
    inbox_bucket_id: str
    inbox_object_id: str
    storage_alias: str
    decrypted_size: int
    expected_decrypted_sha256: str
    upload_date: str
    submitter_public_key: str


class UploadReceivedFingerprint(BaseModel):
    """
    Stores the hash of an InterrogationSubject payload along with its generation
    timestamp. This facilitates the identification of previously encountered
    payloads and the removal of old fingerprints.
    """

    checksum: str
    creation_date: UTCDatetime = Field(default_factory=now_as_utc)

    @staticmethod
    def generate(
        subject: InterrogationSubject,
    ) -> "UploadReceivedFingerprint":
        """Serialize payload to json and generate hash"""
        serialized = subject.model_dump_json()
        checksum = hashlib.sha256(serialized.encode("utf-8")).hexdigest()

        return UploadReceivedFingerprint(checksum=checksum)


class Checksums(BaseModel):
    """Container for checksums needed for validation and outbound events."""

    part_checksums_md5: list[str]
    part_checksums_sha256: list[str]
    content_checksum_sha256: str


class ProcessingResult(BaseModel):
    """Data generated during re-encryption that is needed downstream."""

    offset: int
    secret_id: str
    checksums: Checksums
