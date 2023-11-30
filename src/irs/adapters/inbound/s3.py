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
"""Provides helpers for S3 interaction"""

import math
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Sequence
from dataclasses import dataclass

import requests
from hexkit.providers.s3.provider import S3ObjectStorage

from irs.adapters.http import exceptions


def calc_part_ranges(
    *, part_size: int, object_size: int, byte_offset: int
) -> Sequence[tuple[int, int]]:
    """Calculate and return the ranges (start, end) of file parts as a list of tuples."""
    # calc the ranges for the parts that have the full part_size:
    full_part_number = math.floor(object_size / part_size)

    part_ranges = [
        (
            byte_offset + part_size * (part_no - 1),
            byte_offset + part_size * (part_no) - 1,
        )
        for part_no in range(1, full_part_number + 1)
    ]

    if (object_size % part_size) > 0:
        # if the last part is smaller than the part_size, calculate it range separately:
        part_ranges.append(
            (byte_offset + part_size * full_part_number, object_size - 1)
        )
    return part_ranges


@dataclass
class S3IDs:
    """Container for bucket and object ID"""

    bucket_id: str
    object_id: str


class StagingHandlerPort(ABC):
    """Abstract base for object storage functionality dealing with staging."""

    @abstractmethod
    async def init_staging(self) -> None:
        """Start staging a re-encrypted file to staging area, returns an upload id"""

    @abstractmethod
    async def stage_part(self, *, data: bytes, part_number: int) -> None:
        """Save a file part to the staging area"""

    @abstractmethod
    async def complete_staging(self, *, parts: int) -> None:
        """Complete the staging of a re-encrypted file"""

    @abstractmethod
    async def retrieve_parts(self, *, offset: int = 0) -> AsyncGenerator[bytes, None]:
        """Get all parts from inbox, starting with file content at offset"""

    @abstractmethod
    async def retrieve_part(self, *, url: str, start: int, stop: int) -> bytes:
        """Get one part from inbox by range"""


class StagingHandler(StagingHandlerPort):
    """Wrapper for object storage staging functionality."""

    def __init__(
        self,
        object_storage: S3ObjectStorage,
        inbox_ids: S3IDs,
        staging_ids: S3IDs,
        part_size: int,
    ) -> None:
        self._object_storage = object_storage
        self._part_size = part_size
        self._inbox = inbox_ids
        self._staging = staging_ids
        self._upload_id = ""

    async def init_staging(self) -> None:
        """Start staging a re-encrypted file to staging area, returns an upload id"""
        self._upload_id = await self._object_storage.init_multipart_upload(
            bucket_id=self._staging.bucket_id, object_id=self._staging.object_id
        )

    async def stage_part(self, *, data: bytes, part_number: int) -> None:
        """Save a file part to the staging area"""
        url = await self._object_storage.get_part_upload_url(
            upload_id=self._upload_id,
            bucket_id=self._staging.bucket_id,
            object_id=self._staging.object_id,
            part_number=part_number,
        )

        try:
            requests.put(url=url, data=data, timeout=60)
        except requests.exceptions.RequestException as request_error:
            raise exceptions.RequestFailedError(url=url) from request_error

    async def complete_staging(self, *, parts: int) -> None:
        """Complete the staging of a re-encrypted file"""
        await self._object_storage.complete_multipart_upload(
            upload_id=self._upload_id,
            bucket_id=self._staging.bucket_id,
            object_id=self._staging.object_id,
            anticipated_part_quantity=parts,
            anticipated_part_size=self._part_size,
        )

    async def retrieve_parts(self, *, offset: int = 0) -> AsyncGenerator[bytes, None]:  # type: ignore
        """Get all parts from inbox, starting with file content at offset"""
        download_url = await self._object_storage.get_object_download_url(
            bucket_id=self._inbox.bucket_id, object_id=self._inbox.object_id
        )
        object_size = await self._object_storage.get_object_size(
            bucket_id=self._inbox.bucket_id, object_id=self._inbox.object_id
        )
        for start, stop in calc_part_ranges(
            part_size=self._part_size, object_size=object_size, byte_offset=offset
        ):
            yield await self.retrieve_part(url=download_url, start=start, stop=stop)

    async def retrieve_part(self, *, url: str, start: int, stop: int) -> bytes:
        """Get one part from inbox by range"""
        try:
            response = requests.get(
                url=url, headers={"Range": f"bytes={start}-{stop}"}, timeout=60
            )
        except requests.exceptions.RequestException as request_error:
            raise exceptions.RequestFailedError(url=url) from request_error

        return response.content
