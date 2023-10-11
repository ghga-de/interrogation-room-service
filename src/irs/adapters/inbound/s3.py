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
from collections.abc import AsyncGenerator, Sequence

import requests
from hexkit.providers.s3.provider import S3Config, S3ObjectStorage

from irs.adapters.http import exceptions
from irs.config import CONFIG


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


async def get_download_url(*, object_id: str, bucket_id: str) -> str:
    """Get object download URL from S3 inbox bucket"""
    storage = get_objectstorage()
    return await storage.get_object_download_url(
        bucket_id=bucket_id, object_id=object_id
    )


async def get_object_size(*, object_id: str, bucket_id: str) -> int:
    """Get object size from S3 metadata"""
    storage = get_objectstorage()
    return await storage.get_object_size(bucket_id=bucket_id, object_id=object_id)


def get_objectstorage() -> S3ObjectStorage:
    """Factoring this out makes it overridable by tests"""
    config = S3Config(
        s3_endpoint_url=CONFIG.s3_endpoint_url,
        s3_access_key_id=CONFIG.s3_access_key_id,
        s3_secret_access_key=CONFIG.s3_secret_access_key,
    )
    return S3ObjectStorage(config=config)


async def retrieve_parts(
    *, url: str, object_size: int, part_size: int, offset: int = 0
) -> AsyncGenerator[bytes, None]:
    """Get all parts from inbox, starting with file content at offset"""
    for start, stop in calc_part_ranges(
        part_size=part_size, object_size=object_size, byte_offset=offset
    ):
        yield await retrieve_part(url=url, start=start, stop=stop)


async def retrieve_part(*, url: str, start: int, stop: int) -> bytes:
    """Get one part from inbox by range"""
    try:
        response = requests.get(
            url=url, headers={"Range": f"bytes={start}-{stop}"}, timeout=60
        )
    except requests.exceptions.RequestException as request_error:
        raise exceptions.RequestFailedError(url=url) from request_error

    return response.content


async def init_staging(*, object_id: str) -> str:
    """Start staging a re-encrypted file to staging area, returns an upload id"""
    storage = get_objectstorage()
    return await storage.init_multipart_upload(
        bucket_id=CONFIG.staging_bucket, object_id=object_id
    )


async def stage_part(
    *, upload_id: str, object_id: str, data: bytes, part_number: int
) -> None:
    """Save a file part to the staging area"""
    storage = get_objectstorage()
    url = await storage.get_part_upload_url(
        upload_id=upload_id,
        bucket_id=CONFIG.staging_bucket,
        object_id=object_id,
        part_number=part_number,
    )

    try:
        requests.put(url=url, data=data, timeout=60)
    except requests.exceptions.RequestException as request_error:
        raise exceptions.RequestFailedError(url=url) from request_error


async def complete_staging(
    *, upload_id: str, object_id: str, part_size: int, parts: int
) -> None:
    """Complete the staging of a re-encrypted file"""
    storage = get_objectstorage()
    await storage.complete_multipart_upload(
        upload_id=upload_id,
        bucket_id=CONFIG.staging_bucket,
        object_id=object_id,
        anticipated_part_quantity=parts,
        anticipated_part_size=part_size,
    )
