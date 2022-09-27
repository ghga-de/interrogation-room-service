# Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
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
""" """
import pytest
import pytest_asyncio
from crypt4gh.lib import CIPHER_SEGMENT_SIZE
from ghga_service_chassis_lib.utils import big_temp_file
from hexkit.providers.s3.testutils import s3_fixture  # noqa: F401
from hexkit.providers.s3.testutils import FileObject, S3Fixture

from interrogation_room.core.upload_handler import (
    make_chunks,
    retrieve_part,
    retrieve_parts,
)

BUCKET_ID = "test"
OBJECT_ID = "random-data"
FILE_SIZE = 50 * 1024**2
PART_SIZE = 16 * 1024**2


@pytest_asyncio.fixture
async def prefilled_bucket(s3_fixture: S3Fixture) -> S3Fixture:  # noqa: F811
    """Fill with one test file"""
    with big_temp_file(FILE_SIZE) as data:
        obj = FileObject(file_path=data.name, bucket_id=BUCKET_ID, object_id=OBJECT_ID)
        await s3_fixture.populate_file_objects([obj])
        yield s3_fixture


@pytest.mark.asyncio
async def test_retrieve_parts(prefilled_bucket: S3Fixture):  # noqa: F811
    """Check if ranges match"""
    part_sizes = []
    download_url = await prefilled_bucket.storage.get_object_download_url(
        bucket_id=BUCKET_ID, object_id=OBJECT_ID
    )
    async for part in retrieve_parts(
        url=download_url,
        object_size=FILE_SIZE,
    ):
        part = await part
        part_sizes.append(len(part))
    for size in part_sizes[:-1]:
        assert size == PART_SIZE
    assert part_sizes[-1] > 0


@pytest.mark.asyncio
async def test_make_chunks(prefilled_bucket: S3Fixture):  # noqa: F811
    """Check if chunks have expected size"""
    download_url = await prefilled_bucket.storage.get_object_download_url(
        bucket_id=BUCKET_ID, object_id=OBJECT_ID
    )
    part = await retrieve_part(url=download_url, start=0, stop=PART_SIZE - 1)
    chunks, incomplete_chunk = make_chunks(file_part=part)
    assert len(chunks) == 255
    for chunk in chunks:
        assert len(chunk) == CIPHER_SEGMENT_SIZE
    assert len(incomplete_chunk) == 58_396
