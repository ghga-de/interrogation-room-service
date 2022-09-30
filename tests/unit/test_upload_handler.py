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
from crypt4gh.lib import CIPHER_SEGMENT_SIZE
from hexkit.providers.s3.testutils import s3_fixture  # noqa: F401
from hexkit.providers.s3.testutils import S3Fixture

from irs.core.upload_handler import make_chunks, retrieve_part, retrieve_parts

from .fixtures.file_fixtures import prefilled_bucket  # noqa: F401
from .fixtures.file_fixtures import BUCKET_ID, FILE_SIZE, OBJECT_ID, PART_SIZE


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
