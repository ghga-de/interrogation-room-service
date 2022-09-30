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

import sys

import pytest_asyncio
from ghga_service_chassis_lib.utils import big_temp_file
from hexkit.providers.s3.testutils import s3_fixture  # noqa: F401
from hexkit.providers.s3.testutils import FileObject, S3Fixture

BUCKET_ID = "test"
OBJECT_ID = "random-data"
FILE_SIZE = 50 * 1024**2
PART_SIZE = 16 * 1024**2


@pytest_asyncio.fixture
async def prefilled_bucket(s3_fixture: S3Fixture) -> S3Fixture:  # noqa: F811
    """Bucket prefilled with one random test file"""
    sys.set_int_max_str_digits(256 * 1024**2)  # type: ignore
    with big_temp_file(FILE_SIZE) as data:
        obj = FileObject(file_path=data.name, bucket_id=BUCKET_ID, object_id=OBJECT_ID)
        await s3_fixture.populate_file_objects([obj])
        yield s3_fixture
