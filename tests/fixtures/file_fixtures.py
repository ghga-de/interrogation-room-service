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

import hashlib
import sys
import tempfile
from dataclasses import dataclass
from typing import AsyncGenerator, Tuple

import crypt4gh.header
import crypt4gh.lib
import pytest_asyncio
from ghga_service_chassis_lib.utils import big_temp_file
from hexkit.providers.s3.testutils import s3_fixture  # noqa: F401
from hexkit.providers.s3.testutils import FileObject, S3Fixture

from .keypair_fixtures import KeypairFixture, generate_keypair_fixture  # noqa: F401

BUCKET_ID = "test-bucket"
OBJECT_ID = "test-object"
FILE_SIZE = 50 * 1024**2
PART_SIZE = 16 * 1024**2

sys.set_int_max_str_digits(256 * 1024**2)  # type: ignore


@dataclass
class EncryptedDataFixture:
    """Object metadata for testing purposes"""

    checksum: str
    file_secret: bytes
    file_size: int
    offset: int
    public_key: bytes
    s3_fixture: S3Fixture


@pytest_asyncio.fixture
async def prefilled_random_data(s3_fixture: S3Fixture) -> S3Fixture:  # noqa: F811
    """Bucket prefilled with one random test file"""
    with big_temp_file(FILE_SIZE) as data:
        obj = FileObject(file_path=data.name, bucket_id=BUCKET_ID, object_id=OBJECT_ID)
        await s3_fixture.populate_file_objects([obj])
        yield s3_fixture


@pytest_asyncio.fixture
async def encrypted_random_data(
    monkeypatch,
    generate_keypair_fixture: KeypairFixture,  # noqa: F811
    s3_fixture: S3Fixture,  # noqa: F811
) -> AsyncGenerator[EncryptedDataFixture, None]:
    """Bucket prefilled with crypt4gh-encrypted random data"""
    with big_temp_file(FILE_SIZE) as data:
        # rewind data pointer
        data.seek(0)
        with tempfile.NamedTemporaryFile() as encrypted_file:
            private_key = generate_keypair_fixture.private_key
            public_key = generate_keypair_fixture.public_key
            enc_keys = [(0, private_key, public_key)]

            crypt4gh.lib.encrypt(keys=enc_keys, infile=data, outfile=encrypted_file)

            # get unencrypted checksum
            data.seek(0)
            checksum = hashlib.sha256(data.read()).hexdigest()

            encrypted_file.seek(0)
            dec_keys = [(0, private_key, None)]
            session_keys, _ = crypt4gh.header.deconstruct(
                infile=encrypted_file, keys=dec_keys, sender_pubkey=public_key
            )
            file_secret = session_keys[0]

            offset = encrypted_file.tell()
            # Rewind file
            encrypted_file.seek(0)
            obj = FileObject(
                file_path=encrypted_file.name, bucket_id=BUCKET_ID, object_id=OBJECT_ID
            )
            file_size = len(obj.content)
            await s3_fixture.populate_file_objects([obj])

            def eks_patch(
                *, file_part: bytes, public_key: bytes, api_url: str
            ) -> Tuple[bytes, str, int]:
                """Monkeypatch to emulate API Call"""

                return (
                    file_secret,
                    "secret_id",
                    offset,
                )

            monkeypatch.setattr(
                "irs.core.upload_handler.call_eks_api",
                eks_patch,
            )
            monkeypatch.setattr(
                "irs.adapters.inbound.s3_download.get_objectstorage",
                lambda: s3_fixture.storage,
            )

            yield EncryptedDataFixture(
                checksum=checksum,
                file_secret=file_secret,
                file_size=file_size,
                public_key=public_key,
                offset=offset,
                s3_fixture=s3_fixture,
            )
