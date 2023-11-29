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

"""Provides multiple fixtures in one spot"""

import hashlib
import tempfile
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import crypt4gh.header
import crypt4gh.lib
import pytest_asyncio
from ghga_service_commons.utils.multinode_storage import (
    S3ObjectStorageNodeConfig,
    S3ObjectStoragesConfig,
)
from ghga_service_commons.utils.temp_files import big_temp_file
from hexkit.providers.akafka import KafkaEventSubscriber
from hexkit.providers.akafka.testutils import KafkaFixture, kafka_fixture  # noqa: F401
from hexkit.providers.s3.testutils import (
    FileObject,
    S3Fixture,
    s3_fixture,  # noqa: F401
)

from irs.config import Config
from irs.inject import prepare_core, prepare_event_subscriber
from irs.ports.inbound.interrogator import InterrogatorPort
from tests.fixtures.config import get_config
from tests.fixtures.keypair_fixtures import (
    KeypairFixture,
    generate_keypair_fixture,  # noqa: F401
)

S3_ENDPOINT_ALIAS = "test"
INBOX_BUCKET_ID = "test-inbox"
STAGING_BUCKET_ID = "test-staging"
FILE_ID = "test-file-id"
OBJECT_ID = "test-object"
FILE_SIZE = 50 * 1024**2


@dataclass
class EncryptedData:
    """Object metadata for testing purposes"""

    checksum: str
    file_secret: bytes
    file_size: int
    upload_date: str
    offset: int
    public_key: bytes
    s3_fixture: S3Fixture


@dataclass
class JointFixture:
    """Returned by the `joint_fixture`."""

    config: Config
    event_subscriber: KafkaEventSubscriber
    interrogator: InterrogatorPort
    kafka: KafkaFixture
    encrypted_data: EncryptedData


@pytest_asyncio.fixture()
async def joint_fixture(
    generate_keypair_fixture: KeypairFixture,  # noqa: F811
    kafka_fixture: KafkaFixture,  # noqa: F811
    s3_fixture: S3Fixture,  # noqa: F811
) -> AsyncGenerator[JointFixture, None]:
    """A fixture that embeds all other fixtures for integration testing"""
    # merge configs from different sources with the default one:

    node_config = S3ObjectStorageNodeConfig(
        bucket=STAGING_BUCKET_ID, credentials=s3_fixture.config
    )
    object_storage_config = S3ObjectStoragesConfig(
        object_storages={S3_ENDPOINT_ALIAS: node_config}
    )
    config = get_config(sources=[kafka_fixture.config, object_storage_config])

    await s3_fixture.populate_buckets([INBOX_BUCKET_ID, STAGING_BUCKET_ID])

    # Create joint_fixure using the injection
    async with prepare_core(config=config) as interrogator, prepare_event_subscriber(
        config=config, interrogator_override=interrogator
    ) as event_subscriber:
        with big_temp_file(FILE_SIZE) as data:
            # rewind data pointer
            data.seek(0)
            with tempfile.NamedTemporaryFile() as encrypted_file:
                upload_date = datetime.utcnow().isoformat()
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
                    file_path=Path(encrypted_file.name),
                    bucket_id=INBOX_BUCKET_ID,
                    object_id=OBJECT_ID,
                )
                file_size = len(obj.content)
                await s3_fixture.populate_file_objects([obj])

                encrypted_data = EncryptedData(
                    checksum=checksum,
                    file_secret=file_secret,
                    file_size=file_size,
                    public_key=public_key,
                    offset=offset,
                    s3_fixture=s3_fixture,
                    upload_date=upload_date,
                )

                yield JointFixture(
                    config=config,
                    event_subscriber=event_subscriber,
                    interrogator=interrogator,
                    kafka=kafka_fixture,
                    encrypted_data=encrypted_data,
                )
