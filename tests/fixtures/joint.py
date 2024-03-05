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

import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass

import pytest
import pytest_asyncio
from ghga_service_commons.utils.multinode_storage import (
    S3ObjectStorageNodeConfig,
    S3ObjectStoragesConfig,
)
from hexkit.providers.akafka import KafkaEventSubscriber
from hexkit.providers.akafka.testutils import KafkaFixture, get_kafka_fixture
from hexkit.providers.mongodb.testutils import MongoDbFixture, get_mongodb_fixture
from hexkit.providers.s3.testutils import S3Fixture, get_s3_fixture

from irs.config import Config
from irs.inject import prepare_core, prepare_event_subscriber
from irs.ports.inbound.interrogator import InterrogatorPort
from tests.fixtures.config import get_config
from tests.fixtures.keypair_fixtures import (
    KeypairFixture,
    keypair_fixture,  # noqa: F401
)

FILE_SIZE = 50 * 1024**2
INBOX_BUCKET_ID = "test-inbox"
STAGING_BUCKET_ID = "test-staging"


kafka_fixture = get_kafka_fixture(scope="session")
mongodb_fixture = get_mongodb_fixture(scope="session")
s3_fixture = get_s3_fixture(scope="session")
second_s3_fixture = get_s3_fixture(scope="session")


@dataclass
class EndpointAliases:
    """Container class for endpoint aliases to be used by test code"""

    node1: str = "test"
    node2: str = "test2"


@dataclass
class JointFixture:
    """Returned by the `joint_fixture`."""

    config: Config
    event_subscriber: KafkaEventSubscriber
    interrogator: InterrogatorPort
    kafka: KafkaFixture
    keypair: KeypairFixture
    mongodb: MongoDbFixture
    s3: S3Fixture
    second_s3: S3Fixture
    endpoint_aliases: EndpointAliases

    async def reset_state(self):
        """Completely reset fixture states"""
        await self.s3.empty_buckets()
        await self.second_s3.empty_buckets()
        self.mongodb.empty_collections()
        self.kafka.clear_topics()
        self.keypair.regenerate()


@pytest_asyncio.fixture(scope="session")
async def joint_fixture(
    keypair_fixture: KeypairFixture,  # noqa: F811
    kafka_fixture: KafkaFixture,
    mongodb_fixture: MongoDbFixture,
    s3_fixture: S3Fixture,
    second_s3_fixture: S3Fixture,
) -> AsyncGenerator[JointFixture, None]:
    """A fixture that embeds all other fixtures for integration testing"""
    node_config = S3ObjectStorageNodeConfig(
        bucket=STAGING_BUCKET_ID, credentials=s3_fixture.config
    )
    second_node_config = S3ObjectStorageNodeConfig(
        bucket=STAGING_BUCKET_ID, credentials=second_s3_fixture.config
    )

    endpoint_aliases = EndpointAliases()

    object_storage_config = S3ObjectStoragesConfig(
        object_storages={
            endpoint_aliases.node1: node_config,
            endpoint_aliases.node2: second_node_config,
        }
    )
    config = get_config(
        sources=[kafka_fixture.config, mongodb_fixture.config, object_storage_config]
    )

    await s3_fixture.populate_buckets([INBOX_BUCKET_ID, STAGING_BUCKET_ID])
    await second_s3_fixture.populate_buckets([INBOX_BUCKET_ID, STAGING_BUCKET_ID])

    # Create joint_fixure using the injection
    async with (
        prepare_core(config=config) as interrogator,
        prepare_event_subscriber(
            config=config, interrogator_override=interrogator
        ) as event_subscriber,
    ):
        yield JointFixture(
            config=config,
            event_subscriber=event_subscriber,
            interrogator=interrogator,
            kafka=kafka_fixture,
            keypair=keypair_fixture,
            mongodb=mongodb_fixture,
            s3=s3_fixture,
            second_s3=second_s3_fixture,
            endpoint_aliases=endpoint_aliases,
        )


@pytest.fixture(autouse=True, scope="function")
def reset_state(joint_fixture: JointFixture):
    """Clear joint_fixture state before tests that use this fixture.

    This is a function-level fixture because it needs to run in each test.
    """
    loop = asyncio.get_event_loop()
    loop.run_until_complete(joint_fixture.reset_state())
