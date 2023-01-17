# Copyright 2021 - 2023 Universität Tübingen, DKFZ and EMBL
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
"""Fixtures and classes to test kafka communication"""

import pytest_asyncio
from hexkit.providers.akafka import KafkaEventPublisher, KafkaEventSubscriber
from hexkit.providers.akafka.testutils import KafkaFixture
from testcontainers.kafka import KafkaContainer

from irs.adapters.inbound.akafka import EventSubTranslator
from irs.adapters.outbound.akafka import EventPublisher
from irs.core.interrogator import Interrogator
from tests.fixtures.config import DEFAULT_CONFIG, Config


class IRSKafkaFixture(KafkaFixture):
    """Storing configured publisher/subscriber pair for Kafka"""

    def __init__(
        self,
        kafka_servers: list[str],
        publisher: KafkaEventPublisher,
        subscriber: KafkaEventSubscriber,
        config: Config,
    ):
        """Initialize with connection details and a ready-to-use publisher and subscriber"""
        self.kafka_servers = kafka_servers
        self.publisher = publisher
        self.subscriber = subscriber
        self.config = config


@pytest_asyncio.fixture
async def irs_kafka_fixture():
    """Configure Kafka subscriber/publisher"""
    with KafkaContainer() as kafka_container:
        kafka_servers = [kafka_container.get_bootstrap_server()]
        config = DEFAULT_CONFIG
        config.kafka_servers = kafka_servers

        async with KafkaEventPublisher.construct(config=config) as publish_provider:
            publisher = EventPublisher(config=config, provider=publish_provider)
            interrogator = Interrogator(event_publisher=publisher)

            async with KafkaEventSubscriber.construct(
                config=config,
                translator=EventSubTranslator(config=config, interrogator=interrogator),
            ) as subscriber:
                yield IRSKafkaFixture(
                    kafka_servers=kafka_servers,
                    publisher=publish_provider,
                    subscriber=subscriber,
                    config=config,
                )
