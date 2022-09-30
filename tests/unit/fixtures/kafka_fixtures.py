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
"""Fixtures and classes to test kafka communication"""

from dataclasses import dataclass
from typing import AsyncGenerator, Collection, Mapping

import pytest_asyncio
from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.eventsub import EventSubscriberProtocol
from hexkit.providers.akafka import (
    KafkaConfig,
    KafkaEventPublisher,
    KafkaEventSubscriber,
)
from testcontainers.kafka import KafkaContainer

from irs.adapters.inbound.kafka_ucs_consumer import FileUploadCompletedEvent


class SubscriberTestProtocol(EventSubscriberProtocol):
    """Validate SubscriberEvent"""

    topics_of_interest: list[Ascii] = ["test_topic"]
    types_of_interest: list[Ascii] = ["test_type"]

    async def _consume_validated(
        self, *, payload: JsonObject, type_: Ascii, topic: Ascii
    ) -> None:
        """
        Receive and process an event with already validated topic and type.

        Args:
            payload (JsonObject): The data/payload to send with the event.
            type_ (str): The type of the event.
            topic (str): Name of the topic the event was published to.
        """
        assert topic in self.topics_of_interest
        assert type_ in self.types_of_interest
        assert payload["file_id"] == "123"
        assert payload["public_key"] == "shashashasha"
        assert payload["size"] == 100
        assert payload["sha256_checksum"] == "456"


@pytest_asyncio.fixture
async def event_fixture() -> AsyncGenerator[Mapping[str, Collection[str]], None]:
    """ """
    data = FileUploadCompletedEvent(
        file_id="123", public_key="shashashasha", sha256_checksum="456", size=100
    )
    type_ = "test_type"
    key = "test_key"
    topic = "test_topic"
    print(data)
    event = {"payload": data.dict(), "type_": type_, "key": key, "topic": topic}
    yield event


@dataclass
class KafkaFixture:
    """ """

    publisher: KafkaEventPublisher
    subscriber: KafkaEventSubscriber


@pytest_asyncio.fixture
async def kafka_fixture():
    """ """
    with KafkaContainer() as kafka_container:
        kafka_server = kafka_container.get_bootstrap_server()
        print(kafka_server)
        config = KafkaConfig(
            service_name="irs_test_publisher",
            service_instance_id="1",
            kafka_servers=[kafka_server],
        )
        async with KafkaEventPublisher.construct(config=config) as publisher:
            async with KafkaEventSubscriber.construct(
                config=config, translator=SubscriberTestProtocol()
            ) as subscriber:
                yield KafkaFixture(publisher=publisher, subscriber=subscriber)
