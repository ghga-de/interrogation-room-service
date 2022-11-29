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

"""Entrypoint of the package"""

import asyncio

from hexkit.providers.akafka import KafkaEventPublisher, KafkaEventSubscriber

from irs.adapters.inbound.kafka_ucs_consumer import EventSubTranslator
from irs.adapters.outbound.kafka_producer import EventPublisher
from irs.core.interrogator import Interrogator

from .config import CONFIG, Config


async def run_subscriber(config: Config = CONFIG):
    """Start the EventSubscriber part of the service"""

    async with KafkaEventPublisher.construct(config=config) as publish_provider:
        event_publisher = EventPublisher(config=config, provider=publish_provider)
        interrogator = Interrogator(event_publisher=event_publisher)
        async with KafkaEventSubscriber.construct(
            config=config,
            translator=EventSubTranslator(config=config, interrogator=interrogator),
        ) as subscriber:
            await subscriber.run()


def run():
    """Synchronous entrypoint for Docker container, etc."""
    asyncio.run(run_subscriber())
