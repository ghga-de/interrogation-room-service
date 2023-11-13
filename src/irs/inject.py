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
"""Module hosting the dependency injection container."""
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Optional

from ghga_service_commons.utils.context import asyncnullcontext
from hexkit.providers.akafka.provider import KafkaEventPublisher, KafkaEventSubscriber

from irs.adapters.inbound.event_sub import EventSubTranslator
from irs.adapters.outbound.event_pub import EventPublisher
from irs.config import Config
from irs.core.interrogator import Interrogator
from irs.ports.inbound.interrogator import InterrogatorPort


@asynccontextmanager
async def prepare_core(*, config: Config) -> AsyncGenerator[InterrogatorPort, None]:
    """Constructs and initializes all core components and their outbound dependencies."""
    async with KafkaEventPublisher.construct(config=config) as event_pub_provider:
        event_publisher = EventPublisher(config=config, provider=event_pub_provider)
        yield Interrogator(event_publisher=event_publisher)


def prepare_core_with_override(
    *, config: Config, interrogator_override: Optional[InterrogatorPort] = None
):
    """Resolve the interrogator context manager based on config and override (if any)."""
    return (
        asyncnullcontext(interrogator_override)
        if interrogator_override
        else prepare_core(config=config)
    )


@asynccontextmanager
async def prepare_event_subscriber(
    *,
    config: Config,
    interrogator_override: Optional[InterrogatorPort] = None,
) -> AsyncGenerator[KafkaEventSubscriber, None]:
    """Construct and initialize an event subscriber with all its dependencies.

    By default, the core dependencies are automatically prepared but you can also
    provide them using the interrogator_override parameter.
    """
    async with prepare_core_with_override(
        config=config, interrogator_override=interrogator_override
    ) as interrogator:
        event_sub_translator = EventSubTranslator(
            interrogator=interrogator,
            config=config,
        )

        async with KafkaEventSubscriber.construct(
            config=config, translator=event_sub_translator
        ) as event_subscriber:
            yield event_subscriber
