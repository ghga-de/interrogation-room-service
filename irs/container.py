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
"""Contains a configurable DI Container"""

from hexkit.inject import ContainerBase, get_configurator, get_constructor
from hexkit.providers.akafka.provider import KafkaEventPublisher, KafkaEventSubscriber

from irs.adapters.inbound.event_sub import EventSubTranslator
from irs.adapters.outbound.event_pub import EventPublisher
from irs.config import Config
from irs.core.interrogator import Interrogator


class Container(ContainerBase):
    """Dependency-Injection Container"""

    config = get_configurator(Config)
    event_pub_provider = get_constructor(KafkaEventPublisher, config=config)
    event_publisher = get_constructor(
        EventPublisher,
        config=config,
        provider=event_pub_provider,
    )

    interrogator = get_constructor(Interrogator, event_publisher=event_publisher)

    event_sub_translator = get_constructor(
        EventSubTranslator,
        config=config,
        interrogator=interrogator,
    )

    event_subscriber = get_constructor(
        KafkaEventSubscriber,
        config=config,
        translator=event_sub_translator,
    )
