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

from typing import Dict, Union

import pytest

from .fixtures.kafka_fixtures import event_fixture  # noqa: F401
from .fixtures.kafka_fixtures import kafka_fixture  # noqa: F401
from .fixtures.kafka_fixtures import KafkaFixture


@pytest.mark.asyncio
async def test_incoming_message(
    *,
    kafka_fixture: KafkaFixture,  # noqa: F811
    event_fixture: Dict[str, Union[object, str]],  # noqa: F811
):
    """Start"""
    await kafka_fixture.publisher.publish(**event_fixture)
    await kafka_fixture.subscriber.run(forever=False)
