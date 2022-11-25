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

import base64
from typing import Any, Collection, Mapping, Tuple

import pytest
from hexkit.providers.akafka.testutils import ExpectedEvent
from hexkit.providers.s3.testutils import s3_fixture  # noqa: F401
from hexkit.utils import calc_part_size

from tests.fixtures.file_fixtures import encrypted_random_data  # noqa: F401
from tests.fixtures.file_fixtures import OBJECT_ID, EncryptedDataFixture
from tests.fixtures.kafka_fixtures import (  # noqa: F401
    IRSKafkaFixture,
    irs_kafka_fixture,
)
from tests.fixtures.keypair_fixtures import generate_keypair_fixture  # noqa: F401


def incoming_irs_event(payload: dict[str, object]) -> Mapping[str, Collection[str]]:
    """Emulate incoming event from ucs"""
    type_ = "file_uploads"
    key = OBJECT_ID
    topic = "file_upload_received"
    event = {"payload": payload, "type_": type_, "key": key, "topic": topic}
    return event


def incoming_payload(data: EncryptedDataFixture) -> dict[str, Any]:
    """Payload arriving at the interrogation room"""
    return {
        "file_id": OBJECT_ID,
        "submitter_public_key": base64.b64encode(data.public_key).decode("utf-8"),
        "upload_date": data.upload_date,
        "expected_decrypted_sha256": data.checksum,
        "decrypted_size": data.file_size,
    }


@pytest.mark.asyncio
async def test_failure_event(
    monkeypatch,
    encrypted_random_data: EncryptedDataFixture,  # noqa: F811
    irs_kafka_fixture: IRSKafkaFixture,  # noqa: F811
):
    """
    Test the whole pipeline from receiving an event to notifying about failure
    """
    # explicit patchingng required for now
    def eks_patch(
        *, file_part: bytes, public_key: bytes, api_url: str
    ) -> Tuple[bytes, str, int]:
        """Monkeypatch to emulate API Call"""
        return (
            encrypted_random_data.file_secret,
            "secret_id",
            encrypted_random_data.offset,
        )

    async def publisher_patch():
        return irs_kafka_fixture.publisher

    monkeypatch.setattr(
        "irs.core.upload_handler.call_eks_api",
        eks_patch,
    )
    monkeypatch.setattr(
        "irs.adapters.inbound.s3_download.get_objectstorage",
        lambda: encrypted_random_data.s3_fixture.storage,
    )
    monkeypatch.setattr(
        "irs.adapters.outbound.kafka_producer.get_publisher", publisher_patch
    )

    payload_in = incoming_payload(encrypted_random_data)
    # introduce invalid checksum
    payload_in["expected_decrypted_sha256"] = payload_in["expected_decrypted_sha256"][
        1:
    ]
    event_in = incoming_irs_event(payload=payload_in)

    payload_out = {
        "file_id": OBJECT_ID,
        "reason": "Checksum mismatch",
    }
    expected_event_out = ExpectedEvent(
        payload=payload_out, type_="upload_validation_failure", key=OBJECT_ID
    )

    async with irs_kafka_fixture.record_events(
        in_topic="file_interrogation",
    ) as event_recorder:
        await irs_kafka_fixture.publish_event(**event_in)
        await irs_kafka_fixture.subscriber.run(forever=False)

    recorded_events = event_recorder.recorded_events

    assert len(recorded_events) == 1
    assert recorded_events[0].payload == expected_event_out.payload


@pytest.mark.asyncio
async def test_success_event(
    monkeypatch,
    encrypted_random_data: EncryptedDataFixture,  # noqa: F811
    irs_kafka_fixture: IRSKafkaFixture,  # noqa: F811
):
    """
    Test the whole pipeline from receiving an event to notifying about success
    """
    # explicit patching required for now
    def eks_patch(
        *, file_part: bytes, public_key: bytes, api_url: str
    ) -> Tuple[bytes, str, int]:
        """Monkeypatch to emulate API Call"""
        return (
            encrypted_random_data.file_secret,
            "secret_id",
            encrypted_random_data.offset,
        )

    async def publisher_patch():
        return irs_kafka_fixture.publisher

    monkeypatch.setattr(
        "irs.core.upload_handler.call_eks_api",
        eks_patch,
    )
    monkeypatch.setattr(
        "irs.adapters.inbound.s3_download.get_objectstorage",
        lambda: encrypted_random_data.s3_fixture.storage,
    )
    monkeypatch.setattr(
        "irs.adapters.outbound.kafka_producer.get_publisher", publisher_patch
    )

    payload_in = incoming_payload(encrypted_random_data)
    event_in = incoming_irs_event(payload=payload_in)

    part_size = calc_part_size(file_size=encrypted_random_data.file_size)

    payload_out = {
        "file_id": OBJECT_ID,
        "upload_date": encrypted_random_data.upload_date,
        "decryption_secret_id": "secret_id",
        "content_offset": encrypted_random_data.offset,
        "encrypted_part_size": part_size,
        "decrypted_sha256": encrypted_random_data.checksum,
        "encrypted_parts_md5": "",  # TODO
        "encrypted_parts_sha256": "",  # TODO
    }
    expected_event_out = ExpectedEvent(
        payload=payload_out, type_="upload_validation_success", key="test-object"
    )

    async with irs_kafka_fixture.record_events(
        in_topic="file_interrogation",
    ) as event_recorder:
        await irs_kafka_fixture.publish_event(**event_in)
        await irs_kafka_fixture.subscriber.run(forever=False)

    recorded_events = event_recorder.recorded_events

    assert len(recorded_events) == 1
    event = recorded_events[0]

    for key in payload_out.keys():
        assert event.payload[key] == expected_event_out.payload[key]
