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

import base64
import os
from typing import Any, Collection, Mapping, Tuple

import pytest
from hexkit.providers.akafka.testutils import ExpectedEvent, kafka_fixture  # noqa: F401
from hexkit.providers.s3.testutils import s3_fixture  # noqa: F401
from hexkit.utils import calc_part_size

from tests.fixtures.config import Config
from tests.fixtures.file_fixtures import encrypted_random_data  # noqa: F401
from tests.fixtures.file_fixtures import (
    FILE_ID,
    INBOX_BUCKET_ID,
    OBJECT_ID,
    STAGING_BUCKET_ID,
    EncryptedDataFixture,
)
from tests.fixtures.joint import JointFixture, joint_fixture  # noqa: F401
from tests.fixtures.keypair_fixtures import generate_keypair_fixture  # noqa: F401

EKSS_NEW_SECRET = os.urandom(32)


def incoming_irs_event(
    payload: dict[str, object], config: Config
) -> Mapping[str, Collection[str]]:
    """Emulate incoming event from ucs"""
    type_ = config.upload_received_event_type
    key = FILE_ID
    topic = config.upload_received_event_topic
    event = {"payload": payload, "type_": type_, "key": key, "topic": topic}
    return event


def incoming_payload(data: EncryptedDataFixture) -> dict[str, Any]:
    """Payload arriving at the interrogation room"""
    return {
        "file_id": FILE_ID,
        "object_id": OBJECT_ID,
        "bucket_id": INBOX_BUCKET_ID,
        "submitter_public_key": base64.b64encode(data.public_key).decode("utf-8"),
        "upload_date": data.upload_date,
        "expected_decrypted_sha256": data.checksum,
        "decrypted_size": data.file_size,
    }


@pytest.mark.asyncio
async def test_failure_event(
    monkeypatch,
    encrypted_random_data: EncryptedDataFixture,  # noqa: F811
    joint_fixture: JointFixture,  # noqa: F811
):
    """
    Test the whole pipeline from receiving an event to notifying about failure
    """

    # explicit patching required for now
    def eks_patch(
        *, file_part: bytes, public_key: bytes, api_url: str
    ) -> Tuple[bytes, bytes, str, int]:
        """Monkeypatch to emulate API Call"""
        return (
            encrypted_random_data.file_secret,
            EKSS_NEW_SECRET,
            "secret_id",
            encrypted_random_data.offset,
        )

    monkeypatch.setattr(
        "irs.core.interrogator.call_eks_api",
        eks_patch,
    )
    monkeypatch.setattr(
        "irs.adapters.inbound.s3.get_objectstorage",
        lambda: encrypted_random_data.s3_fixture.storage,
    )

    payload_in = incoming_payload(encrypted_random_data)
    # introduce invalid checksum
    payload_in["expected_decrypted_sha256"] = payload_in["expected_decrypted_sha256"][
        1:
    ]
    event_in = incoming_irs_event(payload=payload_in, config=joint_fixture.config)

    payload_out = {
        "file_id": FILE_ID,
        "bucket_id": STAGING_BUCKET_ID,
        "reason": "Checksum mismatch",
        "upload_date": encrypted_random_data.upload_date,
    }
    expected_event_out = ExpectedEvent(
        payload=payload_out,
        type_=joint_fixture.config.interrogation_failure_type,
        key=FILE_ID,
    )

    consumer = await joint_fixture.container.event_subscriber()
    async with joint_fixture.kafka.record_events(
        in_topic=joint_fixture.config.interrogation_topic,
    ) as event_recorder:
        await joint_fixture.kafka.publish_event(**event_in)
        await consumer.run(forever=False)

    recorded_events = event_recorder.recorded_events

    assert len(recorded_events) == 1
    assert recorded_events[0].payload["object_id"] != ""
    expected_event_out.payload["object_id"] = recorded_events[0].payload["object_id"]
    assert recorded_events[0].payload == expected_event_out.payload


@pytest.mark.asyncio
async def test_success_event(
    monkeypatch,
    encrypted_random_data: EncryptedDataFixture,  # noqa: F811
    joint_fixture: JointFixture,  # noqa: F811
):
    """
    Test the whole pipeline from receiving an event to notifying about success
    """

    # explicit patching required for now
    def eks_patch(
        *, file_part: bytes, public_key: bytes, api_url: str
    ) -> Tuple[bytes, bytes, str, int]:
        """Monkeypatch to emulate API Call"""
        return (
            encrypted_random_data.file_secret,
            EKSS_NEW_SECRET,
            "secret_id",
            encrypted_random_data.offset,
        )

    monkeypatch.setattr(
        "irs.core.interrogator.call_eks_api",
        eks_patch,
    )
    monkeypatch.setattr(
        "irs.adapters.inbound.s3.get_objectstorage",
        lambda: encrypted_random_data.s3_fixture.storage,
    )

    payload_in = incoming_payload(encrypted_random_data)
    event_in = incoming_irs_event(payload=payload_in, config=joint_fixture.config)

    part_size = calc_part_size(file_size=encrypted_random_data.file_size)

    payload_out = {
        "file_id": FILE_ID,
        "object_id": OBJECT_ID,
        "bucket_id": STAGING_BUCKET_ID,
        "upload_date": encrypted_random_data.upload_date,
        "decryption_secret_id": "secret_id",
        "content_offset": encrypted_random_data.offset,
        "encrypted_part_size": part_size,
        "decrypted_sha256": encrypted_random_data.checksum,
    }
    expected_event_out = ExpectedEvent(
        payload=payload_out,
        type_=joint_fixture.config.interrogation_success_type,
        key=FILE_ID,
    )

    consumer = await joint_fixture.container.event_subscriber()
    async with joint_fixture.kafka.record_events(
        in_topic=joint_fixture.config.interrogation_topic,
    ) as event_recorder:
        await joint_fixture.kafka.publish_event(**event_in)

        await consumer.run(forever=False)

    recorded_events = event_recorder.recorded_events

    assert len(recorded_events) == 1
    event = recorded_events[0]

    expected_event_out.payload["object_id"] = event.payload["object_id"]
    for key in payload_out.keys():
        assert event.payload[key] == expected_event_out.payload[key]
