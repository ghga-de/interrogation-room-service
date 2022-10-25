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

from irs.core.upload_handler import compute_checksums
from tests.fixtures.file_fixtures import encrypted_random_data  # noqa: F401
from tests.fixtures.file_fixtures import (
    BUCKET_ID,
    OBJECT_ID,
    PART_SIZE,
    EncryptedDataFixture,
)
from tests.fixtures.kafka_fixtures import (  # noqa: F401
    IRSKafkaFixture,
    irs_kafka_fixture,
)
from tests.fixtures.keypair_fixtures import generate_keypair_fixture  # noqa: F401


def incoming_irs_event(payload: dict[str, object]) -> Mapping[str, Collection[str]]:
    """Emulate incoming event from ucs"""
    type_ = "ucs"
    key = OBJECT_ID
    topic = "file_upload_received"
    event = {"payload": payload, "type_": type_, "key": key, "topic": topic}
    return event


def incoming_payload(data: EncryptedDataFixture) -> dict[str, Any]:
    """Payload arriving at the interrogation room"""
    return {
        "file_id": OBJECT_ID,
        "public_key": base64.b64encode(data.public_key).decode("utf-8"),
        "sha256_checksum": data.checksum,
        "size": data.file_size,
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
    payload_in["sha256_checksum"] = payload_in["sha256_checksum"][1:]
    event_in = incoming_irs_event(payload=payload_in)

    payload_out = {
        "file_id": OBJECT_ID,
        "cause": "Checksum mismatch",
    }
    expected_event_out = ExpectedEvent(
        payload=payload_out, type_="upload_validation_failure"
    )

    async with irs_kafka_fixture.expect_events(
        events=[expected_event_out],
        in_topic="file_interrogation",
        with_key=OBJECT_ID,
    ):
        await irs_kafka_fixture.publish_event(**event_in)
        await irs_kafka_fixture.subscriber.run(forever=False)


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

    download_url = (
        await encrypted_random_data.s3_fixture.storage.get_object_download_url(
            bucket_id=BUCKET_ID, object_id=OBJECT_ID
        )
    )
    part_checksums_md5, part_checksums_sha256, _ = await compute_checksums(
        download_url=download_url,
        secret=encrypted_random_data.file_secret,
        object_size=encrypted_random_data.file_size,
        offset=encrypted_random_data.offset,
    )

    payload_out = {
        "file_id": OBJECT_ID,
        "secret_id": "secret_id",
        "offset": encrypted_random_data.offset,
        "part_size": PART_SIZE,
        "part_checksums_md5": part_checksums_md5,
        "part_checksums_sha256": part_checksums_sha256,
        "content_checksum_sha256": encrypted_random_data.checksum,
    }
    expected_event_out = ExpectedEvent(
        payload=payload_out, type_="upload_validation_success"
    )

    async with irs_kafka_fixture.expect_events(
        events=[expected_event_out],
        in_topic="file_interrogation",
        with_key="test-object",
    ):
        await irs_kafka_fixture.publish_event(**event_in)
        await irs_kafka_fixture.subscriber.run(forever=False)
