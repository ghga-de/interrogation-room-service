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

import asyncio
import base64
from typing import Any, Collection, Mapping

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
from tests.fixtures.kafka_fixtures import IRSKafkaFixture, kafka_fixture  # noqa: F401
from tests.fixtures.keypair_fixtures import generate_keypair_fixture  # noqa: F401


def incoming_irs_event(payload: dict[str, object]) -> Mapping[str, Collection[str]]:
    """Generate test event for sanity check"""
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
    encrypted_random_data: EncryptedDataFixture,  # noqa: F811
    kafka_fixture: IRSKafkaFixture,  # noqa: F811
):
    """
    Test the whole pipeline from receiving an event to notifying about success/failure
    """
    upload_task = asyncio.create_task(kafka_fixture.subscriber.run(forever=False))

    payload_in = incoming_payload(encrypted_random_data)
    # introduce invalid checksum
    payload_in["sha256_checksum"] = payload_in["sha256_checksum"][1:]
    expected_event_in = ExpectedEvent(payload=payload_in, type_="ucs")
    event_in = incoming_irs_event(payload=payload_in)

    payload_out = {
        "file_id": OBJECT_ID,
        "cause": "Checksum mismatch",
    }
    expected_event_out = ExpectedEvent(
        payload=payload_out, type_="upload_validation_failure"
    )

    async with kafka_fixture.expect_events(
        events=[expected_event_in],
        in_topic="file_upload_received",
        with_key=OBJECT_ID,
    ):
        async with kafka_fixture.expect_events(
            events=[expected_event_out],
            in_topic="file_interrogation",
            with_key=OBJECT_ID,
        ):
            await kafka_fixture.publish_event(**event_in)
            await upload_task


@pytest.mark.asyncio
async def test_success_event(
    encrypted_random_data: EncryptedDataFixture,  # noqa: F811
    kafka_fixture: IRSKafkaFixture,  # noqa: F811
):
    """
    Test the whole pipeline from receiving an event to notifying about success/failure
    """
    upload_task = asyncio.create_task(kafka_fixture.subscriber.run(forever=False))

    payload_in = incoming_payload(encrypted_random_data)
    expected_event_in = ExpectedEvent(payload=payload_in, type_="ucs")
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

    async with kafka_fixture.expect_events(
        events=[expected_event_in],
        in_topic="file_upload_received",
        with_key="test-object",
    ):
        async with kafka_fixture.expect_events(
            events=[expected_event_out],
            in_topic="file_interrogation",
            with_key="test-object",
        ):
            await kafka_fixture.publish_event(**event_in)
            await upload_task
