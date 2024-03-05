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
"""Tests for event handling"""

import base64
import os
from collections.abc import Mapping
from copy import deepcopy
from functools import partial
from typing import Any

import pytest
from ghga_service_commons.utils.utc_dates import now_as_utc
from hexkit.protocols.dao import ResourceNotFoundError
from hexkit.providers.akafka.testutils import ExpectedEvent
from hexkit.utils import calc_part_size

from irs.adapters.outbound.dao import (
    FingerprintDaoConstructor,
    StagingObjectDaoConstructor,
)
from irs.core.models import InterrogationSubject, UploadReceivedFingerprint
from tests.fixtures.config import Config
from tests.fixtures.joint import (
    INBOX_BUCKET_ID,
    STAGING_BUCKET_ID,
    JointFixture,
    joint_fixture,  # noqa: F401
    kafka_fixture,  # noqa: F401
    keypair_fixture,  # noqa: F401
    mongodb_fixture,  # noqa: F401
    s3_fixture,  # noqa: F401
    second_s3_fixture,  # noqa: F401
)
from tests.fixtures.test_files import EncryptedData, create_test_file

EKSS_NEW_SECRET = os.urandom(32)


def _incoming_event_file_registered(
    payload: dict[str, object], config: Config
) -> Mapping[str, object]:
    """Emulate incoming file registered event"""
    type_ = config.file_registered_event_type
    key = payload["file_id"]
    topic = config.file_registered_event_type
    event = {"payload": payload, "type_": type_, "key": key, "topic": topic}
    return event


def _incoming_event_upload_received(
    payload: dict[str, object], config: Config
) -> Mapping[str, object]:
    """Emulate incoming upload received event"""
    type_ = config.upload_received_event_type
    key = payload["file_id"]
    topic = config.upload_received_event_topic
    event = {"payload": payload, "type_": type_, "key": key, "topic": topic}
    return event


def _populate_subject(payload: dict[str, Any]) -> InterrogationSubject:
    """Convert payload to internally used model."""
    fields = deepcopy(payload)

    inbox_bucket_id = fields.pop("bucket_id")
    inbox_object_id = fields.pop("object_id")
    storage_alias = fields.pop("s3_endpoint_alias")

    fields["storage_alias"] = storage_alias
    fields["inbox_bucket_id"] = inbox_bucket_id
    fields["inbox_object_id"] = inbox_object_id

    return InterrogationSubject(**fields)


def _ekss_call(
    *, data: EncryptedData, file_part: bytes, public_key: bytes, api_url: str
) -> tuple[bytes, bytes, str, int]:
    """Monkeypatch to emulate API Call"""
    return (
        data.file_secret,
        EKSS_NEW_SECRET,
        "secret_id",
        data.offset,
    )


@pytest.mark.asyncio(scope="session")
async def test_failure_event(
    monkeypatch,
    joint_fixture: JointFixture,  # noqa: F811
):
    """Test the whole pipeline from receiving an event to notifying about failure"""
    for s3, endpoint_alias in (
        (joint_fixture.s3, joint_fixture.endpoint_aliases.node1),
        (joint_fixture.second_s3, joint_fixture.endpoint_aliases.node2),
    ):
        data = await create_test_file(
            bucket_id=INBOX_BUCKET_ID,
            private_key=joint_fixture.keypair.private,
            public_key=joint_fixture.keypair.public,
            s3=s3,
        )
        ekss_patch = partial(_ekss_call, data=data)

        monkeypatch.setattr(
            "irs.core.interrogator.call_eks_api",
            ekss_patch,
        )

        payload_in = {
            "s3_endpoint_alias": endpoint_alias,
            "file_id": data.file_id,
            "object_id": data.file_object.object_id,
            "bucket_id": INBOX_BUCKET_ID,
            "submitter_public_key": base64.b64encode(
                joint_fixture.keypair.public
            ).decode("utf-8"),
            "upload_date": data.upload_date,
            "expected_decrypted_sha256": data.checksum,
            "decrypted_size": data.file_size,
        }

        # introduce invalid checksum
        payload_in["expected_decrypted_sha256"] = payload_in[
            "expected_decrypted_sha256"
        ][1:]
        event_in = _incoming_event_upload_received(
            payload=payload_in, config=joint_fixture.config
        )

        payload_out = {
            "s3_endpoint_alias": endpoint_alias,
            "file_id": data.file_id,
            "bucket_id": STAGING_BUCKET_ID,
            "reason": "Checksum mismatch",
            "upload_date": data.upload_date,
        }
        expected_event_out = ExpectedEvent(
            payload=payload_out,
            type_=joint_fixture.config.interrogation_failure_type,
            key=data.file_id,
        )

        async with joint_fixture.kafka.record_events(
            in_topic=joint_fixture.config.interrogation_topic,
        ) as event_recorder:
            await joint_fixture.kafka.publish_event(**event_in)
            await joint_fixture.event_subscriber.run(forever=False)

        recorded_events = event_recorder.recorded_events

        assert len(recorded_events) == 1
        assert recorded_events[0].payload["object_id"] != ""
        expected_event_out.payload["object_id"] = recorded_events[0].payload[
            "object_id"
        ]
        assert recorded_events[0].payload == expected_event_out.payload

        # check staging object dao state
        staging_object_dao = await StagingObjectDaoConstructor.construct(
            dao_factory=joint_fixture.mongodb.dao_factory
        )
        with pytest.raises(ResourceNotFoundError):
            await staging_object_dao.get_by_id(id_=data.file_id)

        # check fingerprint is created for unsuccessful processing
        fingerprint_dao = await FingerprintDaoConstructor.construct(
            dao_factory=joint_fixture.mongodb.dao_factory
        )

        seen_event = _populate_subject(payload_in)
        fingerprint = UploadReceivedFingerprint.generate(seen_event)

        await fingerprint_dao.get_by_id(fingerprint.checksum)


@pytest.mark.asyncio(scope="session")
async def test_success_event(
    monkeypatch,
    joint_fixture: JointFixture,  # noqa: F811
):
    """Test the whole pipeline from receiving an event to notifying about success"""
    secret_id = "secret_id"
    encrypted_parts_md5 = ["abc", "def", "ghi"]
    encrypted_parts_sha256 = ["abc", "def", "ghi"]

    for s3, endpoint_alias in (
        (joint_fixture.s3, joint_fixture.endpoint_aliases.node1),
        (joint_fixture.second_s3, joint_fixture.endpoint_aliases.node2),
    ):
        data = await create_test_file(
            bucket_id=INBOX_BUCKET_ID,
            private_key=joint_fixture.keypair.private,
            public_key=joint_fixture.keypair.public,
            s3=s3,
        )

        ekss_patch = partial(_ekss_call, data=data)

        monkeypatch.setattr(
            "irs.core.interrogator.call_eks_api",
            ekss_patch,
        )

        payload_in = {
            "s3_endpoint_alias": endpoint_alias,
            "file_id": data.file_id,
            "object_id": data.file_object.object_id,
            "bucket_id": INBOX_BUCKET_ID,
            "submitter_public_key": base64.b64encode(
                joint_fixture.keypair.public
            ).decode("utf-8"),
            "upload_date": data.upload_date,
            "expected_decrypted_sha256": data.checksum,
            "decrypted_size": data.file_size,
        }
        event_in = _incoming_event_upload_received(
            payload=payload_in, config=joint_fixture.config
        )

        part_size = calc_part_size(file_size=data.file_size)

        payload_out = {
            "s3_endpoint_alias": endpoint_alias,
            "file_id": data.file_id,
            "object_id": data.file_object.object_id,
            "bucket_id": STAGING_BUCKET_ID,
            "upload_date": data.upload_date,
            "decryption_secret_id": secret_id,
            "content_offset": data.offset,
            "encrypted_part_size": part_size,
            "decrypted_sha256": data.checksum,
        }
        expected_event_out = ExpectedEvent(
            payload=payload_out,
            type_=joint_fixture.config.interrogation_success_type,
            key=data.file_id,
        )

        async with joint_fixture.kafka.record_events(
            in_topic=joint_fixture.config.interrogation_topic,
        ) as event_recorder:
            await joint_fixture.kafka.publish_event(**event_in)

            await joint_fixture.event_subscriber.run(forever=False)

        recorded_events = event_recorder.recorded_events

        assert len(recorded_events) == 1
        event = recorded_events[0]

        expected_event_out.payload["object_id"] = event.payload["object_id"]
        for key in payload_out:
            assert event.payload[key] == expected_event_out.payload[key]

        # check staging object dao state and ensure, object actually exists in storage
        staging_object_dao = await StagingObjectDaoConstructor.construct(
            dao_factory=joint_fixture.mongodb.dao_factory
        )
        staging_object = await staging_object_dao.get_by_id(id_=data.file_id)

        assert await s3.storage.does_object_exist(
            bucket_id=STAGING_BUCKET_ID, object_id=staging_object.object_id
        )

        # check event fingerprint is stored in DB
        mongo_dao = await FingerprintDaoConstructor.construct(
            dao_factory=joint_fixture.mongodb.dao_factory
        )

        seen_event = _populate_subject(payload_in)
        fingerprint = UploadReceivedFingerprint.generate(seen_event)

        await mongo_dao.get_by_id(fingerprint.checksum)

        # check removal on successful registration
        payload_remove_object = {
            "file_id": data.file_id,
            "object_id": data.file_object.object_id,
            "bucket_id": data.file_object.bucket_id,
            "s3_endpoint_alias": endpoint_alias,
            "decrypted_size": data.file_size,
            "decryption_secret_id": secret_id,
            "content_offset": data.offset,
            "encrypted_part_size": part_size,
            "encrypted_parts_md5": encrypted_parts_md5,
            "encrypted_parts_sha256": encrypted_parts_sha256,
            "decrypted_sha256": data.checksum,
            "upload_date": now_as_utc().isoformat(),
        }

        remove_event = _incoming_event_file_registered(
            payload=payload_remove_object, config=joint_fixture.config
        )

        async with joint_fixture.kafka.record_events(
            in_topic=joint_fixture.config.file_registered_event_topic,
        ) as event_recorder:
            await joint_fixture.kafka.publish_event(**remove_event)
            await joint_fixture.event_subscriber.run(forever=False)

        assert len(event_recorder.recorded_events) == 1

        assert not await s3.storage.does_object_exist(
            bucket_id=STAGING_BUCKET_ID, object_id=staging_object.object_id
        )


@pytest.mark.asyncio(scope="session")
async def test_fingerprint_already_present(
    caplog,
    monkeypatch,
    joint_fixture: JointFixture,  # noqa: F811
):
    """Test the whole pipeline from receiving an event to notifying about success"""
    for s3, endpoint_alias in (
        (joint_fixture.s3, joint_fixture.endpoint_aliases.node1),
        (joint_fixture.second_s3, joint_fixture.endpoint_aliases.node2),
    ):
        data = await create_test_file(
            bucket_id=INBOX_BUCKET_ID,
            private_key=joint_fixture.keypair.private,
            public_key=joint_fixture.keypair.public,
            s3=s3,
        )

        ekss_patch = partial(_ekss_call, data=data)

        monkeypatch.setattr(
            "irs.core.interrogator.call_eks_api",
            ekss_patch,
        )

        payload_in = {
            "s3_endpoint_alias": endpoint_alias,
            "file_id": data.file_id,
            "object_id": data.file_object.object_id,
            "bucket_id": INBOX_BUCKET_ID,
            "submitter_public_key": base64.b64encode(
                joint_fixture.keypair.public
            ).decode("utf-8"),
            "upload_date": data.upload_date,
            "expected_decrypted_sha256": data.checksum,
            "decrypted_size": data.file_size,
        }
        event_in = _incoming_event_upload_received(
            payload=payload_in, config=joint_fixture.config
        )

        # create db fingerprint entry
        mongo_dao = await FingerprintDaoConstructor.construct(
            dao_factory=joint_fixture.mongodb.dao_factory
        )
        seen_event = _populate_subject(payload_in)
        fingerprint = UploadReceivedFingerprint.generate(seen_event)

        await mongo_dao.insert(fingerprint)

        # reset captured logs
        caplog.clear()
        await joint_fixture.kafka.publish_event(**event_in)
        await joint_fixture.event_subscriber.run(forever=False)
        assert (
            f"Payload for file ID '{seen_event.file_id}' has already been processed."
            in caplog.messages
        )
