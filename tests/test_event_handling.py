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
import hashlib
import os
import sys
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from functools import partial
from pathlib import Path

import crypt4gh.header
import crypt4gh.lib
import pytest
from ghga_event_schemas import pydantic_ as event_schemas
from ghga_service_commons.utils.temp_files import big_temp_file
from ghga_service_commons.utils.utc_dates import now_as_utc
from hexkit.providers.akafka.testutils import ExpectedEvent
from hexkit.providers.s3.testutils import FileObject
from hexkit.utils import calc_part_size

from irs.adapters.outbound.dao import FingerprintDaoConstructor
from irs.core.models import UploadReceivedFingerprint
from tests.fixtures.config import Config
from tests.fixtures.joint import (
    FILE_SIZE,
    INBOX_BUCKET_ID,
    STAGING_BUCKET_ID,
    JointFixture,
    S3Fixture,
    joint_fixture,  # noqa: F401
    kafka_fixture,  # noqa: F401
    keypair_fixture,  # noqa: F401
    mongodb_fixture,  # noqa: F401
    s3_fixture,  # noqa: F401
    second_s3_fixture,  # noqa: F401
)

EKSS_NEW_SECRET = os.urandom(32)


@dataclass
class EncryptedData:
    """Object metadata for testing purposes"""

    checksum: str
    file_id: str
    file_object: FileObject
    file_secret: bytes
    file_size: int
    upload_date: str
    offset: int


async def create_test_file(private_key: bytes, public_key: bytes, s3: S3Fixture):
    """Generate encrypted random test data using a specified keypair"""
    sys.set_int_max_str_digits(FILE_SIZE)
    with big_temp_file(FILE_SIZE) as data:
        # rewind data pointer
        data.seek(0)
        with tempfile.NamedTemporaryFile() as encrypted_file:
            upload_date = now_as_utc().isoformat()
            enc_keys = [(0, private_key, public_key)]

            crypt4gh.lib.encrypt(keys=enc_keys, infile=data, outfile=encrypted_file)

            # get unencrypted checksum
            data.seek(0)
            checksum = hashlib.sha256(data.read()).hexdigest()

            encrypted_file.seek(0)
            dec_keys = [(0, private_key, None)]
            session_keys, _ = crypt4gh.header.deconstruct(
                infile=encrypted_file, keys=dec_keys, sender_pubkey=public_key
            )
            file_secret = session_keys[0]

            offset = encrypted_file.tell()
            # Rewind file
            encrypted_file.seek(0)
            object_id = os.urandom(16).hex()
            file_id = f"F{object_id}"
            file_object = FileObject(
                file_path=Path(encrypted_file.name),
                bucket_id=INBOX_BUCKET_ID,
                object_id=object_id,
            )
            await s3.populate_file_objects([file_object])

            return EncryptedData(
                checksum=checksum,
                file_id=file_id,
                file_object=file_object,
                file_secret=file_secret,
                file_size=len(file_object.content),
                offset=offset,
                upload_date=upload_date,
            )


def incoming_irs_event(
    payload: dict[str, object], config: Config
) -> Mapping[str, object]:
    """Emulate incoming event from ucs"""
    type_ = config.upload_received_event_type
    key = payload["file_id"]
    topic = config.upload_received_event_topic
    event = {"payload": payload, "type_": type_, "key": key, "topic": topic}
    return event


def ekss_call(
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
            private_key=joint_fixture.keypair.private,
            public_key=joint_fixture.keypair.public,
            s3=s3,
        )
        ekss_patch = partial(ekss_call, data=data)

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
        event_in = incoming_irs_event(payload=payload_in, config=joint_fixture.config)

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


@pytest.mark.asyncio(scope="session")
async def test_success_event(
    monkeypatch,
    joint_fixture: JointFixture,  # noqa: F811
):
    """Test the whole pipeline from receiving an event to notifying about success"""
    for s3, endpoint_alias in (
        (joint_fixture.s3, joint_fixture.endpoint_aliases.node1),
        (joint_fixture.second_s3, joint_fixture.endpoint_aliases.node2),
    ):
        data = await create_test_file(
            private_key=joint_fixture.keypair.private,
            public_key=joint_fixture.keypair.public,
            s3=s3,
        )

        ekss_patch = partial(ekss_call, data=data)

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
        event_in = incoming_irs_event(payload=payload_in, config=joint_fixture.config)

        part_size = calc_part_size(file_size=data.file_size)

        payload_out = {
            "s3_endpoint_alias": endpoint_alias,
            "file_id": data.file_id,
            "object_id": data.file_object.object_id,
            "bucket_id": STAGING_BUCKET_ID,
            "upload_date": data.upload_date,
            "decryption_secret_id": "secret_id",
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

        # check event fingerprint is stored in DB
        mongo_dao = await FingerprintDaoConstructor.construct(
            dao_factory=joint_fixture.mongodb.dao_factory
        )
        seen_event = event_schemas.FileUploadReceived(**payload_in)
        fingerprint = UploadReceivedFingerprint.generate(seen_event)

        await mongo_dao.get_by_id(fingerprint.checksum)


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
            private_key=joint_fixture.keypair.private,
            public_key=joint_fixture.keypair.public,
            s3=s3,
        )

        ekss_patch = partial(ekss_call, data=data)

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
        event_in = incoming_irs_event(payload=payload_in, config=joint_fixture.config)

        # create db fingerprint entry
        mongo_dao = await FingerprintDaoConstructor.construct(
            dao_factory=joint_fixture.mongodb.dao_factory
        )
        seen_event = event_schemas.FileUploadReceived(**payload_in)
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
