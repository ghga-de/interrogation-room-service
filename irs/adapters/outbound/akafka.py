# Copyright 2021 - 2023 Universität Tübingen, DKFZ and EMBL
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
"""KafkaEventPublisher for file upload validation success/failure event details"""

import json
from datetime import datetime

from ghga_event_schemas import pydantic_ as event_schemas
from hexkit.protocols.eventpub import EventPublisherProtocol
from pydantic import BaseSettings, Field

from irs.ports.outbound.event_pub import EventPublisherPort


class EventPubTanslatorConfig(BaseSettings):
    """Config for publishing file upload-related events."""

    interrogation_topic: str = Field(
        ...,
        description=(
            "Name of the topic used for events informing about the outcome of file validations."
        ),
        example="file_interrogation",
    )
    interrogation_success_type: str = Field(
        ...,
        description=(
            "The type used for events informing about the success of a file validation."
        ),
        example="file_validation_success",
    )
    interrogation_failure_type: str = Field(
        ...,
        description=(
            "The type used for events informing about the failure of a file validation."
        ),
        example="file_validation_failure",
    )


class EventPublisher(EventPublisherPort):
    """Contains details for event publishing at the end of file validation"""

    def __init__(
        self, *, config: EventPubTanslatorConfig, provider: EventPublisherProtocol
    ):
        """Initialize with a suitable protocol provider."""

        self._config = config
        self._provider = provider

    async def publish_validation_success(
        self,
        *,
        file_id: str,
        upload_date: datetime,
        secret_id: str,
        offset: int,
        part_size: int,
        decrypted_size: int,
        part_checksums_md5: list[str],
        part_checksums_sha256: list[str],
        content_checksum_sha256: str,
    ):  # pylint: disable=too-many-arguments
        """Produce and send an event if checksum validation was successful"""

        event_payload = event_schemas.FileUploadValidationSuccess(
            file_id=file_id,
            upload_date=upload_date,
            decrypted_size=decrypted_size,
            decryption_secret_id=secret_id,
            content_offset=offset,
            encrypted_part_size=part_size,
            encrypted_parts_md5=part_checksums_md5,
            encrypted_parts_sha256=part_checksums_sha256,
            decrypted_sha256=content_checksum_sha256,
        )
        await self._provider.publish(
            payload=json.loads(event_payload.json()),
            type_=self._config.interrogation_success_type,
            topic=self._config.interrogation_topic,
            key=file_id,
        )

    async def publish_validation_failure(
        self,
        *,
        file_id: str,
        upload_date: datetime,
        cause: str = "Checksum mismatch",
    ):
        """
        Produce and send an event if checksum validation failed due to mismatch or
        if an exception was encountered
        """
        event_payload = event_schemas.FileUploadValidationFailure(
            file_id=file_id,
            upload_date=upload_date,
            reason=cause,
        )
        await self._provider.publish(
            payload=json.loads(event_payload.json()),
            type_=self._config.interrogation_failure_type,
            topic=self._config.interrogation_topic,
            key=file_id,
        )
