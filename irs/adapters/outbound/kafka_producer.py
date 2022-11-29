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
"""KafkaEventPublisher for file upload validation success/failure event details"""

from datetime import datetime

from ghga_event_schemas import pydantic_ as event_schemas
from hexkit.protocols.eventpub import EventPublisherProtocol
from pydantic import BaseSettings, Field

from irs.ports.outbound.event_pub import EventPublisherPort


class EventPubTanslatorConfig(BaseSettings):
    """Config for publishing file upload-related events."""

    files_to_register_success_type: str = Field(
        "file_validation_success",
        description=(
            "The name of the topic to receive events informing about new files to register."
        ),
        example="file_validation_success",
    )
    files_to_register_failure_type: str = Field(
        "file_validation_failure",
        description=(
            "The name of the topic to receive events informing about new files to register."
        ),
        example="file_validation_failure",
    )
    files_to_register_topic: str = Field(
        "file_interrogation",
        description=("The type used for events informing about new files to register."),
        example="file_interrogation",
    )


class EventPublisher(EventPublisherPort):
    """Contains details for successfully checksum validated file processing"""

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
        ).dict()
        await self._provider.publish(
            payload=event_payload,
            type_=self._config.files_to_register_success_type,
            topic=self._config.files_to_register_topic,
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
        ).dict()
        await self._provider.publish(
            payload=event_payload,
            type_=self._config.files_to_register_failure_type,
            topic=self._config.files_to_register_topic,
            key=file_id,
        )
