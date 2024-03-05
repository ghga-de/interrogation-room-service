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
"""KafkaEventPublisher for file upload validation success/failure event details"""

import json

from ghga_event_schemas import pydantic_ as event_schemas
from hexkit.protocols.eventpub import EventPublisherProtocol
from pydantic import Field
from pydantic_settings import BaseSettings

from irs.core.models import InterrogationSubject, ProcessingResult
from irs.core.staging_handler import StagingHandler
from irs.ports.outbound.event_pub import EventPublisherPort


class EventPubTanslatorConfig(BaseSettings):
    """Config for publishing file upload-related events."""

    interrogation_topic: str = Field(
        default=...,
        description=(
            "Name of the topic used for events informing about the outcome of file validations."
        ),
        examples=["file_interrogation"],
    )
    interrogation_success_type: str = Field(
        default=...,
        description=(
            "The type used for events informing about the success of a file validation."
        ),
        examples=["file_validation_success"],
    )
    interrogation_failure_type: str = Field(
        default=...,
        description=(
            "The type used for events informing about the failure of a file validation."
        ),
        examples=["file_validation_failure"],
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
        processing_result: ProcessingResult,
        staging_handler: StagingHandler,
        subject: InterrogationSubject,
    ) -> None:
        """Publish event informing that a validation was successful."""
        event_payload = event_schemas.FileUploadValidationSuccess(
            s3_endpoint_alias=subject.storage_alias,
            file_id=subject.file_id,
            object_id=staging_handler.staging.object_id,
            bucket_id=staging_handler.staging.bucket_id,
            upload_date=subject.upload_date,
            decrypted_size=subject.decrypted_size,
            decryption_secret_id=processing_result.secret_id,
            content_offset=processing_result.offset,
            encrypted_part_size=staging_handler.part_size,
            encrypted_parts_md5=processing_result.checksums.part_checksums_md5,
            encrypted_parts_sha256=processing_result.checksums.part_checksums_sha256,
            decrypted_sha256=processing_result.checksums.content_checksum_sha256,
        )
        await self._provider.publish(
            payload=json.loads(event_payload.model_dump_json()),
            type_=self._config.interrogation_success_type,
            topic=self._config.interrogation_topic,
            key=subject.file_id,
        )

    async def publish_validation_failure(
        self,
        *,
        staging_handler: StagingHandler,
        subject: InterrogationSubject,
        cause: str = "Checksum mismatch",
    ) -> None:
        """Publish event informing that a validation was not successful."""
        event_payload = event_schemas.FileUploadValidationFailure(
            s3_endpoint_alias=subject.storage_alias,
            file_id=subject.file_id,
            object_id=staging_handler.staging.object_id,
            bucket_id=staging_handler.staging.bucket_id,
            upload_date=subject.upload_date,
            reason=cause,
        )
        await self._provider.publish(
            payload=json.loads(event_payload.model_dump_json()),
            type_=self._config.interrogation_failure_type,
            topic=self._config.interrogation_topic,
            key=subject.file_id,
        )
