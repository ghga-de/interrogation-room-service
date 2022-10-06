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

from hexkit.providers.akafka import KafkaEventPublisher
from pydantic import BaseModel, Field

from irs.config import CONFIG


class UploadValidationSuccessEvent(BaseModel):
    """Contains details for successfully checksum validated file processing"""

    file_id: str = Field(..., alias="file-id")
    secret_id: str = Field(..., alias="secret-id")
    offset: int
    part_size: int = Field(..., alias="part-size")
    part_checksums_md5: list[str] = Field(..., alias="part-checksums-md5")
    part_checksums_sha256: list[str] = Field(..., alias="part-checksums-sha256")
    content_checksum_sha256: str = Field(..., alias="sha256-checksum")

    class Config:
        """Needed config to allow popupalation by non-alias name"""

        allow_population_by_field_name = True


class UploadValidationFailureEvent(BaseModel):
    """Contains details in case of file checksum validation failure"""

    file_id: str = Field(..., alias="file-id")
    cause: str

    class Config:
        """Needed config to allow popupalation by non-alias name"""

        allow_population_by_field_name = True


async def produce_success_event(
    file_id: str,
    secret_id: str,
    offset: int,
    part_size: int,
    part_checksums_md5: list[str],
    part_checksums_sha256: list[str],
    content_checksum_sha256: str,
):  # pylint: disable=too-many-arguments
    """Produce and send an event if checksum validation was successful"""
    async with KafkaEventPublisher.construct(config=CONFIG) as publisher:
        type_ = "upload_validation_success"
        data = UploadValidationSuccessEvent(
            file_id=file_id,
            secret_id=secret_id,
            offset=offset,
            part_size=part_size,
            part_checksums_md5=part_checksums_md5,
            part_checksums_sha256=part_checksums_sha256,
            content_checksum_sha256=content_checksum_sha256,
        ).dict()
        publisher.publish(paylod=data, type_=type_, key=file_id, topic=CONFIG.topic)


async def produce_failure_event(file_id: str, cause: str = "Checksum mismatch"):
    """
    Produce and send an event if checksum validation failed due to mismatch or
    if an exception was encountered
    """
    async with KafkaEventPublisher.construct(config=CONFIG) as publisher:
        type_ = "upload_validation_failure"
        data = UploadValidationFailureEvent(file_id=file_id, cause=cause).dict()
        publisher.publish(paylod=data, type_=type_, key=file_id, topic=CONFIG.topic)
