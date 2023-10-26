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
"""KafkaEventSubscriber receiving events from UCS and validating file uploads"""

from ghga_event_schemas import pydantic_ as event_schemas
from ghga_event_schemas.validation import get_validated_payload
from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.eventsub import EventSubscriberProtocol
from pydantic import Field
from pydantic_settings import BaseSettings

from irs.ports.inbound.interrogator import InterrogatorPort


class EventSubTanslatorConfig(BaseSettings):
    """Config for publishing file upload-related events."""

    upload_received_event_topic: str = Field(
        ...,
        description="Name of the topic to publish events that inform about new file uploads.",
        examples=["file_uploads"],
    )
    upload_received_event_type: str = Field(
        ...,
        description="The type to use for events that inform about new file uploads.",
        examples=["file_upload_received"],
    )


class EventSubTranslator(EventSubscriberProtocol):
    """A triple hexagonal translator compatible with the EventSubscriberProtocol that
    is used to received events relevant for file uploads.
    """

    def __init__(self, config: EventSubTanslatorConfig, interrogator: InterrogatorPort):
        """Initialize with config parameters and core dependencies."""
        self.topics_of_interest = [
            config.upload_received_event_topic,
        ]
        self.types_of_interest = [
            config.upload_received_event_type,
        ]

        self._interrogator = interrogator

        self._config = config

    async def _consume_validated(
        self, *, payload: JsonObject, type_: Ascii, topic: Ascii
    ) -> None:
        """
        Receive and process an event with already validated topic and type.

        Args:
            payload (JsonObject): The data/payload to send with the event.
            type_ (str): The type of the event.
            topic (str): Name of the topic the event was published to.
        """
        validated_payload = get_validated_payload(
            payload=payload,
            schema=event_schemas.FileUploadReceived,
        )

        await self._interrogator.interrogate(
            file_id=validated_payload.file_id,
            source_object_id=validated_payload.object_id,
            source_bucket_id=validated_payload.bucket_id,
            public_key=validated_payload.submitter_public_key,
            upload_date=validated_payload.upload_date,
            decrypted_size=validated_payload.decrypted_size,
            sha256_checksum=validated_payload.expected_decrypted_sha256,
        )
