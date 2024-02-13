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

from irs.ports.inbound.bucket_cleaner import BucketCleanerPort
from irs.ports.inbound.interrogator import InterrogatorPort


class EventSubTanslatorConfig(BaseSettings):
    """Config for publishing file upload-related events."""

    file_registered_event_topic: str = Field(
        default=...,
        description="Name of the topic used for events indicating that a new file has"
        + " been internally registered.",
        examples=["internal_file_registry"],
    )
    file_registered_event_type: str = Field(
        default=...,
        description="The type used for events indicating that a new file has"
        + " been internally registered.",
        examples=["file_registered"],
    )
    upload_received_event_topic: str = Field(
        default=...,
        description="Name of the topic to publish events that inform about new file uploads.",
        examples=["file_uploads"],
    )
    upload_received_event_type: str = Field(
        default=...,
        description="The type to use for events that inform about new file uploads.",
        examples=["file_upload_received"],
    )


class EventSubTranslator(EventSubscriberProtocol):
    """A triple hexagonal translator compatible with the EventSubscriberProtocol that
    is used to received events relevant for file uploads.
    """

    def __init__(
        self,
        config: EventSubTanslatorConfig,
        bucket_cleaner: BucketCleanerPort,
        interrogator: InterrogatorPort,
    ):
        """Initialize with config parameters and core dependencies."""
        self._config = config
        self._bucket_cleaner = bucket_cleaner
        self._interrogator = interrogator

        self.topics_of_interest = [
            config.upload_received_event_topic,
        ]
        self.types_of_interest = [
            config.upload_received_event_type,
        ]

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
        if type_ == self._config.upload_received_event_type:
            await self._consume_upload_received(payload=payload)
        elif type_ == "":
            ...
        else:
            raise RuntimeError(f"Unexpected event of type: {type_}")

    async def _consume_upload_received(self, *, payload: JsonObject):
        """TODO"""
        validated_payload = get_validated_payload(
            payload=payload,
            schema=event_schemas.FileUploadReceived,
        )

        await self._interrogator.interrogate(payload=validated_payload)

    async def _consume_file_internally_registered(self, *, payload: JsonObject):
        """TODO"""
        validated_payload = get_validated_payload(
            payload=payload,
            schema=event_schemas.FileInternallyRegistered,
        )

        await self._bucket_cleaner.clean(payload=validated_payload)
