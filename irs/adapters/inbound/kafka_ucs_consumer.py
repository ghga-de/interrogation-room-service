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
"""KafkaEventSubscriber receiving events from UCS and validating file uploads"""

from typing import Optional

from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.eventsub import EventSubscriberProtocol
from pydantic import BaseModel, Field

from irs.core.upload_handler import process_new_upload


class FileUploadCompletedEvent(BaseModel):
    """
    Details for incoming file_upload_completed event from ucs - remove unused fields
    until we integrate with UCS
    """

    file_id: str = Field(..., alias="file-id")
    public_key: str = Field(..., alias="public-key")
    grouping_label: Optional[str] = Field(alias="grouping-label")
    sha256_checksum: str = Field(..., alias="sha256-checksum")
    size: int

    class Config:
        """Needed config to allow popupalation by non-alias name"""

        allow_population_by_field_name = True


class UploadTaskReceiver(EventSubscriberProtocol):
    """
    EventSubscriber Implementation processing relevant information from UCS
    file_upload_completed event
    """

    topics_of_interest: list[Ascii] = ["file_upload_received"]
    types_of_interest: list[Ascii] = ["ucs"]

    async def _consume_validated(  # pylint: disable=unused-argument,no-self-use
        self, *, payload: JsonObject, type_: Ascii, topic: Ascii
    ) -> None:
        """
        Receive and process an event with already validated topic and type.

        Args:
            payload (JsonObject): The data/payload to send with the event.
            type_ (str): The type of the event.
            topic (str): Name of the topic the event was published to.
        """
        object_id = payload["file-id"]
        object_size = payload["size"]
        public_key = payload["public-key"]
        # do we need to handle grouping label for prefixes in inbox?
        checksum = payload["sha256-checksum"]
        await process_new_upload(
            object_id=object_id,
            object_size=object_size,
            public_key=public_key,
            checksum=checksum,
        )
