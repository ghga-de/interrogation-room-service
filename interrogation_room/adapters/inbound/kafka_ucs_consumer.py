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

import logging
from typing import Optional

from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.eventsub import EventSubscriberProtocol
from pydantic import BaseModel, Field

from interrogation_room.core.upload_handler import process_new_upload


class FileUploadCompletedEvent(BaseModel):
    """
    Details for incoming file_upload_completed event from ucs - declare unused fields
    as optional until we integrate with UCS
    """

    creation_date: Optional[str] = Field(alias="creation-date")
    file_id: str = Field(..., alias="file-id")
    format: Optional[str]
    grouping_label: Optional[str] = Field(alias="grouping-label")
    sha256_checksum: str = Field(..., alias="sha256-checksum")
    size: int
    timestamp: Optional[str]
    update_date: Optional[str] = Field(alias="update-date")

    class Config:
        """Needed config to allow popupalation by non-alias name"""

        allow_population_by_field_name = True


class UcsUploadedProtocol(EventSubscriberProtocol):
    """
    EventSubscriber Implementation processing relevant information from UCS
    file_upload_completed event
    """

    topics_of_interest: list[Ascii] = ["file_upload_received"]
    types_of_interest: list[Ascii] = ["ucs"]

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
        if type_ not in self.types_of_interest:
            logging.warning(
                "Type '%s' not in types of interest. Skipping event.", type_
            )
        elif topic not in self.topics_of_interest:
            logging.warning(
                "Topic '%s' not in topics of interest. Skipping event.", topic
            )
        else:
            object_id = payload["file-id"]
            object_size = payload["size"]
            # do we need to handle grouping label for prefixes in inbox?
            checksum = payload["sha256-checksum"]
            await process_new_upload(
                object_id=object_id, object_size=object_size, checksum=checksum
            )
