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

"""Interfaces for event publishing adapters and the exception they may throw."""

from abc import ABC, abstractmethod

from irs.core.models import InterrogationSubject, ProcessingResult
from irs.core.staging_handler import StagingHandler


class EventPublisherPort(ABC):
    """An interface for an adapter that publishes events happening to this service."""

    @abstractmethod
    async def publish_validation_success(
        self,
        *,
        processing_result: ProcessingResult,
        staging_handler: StagingHandler,
        subject: InterrogationSubject,
    ) -> None:
        """Publish event informing that a validation was successful."""
        ...

    @abstractmethod
    async def publish_validation_failure(
        self,
        *,
        staging_handler: StagingHandler,
        subject: InterrogationSubject,
        cause: str = "Checksum mismatch",
    ) -> None:
        """Publish event informing that a validation was not successful."""
        ...
