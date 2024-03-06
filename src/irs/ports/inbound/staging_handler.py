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
#
"""Provides helpers for S3 interaction"""

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator


class StagingHandlerPort(ABC):
    """Wrapper for object storage staging functionality."""

    class UploadNotInitializedError(RuntimeError):
        """Raised when the upload ID is not set for a method that needs it."""

    @abstractmethod
    async def init_staging(self) -> None:
        """Start staging a re-encrypted file to staging area, returns an upload id."""

    @abstractmethod
    async def stage_part(self, *, data: bytes, part_number: int) -> None:
        """Save a file part to the staging area."""

    @abstractmethod
    async def complete_staging(self, *, parts: int) -> None:
        """Complete the staging of a re-encrypted file."""

    @abstractmethod
    async def abort_staging(self) -> None:
        """Abort an ongoing multipart upload."""

    @abstractmethod
    async def delete_staged(self) -> None:
        """Remove uploaded object from staging."""

    @abstractmethod
    def retrieve_parts(self, *, offset: int = 0) -> AsyncGenerator[bytes, None]:
        """Get all parts from inbox, starting with file content at offset."""

    @abstractmethod
    async def retrieve_part(self, *, url: str, start: int, stop: int) -> bytes:
        """Get one part from inbox by range."""
