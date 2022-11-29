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

"""Interface for validating uploaded files"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Tuple


class UploadHandlerPort(ABC):
    """A Service that validates uploaded files"""

    @abstractmethod
    async def interrogate(  # pylint: disable=too-many-locals
        self,
        *,
        object_id: str,
        public_key: str,
        upload_date: datetime,
        object_size: int,
        sha256_checksum: str,
    ):
        """
        Forwards first file part to encryption key store, retrieves file encryption
        secret(s) (K_data), decrypts file and computes checksums
        """
        ...

    @abstractmethod
    async def _compute_checksums(  # pylint: disable=too-many-locals
        self,
        *,
        download_url: str,
        secret: bytes,
        object_size: int,
        part_size: int,
        offset: int,
    ) -> Tuple[List[str], List[str], str]:
        """Compute total unencrypted file checksum and encrypted part checksums"""
        ...

    @abstractmethod
    def _get_segments(self, *, file_part: bytes) -> Tuple[List[bytes], bytes]:
        """Chunk file part into decryptable segments"""
        ...

    @abstractmethod
    def _get_part_checksums(self, *, file_part: bytes):
        """Compute md5 and sha256 for encrypted part"""
        ...
