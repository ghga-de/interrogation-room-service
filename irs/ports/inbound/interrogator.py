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

"""Interface for validating uploaded files"""

from abc import ABC, abstractmethod
from datetime import datetime


class InterrogatorPort(ABC):
    """
    The interface of a service for validating the content of encrypted files.
    """

    @abstractmethod
    async def interrogate(  # pylint: disable=too-many-locals
        self,
        *,
        file_id: str,
        source_object_id: str,
        source_bucket_id: str,
        public_key: str,
        upload_date: datetime,
        decrypted_size: int,
        sha256_checksum: str,
    ):
        """
        Forwards first file part to encryption key store, retrieves file encryption
        secret(s) (K_data), decrypts file and computes checksums
        """
        ...
