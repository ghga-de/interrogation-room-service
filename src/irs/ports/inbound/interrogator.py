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

from irs.core.models import InterrogationSubject


class InterrogatorPort(ABC):
    """The interface of a service for validating the content of encrypted files."""

    @abstractmethod
    async def interrogate(self, *, subject: InterrogationSubject) -> None:
        """
        Forwards first file part to encryption key store, retrieves file encryption
        secret(s) (K_data), decrypts file and computes checksums
        """

    @abstractmethod
    async def remove_staging_object(self, *, file_id: str, storage_alias: str) -> None:
        """Remove transient object from staging once copy to permanent storage has been confirmed"""
