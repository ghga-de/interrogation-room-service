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

"""Contains DAOs for database access."""

from hexkit.protocols.dao import DaoFactoryProtocol

from irs.core import models
from irs.ports.outbound.dao import FingerprintDaoPort, StagingObjectDaoPort


class FingerprintDaoConstructor:
    """Constructor compatible with the hexkit.inject.AsyncConstructable type. Used to
    construct a DAO for interacting with the database.
    """

    @staticmethod
    async def construct(*, dao_factory: DaoFactoryProtocol) -> FingerprintDaoPort:
        """Get a DAO using the specified provider."""
        return await dao_factory.get_dao(
            name="fingerprints",
            dto_model=models.UploadReceivedFingerprint,
            id_field="checksum",
        )


class StagingObjectDaoConstructor:
    """Constructor compatible with the hexkit.inject.AsyncConstructable type. Used to
    construct a DAO for interacting with the database.
    """

    @staticmethod
    async def construct(*, dao_factory: DaoFactoryProtocol) -> StagingObjectDaoPort:
        """Get a DAO using the specified provider."""
        return await dao_factory.get_dao(
            name="staging_objects",
            dto_model=models.StagingObject,
            id_field="file_id",
        )
