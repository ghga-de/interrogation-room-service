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
"""Test stale object deletion/inspection code."""

from datetime import timedelta

import pytest
from ghga_service_commons.utils.utc_dates import now_as_utc

from irs.adapters.outbound.dao import StagingObjectDaoConstructor
from irs.core.models import StagingObject
from irs.inject import prepare_storage_inspector
from tests.fixtures.joint import (
    STAGING_BUCKET_ID,
    JointFixture,
    joint_fixture,  # noqa: F401
    kafka_fixture,  # noqa: F401
    keypair_fixture,  # noqa: F401
    mongodb_fixture,  # noqa: F401
    s3_fixture,  # noqa: F401
    second_s3_fixture,  # noqa: F401
)
from tests.fixtures.test_files import create_test_file


@pytest.mark.asyncio(scope="session")
async def test_staging_inspector(caplog, joint_fixture: JointFixture):  # noqa: F811
    """Check storage inspector functionality."""
    # prepare storage entries
    file_1 = await create_test_file(
        bucket_id=STAGING_BUCKET_ID,
        private_key=joint_fixture.keypair.private,
        public_key=joint_fixture.keypair.public,
        s3=joint_fixture.s3,
    )
    file_2 = await create_test_file(
        bucket_id=STAGING_BUCKET_ID,
        private_key=joint_fixture.keypair.private,
        public_key=joint_fixture.keypair.public,
        s3=joint_fixture.second_s3,
    )
    file_3 = await create_test_file(
        bucket_id=STAGING_BUCKET_ID,
        private_key=joint_fixture.keypair.private,
        public_key=joint_fixture.keypair.public,
        s3=joint_fixture.s3,
    )

    # populate staging object db
    staging_object_1 = StagingObject(
        file_id=file_1.file_id,
        object_id=file_1.file_object.object_id,
        storage_alias=joint_fixture.endpoint_aliases.node1,
    )
    staging_object_2 = StagingObject(
        file_id=file_2.file_id,
        object_id=file_2.file_object.object_id,
        storage_alias=joint_fixture.endpoint_aliases.node2,
    )

    # modify second object to be recognized as stale
    staging_object_2.creation_date = now_as_utc() - timedelta(
        minutes=joint_fixture.config.object_stale_after_minutes
    )

    dao_factory = joint_fixture.mongodb.dao_factory
    staging_object_dao = await StagingObjectDaoConstructor.construct(
        dao_factory=dao_factory
    )
    await staging_object_dao.insert(staging_object_1)
    await staging_object_dao.insert(staging_object_2)

    # reset captured logs
    caplog.clear()

    # run inspector
    async with prepare_storage_inspector(
        config=joint_fixture.config
    ) as staging_inspector:
        await staging_inspector.check_buckets()

    assert len(caplog.messages) == 2
    assert (
        f"Stale object '{file_2.file_object.object_id}' found for file '{file_2.file_id}' in bucket '{STAGING_BUCKET_ID}' of storage '{joint_fixture.endpoint_aliases.node2}'."
        in caplog.messages
    )
    assert (
        f"Object '{file_3.file_object.object_id}' with no corresponding DB entry found in bucket '{STAGING_BUCKET_ID}' of storage '{joint_fixture.endpoint_aliases.node1}'."
        in caplog.messages
    )
