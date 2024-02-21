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
"""A service for validating uploaded files"""

import contextlib
import uuid
from logging import getLogger

from crypt4gh.lib import CryptoError
from ghga_event_schemas import pydantic_ as event_schemas
from ghga_service_commons.utils.multinode_storage import S3ObjectStorages
from hexkit.protocols.dao import ResourceNotFoundError
from hexkit.utils import calc_part_size

from irs.adapters.outbound.http.api_calls import call_eks_api
from irs.adapters.outbound.http.exceptions import KnownError
from irs.config import CONFIG
from irs.core.models import StagingObject, UploadReceivedFingerprint
from irs.core.segment_processor import CipherSegmentProcessor
from irs.core.staging_handler import StagingHandler, StorageIds
from irs.ports.inbound.interrogator import InterrogatorPort
from irs.ports.outbound.dao import FingerprintDaoPort, StagingObjectDaoPort
from irs.ports.outbound.event_pub import EventPublisherPort

log = getLogger(__name__)


class Interrogator(InterrogatorPort):
    """A service that validates the content of encrypted files"""

    def __init__(
        self,
        *,
        event_publisher: EventPublisherPort,
        fingerprint_dao: FingerprintDaoPort,
        staging_object_dao: StagingObjectDaoPort,
        object_storages: S3ObjectStorages,
    ):
        """Initialize class instance with configs and outbound adapter objects."""
        self._event_publisher = event_publisher
        self._fingerprint_dao = fingerprint_dao
        self._staging_object_dao = staging_object_dao
        self._object_storages = object_storages

    async def interrogate(self, *, payload: event_schemas.FileUploadReceived):
        """
        Forwards first file part to encryption key store, retrieves file encryption
        secret(s) (K_data), decrypts file and computes checksums. The object and bucket
        ID parameters refer to the object_id and bucket_id associated with the upload,
        i.e. not the staging bucket.
        """
        try:
            staging_bucket_id, object_storage = self._object_storages.for_alias(
                payload.s3_endpoint_alias
            )
        except KeyError as error:
            storage_not_configured = ValueError(
                f"Storage alias not configured: {payload.s3_endpoint_alias}"
            )
            log.critical(
                storage_not_configured, extra={"alias": payload.s3_endpoint_alias}
            )
            raise storage_not_configured from error

        fingerprint = UploadReceivedFingerprint.generate(payload=payload)

        with contextlib.suppress(ResourceNotFoundError):
            await self._fingerprint_dao.get_by_id(id_=fingerprint.checksum)
            file_id = payload.file_id
            log.warning(
                "Payload for file ID '%s' has already been processed.",
                file_id,
                extra={"file_id": file_id},
            )
            return

        object_size = await object_storage.get_object_size(
            object_id=payload.object_id, bucket_id=payload.bucket_id
        )
        part_size = calc_part_size(file_size=object_size)

        # generate ID for the staging bucket file
        object_id = str(uuid.uuid4())
        staging_ids = StorageIds(bucket_id=staging_bucket_id, object_id=object_id)
        inbox_ids = StorageIds(bucket_id=payload.bucket_id, object_id=payload.object_id)

        object_storage_handler = StagingHandler(
            object_storage=object_storage,
            inbox_ids=inbox_ids,
            staging_ids=staging_ids,
            part_size=part_size,
        )

        # do the actual work
        try:
            download_url = await object_storage.get_object_download_url(
                object_id=payload.object_id, bucket_id=payload.bucket_id
            )
            part = await object_storage_handler.retrieve_part(
                url=download_url, start=0, stop=part_size - 1
            )
            submitter_secret, new_secret, secret_id, offset = call_eks_api(
                file_part=part,
                public_key=payload.submitter_public_key,
                api_url=CONFIG.ekss_base_url,
            )

            cipher_segment_processor = CipherSegmentProcessor(
                secret=submitter_secret,
                new_secret=new_secret,
                part_size=part_size,
                offset=offset,
                object_storage_handler=object_storage_handler,
            )
            (
                part_checksums_md5,
                part_checksums_sha256,
                content_checksum_sha256,
            ) = await cipher_segment_processor.process()
        except (CryptoError, KnownError, ValueError) as exc:
            # remove data for ongoing upload in case of failure
            await object_storage_handler.abort_staging()
            await self._event_publisher.publish_validation_failure(
                file_id=payload.file_id,
                object_id=object_id,
                bucket_id=staging_bucket_id,
                upload_date=payload.upload_date,
                cause=str(exc),
                s3_endpoint_alias=payload.s3_endpoint_alias,
            )
            return

        # handle publishing both outcomes
        if payload.expected_decrypted_sha256 == content_checksum_sha256:
            await self._event_publisher.publish_validation_success(
                file_id=payload.file_id,
                object_id=object_id,
                bucket_id=staging_bucket_id,
                secret_id=secret_id,
                offset=offset,
                upload_date=payload.upload_date,
                part_size=part_size,
                part_checksums_sha256=part_checksums_sha256,
                part_checksums_md5=part_checksums_md5,
                content_checksum_sha256=content_checksum_sha256,
                decrypted_size=payload.decrypted_size,
                s3_endpoint_alias=payload.s3_endpoint_alias,
            )
            # save mapping for stale object check/removal
            staging_object = StagingObject(file_id=payload.file_id, object_id=object_id)
            await self._staging_object_dao.insert(dto=staging_object)
            # Everything has been processed successfully, add fingerprint to db for lookup
            await self._fingerprint_dao.insert(fingerprint)
        else:
            # remove invalid object from its staging bucket
            await object_storage_handler.delete_from_staging()
            await self._event_publisher.publish_validation_failure(
                file_id=payload.file_id,
                object_id=object_id,
                bucket_id=staging_bucket_id,
                upload_date=payload.upload_date,
                s3_endpoint_alias=payload.s3_endpoint_alias,
            )

    async def remove_staging_object(
        self, *, payload: event_schemas.FileInternallyRegistered
    ):
        """Remove transient object from staging once copy to permanent storage has been confirmed."""
        file_id = payload.file_id
        storage_alias = payload.s3_endpoint_alias

        try:
            staging_bucket_id, object_storage = self._object_storages.for_alias(
                storage_alias
            )
        except KeyError as error:
            storage_not_configured = ValueError(
                f"Storage alias not configured: {payload.s3_endpoint_alias}"
            )
            log.critical(
                storage_not_configured, extra={"alias": payload.s3_endpoint_alias}
            )
            raise storage_not_configured from error

        try:
            staging_object = await self._staging_object_dao.get_by_id(id_=file_id)
        except ResourceNotFoundError:
            log.warning("No object in staging bucket for file '%s'.", file_id)
            return

        object_exists = await object_storage.does_object_exist(
            bucket_id=staging_bucket_id, object_id=staging_object.object_id
        )

        if not object_exists:
            out_of_sync_error = ValueError(
                f"Object '{staging_object.object_id}' unexpectedly not in staging bucket"
                + f" '{staging_bucket_id}' of storage '{storage_alias}'."
            )
            log.critical(out_of_sync_error)
            raise out_of_sync_error

        # these have been checked before, i.e. should not raise
        await object_storage.delete_object(
            bucket_id=staging_bucket_id, object_id=staging_object.object_id
        )
        await self._staging_object_dao.delete(id_=file_id)
