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
from ghga_service_commons.utils.multinode_storage import S3ObjectStorages
from hexkit.protocols.dao import ResourceNotFoundError
from hexkit.utils import calc_part_size

from irs.adapters.outbound.http.api_calls import call_eks_api
from irs.adapters.outbound.http.exceptions import EnvelopeError, TransientError
from irs.config import CONFIG
from irs.core.models import (
    InterrogationSubject,
    ProcessingResult,
    StagingObject,
    UploadReceivedFingerprint,
)
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

    async def _fingerprint_already_seen(
        self, *, fingerprint: UploadReceivedFingerprint
    ) -> bool:
        """Check if the incoming event has already been processed."""
        with contextlib.suppress(ResourceNotFoundError):
            await self._fingerprint_dao.get_by_id(id_=fingerprint.checksum)
            return True
        return False

    async def _init_staging_handler(
        self, *, inbox_bucket_id: str, inbox_object_id, storage_alias: str
    ) -> StagingHandler:
        """Initialize staging handler object and necessary variables for re-encryption."""
        try:
            staging_bucket_id, object_storage = self._object_storages.for_alias(
                storage_alias
            )
        except KeyError as error:
            storage_not_configured = ValueError(
                f"Storage alias not configured: {storage_alias}"
            )
            log.critical(storage_not_configured, extra={"alias": storage_alias})
            raise storage_not_configured from error

        # generate ID for the staging bucket file
        object_id = str(uuid.uuid4())
        staging_ids = StorageIds(bucket_id=staging_bucket_id, object_id=object_id)
        inbox_ids = StorageIds(bucket_id=inbox_bucket_id, object_id=inbox_object_id)

        object_size = await object_storage.get_object_size(
            object_id=inbox_object_id, bucket_id=inbox_bucket_id
        )
        part_size = calc_part_size(file_size=object_size)

        return StagingHandler(
            storage=object_storage,
            inbox_ids=inbox_ids,
            staging_ids=staging_ids,
            part_size=part_size,
        )

    async def _reencrypt_and_stage(
        self, *, staging_handler: StagingHandler, submitter_public_key: str
    ) -> ProcessingResult:
        """Re-encrypt and upload part by part to staging bucket."""
        # extract current secret and generate new one
        download_url = await staging_handler.storage.get_object_download_url(
            object_id=staging_handler.inbox.object_id,
            bucket_id=staging_handler.inbox.bucket_id,
        )
        part = await staging_handler.retrieve_part(
            url=download_url, start=0, stop=staging_handler.part_size - 1
        )
        submitter_secret, new_secret, secret_id, offset = call_eks_api(
            file_part=part,
            public_key=submitter_public_key,
            api_url=CONFIG.ekss_base_url,
        )

        # re-encrypt and multipart upload from inbox bucket to staging bucket
        cipher_segment_processor = CipherSegmentProcessor(
            secret=submitter_secret,
            new_secret=new_secret,
            part_size=staging_handler.part_size,
            offset=offset,
            object_storage_handler=staging_handler,
        )

        checksums = await cipher_segment_processor.process()

        return ProcessingResult(offset=offset, secret_id=secret_id, checksums=checksums)

    async def interrogate(self, *, subject: InterrogationSubject) -> None:
        """
        Forwards first file part to encryption key store, retrieves file encryption
        secret(s) (K_data), decrypts file and computes checksums. The object and bucket
        ID parameters refer to the object_id and bucket_id associated with the upload,
        i.e. not the staging bucket.
        """
        file_id = subject.file_id
        fingerprint = UploadReceivedFingerprint.generate(subject=subject)
        if await self._fingerprint_already_seen(fingerprint=fingerprint):
            log.warning(
                "Payload for file ID '%s' has already been processed.",
                file_id,
                extra={"file_id": file_id},
            )
            return
        log.debug("Checked fingerprint for file '%s'.", file_id)

        staging_handler = await self._init_staging_handler(
            inbox_bucket_id=subject.inbox_bucket_id,
            inbox_object_id=subject.inbox_object_id,
            storage_alias=subject.storage_alias,
        )
        log.debug("Initialized staging handler for file '%s'.", file_id)

        try:
            processing_result = await self._reencrypt_and_stage(
                staging_handler=staging_handler,
                submitter_public_key=subject.submitter_public_key,
            )
        except (CryptoError, EnvelopeError, ValueError) as error:
            # These should be systemic issues with the file submitted, i.e. there's no
            # way to recover without resubmitting in the upstream service
            log.error(error)
            await staging_handler.abort_staging()
            await self._event_publisher.publish_validation_failure(
                staging_handler=staging_handler, subject=subject, cause=str(error)
            )
            # Add fingerprint to db for lookup
            await self._fingerprint_dao.insert(fingerprint)
            log.debug("Stored fingerprint for file '%s'.", file_id)
            return

        except TransientError as error:
            # All these exceptions should come from either a misconfigured connection URL,
            # network issues or actual issues with the EKSS. These issues should crash this
            # service to avoid unnecessary work that will ultimately fail
            log.critical(error)
            raise

        log.debug("Finished re-encryption and staging for file '%s'.", file_id)

        # handle publishing both outcomes
        if (
            subject.expected_decrypted_sha256
            != processing_result.checksums.content_checksum_sha256
        ):
            # remove invalid object from its staging bucket
            await staging_handler.delete_staged()
            await self._event_publisher.publish_validation_failure(
                staging_handler=staging_handler, subject=subject
            )
        else:
            # save mapping for stale object check/removal
            staging_object = StagingObject(
                file_id=subject.file_id,
                object_id=staging_handler.staging.object_id,
                storage_alias=subject.storage_alias,
            )
            await self._staging_object_dao.insert(dto=staging_object)
            await self._event_publisher.publish_validation_success(
                processing_result=processing_result,
                staging_handler=staging_handler,
                subject=subject,
            )
        log.debug("Finished cleanup and event sending for file '%s'.", file_id)

        # Everything has been processed, add fingerprint to db for lookup
        await self._fingerprint_dao.insert(fingerprint)
        log.debug("Stored fingerprint for file '%s'.", file_id)

    async def remove_staging_object(self, *, file_id: str, storage_alias: str) -> None:
        """Remove transient object from staging once copy to permanent storage has been confirmed."""
        try:
            staging_bucket_id, object_storage = self._object_storages.for_alias(
                storage_alias
            )
        except KeyError as error:
            storage_not_configured = ValueError(
                f"Storage alias not configured: {storage_alias}"
            )
            log.critical(storage_not_configured, extra={"alias": storage_alias})
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
        log.debug("Successfully removed staging object for file '%s'.", file_id)
