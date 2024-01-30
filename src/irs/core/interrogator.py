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
import hashlib
import math
import os
import uuid
from logging import getLogger

from crypt4gh.lib import CIPHER_SEGMENT_SIZE, CryptoError, decrypt_block
from ghga_event_schemas import pydantic_ as event_schemas
from ghga_service_commons.utils.multinode_storage import S3ObjectStorages
from hexkit.protocols.dao import ResourceNotFoundError
from hexkit.utils import calc_part_size
from nacl.bindings import crypto_aead_chacha20poly1305_ietf_encrypt

from irs.adapters.outbound.http.api_calls import call_eks_api
from irs.adapters.outbound.http.exceptions import KnownError
from irs.config import CONFIG
from irs.core.exceptions import LastSegmentCorruptedError, SegmentCorruptedError
from irs.core.models import UploadReceivedFingerprint
from irs.core.staging_handler import StagingHandler, StorageIds
from irs.ports.inbound.interrogator import InterrogatorPort
from irs.ports.outbound.dao import FingerprintDaoPort
from irs.ports.outbound.event_pub import EventPublisherPort

log = getLogger(__name__)


class CipherSegmentProcessor:
    """Process inbox file for checksum generation and reencryption"""

    def __init__(  # noqa: PLR0913
        self,
        *,
        secret: bytes,
        new_secret: bytes,
        part_size: int,
        offset: int,
        object_storage_handler: StagingHandler,
    ) -> None:
        self.object_storage_handler = object_storage_handler
        self.secret = secret
        self.new_secret = new_secret
        self.part_size = part_size
        self.offset = offset

        # hashsum buffers
        self.total_sha256_checksum = hashlib.sha256()
        self.encrypted_md5_part_checksums: list[str] = []
        self.encrypted_sha256_part_checksums: list[str] = []

        # counter for uploading reencrypted file to staging area
        self.reencrypted_part_number = 0

        # buffers to accumulate ciphersegments up to part size
        self.partial_ciphersegment = b""
        self.reencryption_buffer = b""

    async def process(self):
        """Delegate processing and return checksums"""
        # start multipart upload for staging multipart upload
        await self.object_storage_handler.init_staging()

        await self._process_parts()

        # finish staging the re-encrypted file
        await self.object_storage_handler.complete_staging(
            parts=self.reencrypted_part_number
        )

        return (
            self.encrypted_md5_part_checksums,
            self.encrypted_sha256_part_checksums,
            self.total_sha256_checksum.hexdigest(),
        )

    def _encrypt_segment(self, *, data: bytes, key: bytes) -> bytes:
        """
        Utility function to generate a nonce, encrypt data with Chacha20,
        and authenticate it with Poly1305.
        """
        nonce = os.urandom(12)
        encrypted_data = crypto_aead_chacha20poly1305_ietf_encrypt(
            data, None, nonce, key
        )  # no aad

        return nonce + encrypted_data

    def _get_part_checksums(self, *, file_part: bytes):
        """Compute md5 and sha256 for encrypted part"""
        return (
            hashlib.md5(file_part, usedforsecurity=False).hexdigest(),
            hashlib.sha256(file_part).hexdigest(),
        )

    def _get_segments(self, *, file_part: bytes) -> tuple[list[bytes], bytes]:
        """Chunk file part into decryptable segments"""
        num_segments = len(file_part) / CIPHER_SEGMENT_SIZE
        full_segments = int(num_segments)
        segments = [
            file_part[i * CIPHER_SEGMENT_SIZE : (i + 1) * CIPHER_SEGMENT_SIZE]
            for i in range(full_segments)
        ]

        # check if we have a remainder of bytes that we need to handle,
        # i.e. non-matching boundaries between part and cipher segment size
        incomplete_segment = b""
        partial_segment_idx = math.ceil(num_segments)
        if partial_segment_idx != full_segments:
            incomplete_segment = file_part[full_segments * CIPHER_SEGMENT_SIZE :]
        return segments, incomplete_segment

    async def _process_parts(self):
        """High-level part processing, chunking into ciphersegments"""
        incomplete_ciphersegment = b""

        async for part in self.object_storage_handler.retrieve_parts(
            offset=self.offset,
        ):
            if self.partial_ciphersegment:
                part = self.partial_ciphersegment + part
            ciphersegments, incomplete_ciphersegment = self._get_segments(
                file_part=part
            )
            await self._process_segments(ciphersegments=ciphersegments)
            self.partial_ciphersegment = incomplete_ciphersegment

        await self._process_remaining(incomplete_ciphersegment=incomplete_ciphersegment)

    async def _process_segments(self, *, ciphersegments: list[bytes]):
        """Process complete ciphersegments"""
        for ciphersegment in ciphersegments:
            try:
                decrypted = decrypt_block(
                    ciphersegment=ciphersegment, session_keys=[self.secret]
                )
            except Exception as exc:
                raise SegmentCorruptedError(
                    part_number=self.reencrypted_part_number
                ) from exc
            self.total_sha256_checksum.update(decrypted)

            # reencrypt using the new secret
            self.reencryption_buffer += self._encrypt_segment(
                data=decrypted, key=self.new_secret
            )

        if len(self.reencryption_buffer) >= self.part_size:
            await self._reencrypt_and_stage()

    async def _process_remaining(self, *, incomplete_ciphersegment: bytes):
        """Process last, possibly incomplete ciphersegment"""
        if incomplete_ciphersegment:
            try:
                decrypted = decrypt_block(
                    ciphersegment=incomplete_ciphersegment, session_keys=[self.secret]
                )
            # could also depend on PyNaCl directly for the exact exception
            except Exception as exc:
                raise LastSegmentCorruptedError() from exc
            self.total_sha256_checksum.update(decrypted)

            # encrypt the last segment
            self.reencryption_buffer += self._encrypt_segment(
                data=decrypted, key=self.new_secret
            )

        if len(self.reencryption_buffer) > self.part_size:
            await self._reencrypt_and_stage()

        await self._reencrypt_and_stage(consume_remainder=True)

    async def _reencrypt_and_stage(self, *, consume_remainder: bool = False):
        """Reencrypt, calculate checksums and stage file part"""
        if consume_remainder:
            part = self.reencryption_buffer
        else:
            part = self.reencryption_buffer[: self.part_size]

        # calculate re-encrypted checksums
        md5sum, sha256sum = self._get_part_checksums(file_part=part)
        self.encrypted_md5_part_checksums.append(md5sum)
        self.encrypted_sha256_part_checksums.append(sha256sum)

        self.reencrypted_part_number += 1
        await self.object_storage_handler.stage_part(
            data=part,
            part_number=self.reencrypted_part_number,
        )

        if not consume_remainder:
            rest_len = len(self.reencryption_buffer) - self.part_size
            self.reencryption_buffer = self.reencryption_buffer[-rest_len:]


class Interrogator(InterrogatorPort):
    """A service that validates the content of encrypted files"""

    def __init__(
        self,
        *,
        event_publisher: EventPublisherPort,
        fingerprint_dao: FingerprintDaoPort,
        object_storages: S3ObjectStorages,
    ):
        """Initialize class instance with configs and outbound adapter objects."""
        self._event_publisher = event_publisher
        self._fingerprint_dao = fingerprint_dao
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
            raise ValueError(
                f"Storage alias not configured: {payload.s3_endpoint_alias}"
            ) from error

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
            await self._event_publisher.publish_validation_failure(
                file_id=payload.file_id,
                object_id=object_id,
                bucket_id=staging_bucket_id,
                upload_date=payload.upload_date,
                cause=str(exc),
                s3_endpoint_alias=payload.s3_endpoint_alias,
            )
            return

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
        else:
            await self._event_publisher.publish_validation_failure(
                file_id=payload.file_id,
                object_id=object_id,
                bucket_id=staging_bucket_id,
                upload_date=payload.upload_date,
                s3_endpoint_alias=payload.s3_endpoint_alias,
            )

        # Everything has been processed successfully, add fingerprint to db for lookup
        await self._fingerprint_dao.insert(fingerprint)
