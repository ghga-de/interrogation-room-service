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
"""A service for validating uploaded files"""

import hashlib
import math
from datetime import datetime
from typing import List, Tuple

from crypt4gh.lib import CIPHER_SEGMENT_SIZE, CryptoError, decrypt_block
from hexkit.utils import calc_part_size

from irs.adapters.http.api_calls import call_eks_api
from irs.adapters.http.exceptions import KnownError
from irs.adapters.inbound.s3_download import (
    get_download_url,
    retrieve_part,
    retrieve_parts,
)
from irs.config import CONFIG
from irs.ports.inbound.upload_handler import UploadHandlerPort
from irs.ports.outbound.event_pub import EventPublisherPort


class Interrogator(InterrogatorPort):
    """A service that validates the content of encrypted files and performs a re-encryption."""

    def __init__(
        self,
        *,
        event_publisher: EventPublisherPort,
    ):
        """Initialize class instance with configs and outbound adapter objects."""
        self._event_publisher = event_publisher

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
        part_size = calc_part_size(file_size=object_size)
        try:
            download_url = await get_download_url(object_id=object_id)
            part = await retrieve_part(url=download_url, start=0, stop=part_size - 1)
            secret, secret_id, offset = call_eks_api(
                file_part=part, public_key=public_key, api_url=CONFIG.eks_url
            )
            (
                part_checksums_md5,
                part_checksums_sha256,
                content_checksum_sha256,
            ) = await self._compute_checksums(
                download_url=download_url,
                secret=secret,
                object_size=object_size,
                part_size=part_size,
                offset=offset,
            )
        except (CryptoError, KnownError, ValueError) as exc:
            await self._event_publisher.publish_validation_failure(
                file_id=object_id, upload_date=upload_date, cause=str(exc)
            )
            return

        if sha256_checksum == content_checksum_sha256:
            await self._event_publisher.publish_validation_success(
                file_id=object_id,
                secret_id=secret_id,
                offset=offset,
                upload_date=upload_date,
                part_size=part_size,
                part_checksums_sha256=part_checksums_sha256,
                part_checksums_md5=part_checksums_md5,
                content_checksum_sha256=content_checksum_sha256,
                decrypted_size=object_size,
            )
        else:
            await self._event_publisher.publish_validation_failure(
                file_id=object_id, upload_date=upload_date
            )

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
        total_sha256_checksum = hashlib.sha256()
        encrypted_md5_part_checksums = []
        encrypted_sha256_part_checksums = []

        # buffers to account for non part/cipher segment size aligned blocks
        partial_ciphersegment = b""

        async for part in retrieve_parts(
            url=download_url,
            object_size=object_size,
            part_size=part_size,
            offset=offset,
        ):
            md5sum, sha256sum = self._get_part_checksums(file_part=part)
            encrypted_md5_part_checksums.append(md5sum)
            encrypted_sha256_part_checksums.append(sha256sum)

            if partial_ciphersegment:
                part = partial_ciphersegment + part
            ciphersegments, incomplete_ciphersegment = self._get_segments(
                file_part=part
            )
            partial_ciphersegment = incomplete_ciphersegment

            for ciphersegment in ciphersegments:
                decrypted = decrypt_block(
                    ciphersegment=ciphersegment, session_keys=[secret]
                )
                total_sha256_checksum.update(decrypted)

        # process remaining data
        if incomplete_ciphersegment:
            decrypted = decrypt_block(
                ciphersegment=incomplete_ciphersegment, session_keys=[secret]
            )
            total_sha256_checksum.update(decrypted)

        return (
            encrypted_md5_part_checksums,
            encrypted_sha256_part_checksums,
            total_sha256_checksum.hexdigest(),
        )

    def _get_segments(self, *, file_part: bytes) -> Tuple[List[bytes], bytes]:
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

    def _get_part_checksums(self, *, file_part: bytes):
        """Compute md5 and sha256 for encrypted part"""
        return (
            hashlib.md5(file_part, usedforsecurity=False).hexdigest(),
            hashlib.sha256(file_part).hexdigest(),
        )
