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

import hashlib
import math
import os
from datetime import datetime
from typing import List, Tuple

from crypt4gh.lib import CIPHER_SEGMENT_SIZE, CryptoError, decrypt_block
from hexkit.utils import calc_part_size
from nacl.bindings import crypto_aead_chacha20poly1305_ietf_encrypt

from irs.adapters.http.api_calls import call_eks_api
from irs.adapters.http.exceptions import KnownError
from irs.adapters.inbound.s3 import (
    complete_staging,
    get_download_url,
    get_object_size,
    init_staging,
    retrieve_part,
    retrieve_parts,
    stage_part,
)
from irs.config import CONFIG
from irs.core.exceptions import LastSegmentCorruptedError, SegmentCorruptedError
from irs.ports.inbound.interrogator import InterrogatorPort
from irs.ports.outbound.event_pub import EventPublisherPort


class Interrogator(InterrogatorPort):
    """A service that validates the content of encrypted files"""

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
        decrypted_size: int,
        sha256_checksum: str,
    ):
        """
        Forwards first file part to encryption key store, retrieves file encryption
        secret(s) (K_data), decrypts file and computes checksums
        """
        object_size = await get_object_size(object_id=object_id)
        part_size = calc_part_size(file_size=object_size)
        try:
            download_url = await get_download_url(object_id=object_id)
            part = await retrieve_part(url=download_url, start=0, stop=part_size - 1)
            submitter_secret, new_secret, secret_id, offset = call_eks_api(
                file_part=part, public_key=public_key, api_url=CONFIG.eks_url
            )
            (
                part_checksums_md5,
                part_checksums_sha256,
                content_checksum_sha256,
            ) = await self._reencrypt_and_compute_checksums(
                download_url=download_url,
                secret=submitter_secret,
                new_secret=new_secret,
                object_size=object_size,
                object_id=object_id,
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
                decrypted_size=decrypted_size,
            )
        else:
            await self._event_publisher.publish_validation_failure(
                file_id=object_id, upload_date=upload_date
            )

    async def _reencrypt_and_compute_checksums(  # noqa: C901; pylint: disable=too-many-locals, too-many-statements
        self,
        *,
        download_url: str,
        secret: bytes,
        new_secret: bytes,
        object_size: int,
        object_id: str,
        part_size: int,
        offset: int,
    ) -> Tuple[List[str], List[str], str]:
        """Compute total unencrypted file checksum and encrypted part checksums"""
        total_sha256_checksum = hashlib.sha256()
        encrypted_md5_part_checksums = []
        encrypted_sha256_part_checksums = []

        # buffers to account for non part/cipher segment size aligned blocks
        partial_ciphersegment = b""
        reencrypted_buffer = b""

        # counter for uploading reencrypted file to staging area
        reencrypted_part_number = 0

        # start multipart upload for staging multipart upload
        upload_id = await init_staging(object_id=object_id)

        part_number = 0
        async for part in retrieve_parts(
            url=download_url,
            object_size=object_size,
            part_size=part_size,
            offset=offset,
        ):
            part_number += 1

            if partial_ciphersegment:
                part = partial_ciphersegment + part
            ciphersegments, incomplete_ciphersegment = self._get_segments(
                file_part=part
            )
            partial_ciphersegment = incomplete_ciphersegment

            for ciphersegment in ciphersegments:
                try:
                    decrypted = decrypt_block(
                        ciphersegment=ciphersegment, session_keys=[secret]
                    )
                # could also depend on PyNaCl directly for the exact exception
                except Exception as exc:
                    raise SegmentCorruptedError(part_number=part_number) from exc
                total_sha256_checksum.update(decrypted)

                # reencrypt using the new secret
                encrypted = self._encrypt_segment(data=decrypted, key=new_secret)
                reencrypted_buffer += encrypted

            if len(reencrypted_buffer) >= part_size:
                part = reencrypted_buffer[:part_size]

                # calculate re-encrypted checksums
                md5sum, sha256sum = self._get_part_checksums(file_part=part)
                encrypted_md5_part_checksums.append(md5sum)
                encrypted_sha256_part_checksums.append(sha256sum)

                reencrypted_part_number += 1
                await stage_part(
                    upload_id=upload_id,
                    object_id=object_id,
                    data=part,
                    part_number=reencrypted_part_number,
                )

                rest_len = len(reencrypted_buffer) - part_size
                reencrypted_buffer = reencrypted_buffer[-rest_len:]

        # process remaining data
        if incomplete_ciphersegment:
            try:
                decrypted = decrypt_block(
                    ciphersegment=incomplete_ciphersegment, session_keys=[secret]
                )
            # could also depend on PyNaCl directly for the exact exception
            except Exception as exc:
                raise LastSegmentCorruptedError() from exc
            total_sha256_checksum.update(decrypted)

            # encrypt the last segment
            encrypted = self._encrypt_segment(data=decrypted, key=new_secret)
            reencrypted_buffer += encrypted

        if len(reencrypted_buffer) > part_size:
            part = reencrypted_buffer[:part_size]

            # calculate re-encrypted checksums
            md5sum, sha256sum = self._get_part_checksums(file_part=part)
            encrypted_md5_part_checksums.append(md5sum)
            encrypted_sha256_part_checksums.append(sha256sum)

            reencrypted_part_number += 1
            await stage_part(
                upload_id=upload_id,
                object_id=object_id,
                data=part,
                part_number=reencrypted_part_number,
            )

            rest_len = len(reencrypted_buffer) - part_size
            reencrypted_buffer = reencrypted_buffer[-rest_len:]

        # re-encrypt the last (incomplete) part
        part = reencrypted_buffer

        # calculate re-encrypted checksums
        md5sum, sha256sum = self._get_part_checksums(file_part=part)
        encrypted_md5_part_checksums.append(md5sum)
        encrypted_sha256_part_checksums.append(sha256sum)

        reencrypted_part_number += 1
        await stage_part(
            upload_id=upload_id,
            object_id=object_id,
            data=part,
            part_number=reencrypted_part_number,
        )

        # finish staging the re-encrypted file
        await complete_staging(
            upload_id=upload_id,
            object_id=object_id,
            part_size=part_size,
            parts=reencrypted_part_number,
        )

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
