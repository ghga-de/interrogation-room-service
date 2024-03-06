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
"""Contains functionality dealing with low-level on-the-fly re-encryption."""

import hashlib
import logging
import math
import os

from crypt4gh.lib import CIPHER_SEGMENT_SIZE, decrypt_block
from nacl.bindings import crypto_aead_chacha20poly1305_ietf_encrypt

from irs.core.exceptions import LastSegmentCorruptedError, SegmentCorruptedError
from irs.core.models import Checksums
from irs.core.staging_handler import StagingHandler

log = logging.getLogger(__name__)


class CipherSegmentProcessor:
    """Process inbox file for checksum generation and re-encryption"""

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

    async def process(self) -> Checksums:
        """Delegate processing and return checksums"""
        # start multipart upload for staging multipart upload
        await self.object_storage_handler.init_staging()

        await self._process_parts()

        # finish staging the re-encrypted file
        await self.object_storage_handler.complete_staging(
            parts=self.reencrypted_part_number
        )

        return Checksums(
            part_checksums_md5=self.encrypted_md5_part_checksums,
            part_checksums_sha256=self.encrypted_sha256_part_checksums,
            content_checksum_sha256=self.total_sha256_checksum.hexdigest(),
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
        current_part = 0

        async for part in self.object_storage_handler.retrieve_parts(
            offset=self.offset,
        ):
            current_part += 1
            log.debug("Processing part no. '%i'.", current_part)

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
                segment_corrupted = SegmentCorruptedError(
                    part_number=self.reencrypted_part_number
                )
                log.error(
                    segment_corrupted,
                    extra={"part_number": self.reencrypted_part_number},
                )
                raise segment_corrupted from exc
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
                last_segment_corrupted = LastSegmentCorruptedError()
                log.error(last_segment_corrupted)
                raise last_segment_corrupted from exc
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
