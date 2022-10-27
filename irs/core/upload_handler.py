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
"""Core interrogation room logic called by event subscriber"""

import hashlib
import math
from typing import List, Tuple

from crypt4gh.lib import CIPHER_SEGMENT_SIZE, CryptoError, decrypt_block

from irs.adapters.http.api_calls import call_eks_api
from irs.adapters.http.exceptions import KnownError
from irs.adapters.inbound.s3_download import (
    get_download_url,
    retrieve_part,
    retrieve_parts,
)
from irs.adapters.outbound.kafka_producer import (
    produce_failure_event,
    produce_success_event,
)
from irs.config import CONFIG

PART_SIZE = 16 * 1024**2


async def process_new_upload(  # pylint: disable=too-many-locals
    *, object_id: str, object_size: int, public_key: str, checksum: str
):
    """
    Forwards first file part to encryption key store, retrieves file encryption
    secret(s) (K_data), decrypts file and computes checksums
    """
    try:
        download_url = await get_download_url(object_id=object_id)
        part = await retrieve_part(url=download_url, start=0, stop=PART_SIZE - 1)
        secret, secret_id, offset = call_eks_api(
            file_part=part, public_key=public_key, api_url=CONFIG.eks_url
        )
        (
            part_checksums_md5,
            part_checksums_sha256,
            content_checksum_sha256,
        ) = await compute_checksums(
            download_url=download_url,
            secret=secret,
            object_size=object_size,
            offset=offset,
        )
    except (CryptoError, KnownError, ValueError) as exc:
        await produce_failure_event(file_id=object_id, cause=str(exc))
        return

    if checksum == content_checksum_sha256:
        await produce_success_event(
            file_id=object_id,
            secret_id=secret_id,
            offset=offset,
            part_size=PART_SIZE,
            part_checksums_sha256=part_checksums_sha256,
            part_checksums_md5=part_checksums_md5,
            content_checksum_sha256=content_checksum_sha256,
        )
    else:
        await produce_failure_event(file_id=object_id)


async def compute_checksums(
    *, download_url: str, secret: bytes, object_size: int, offset: int
) -> Tuple[List[str], List[str], str]:
    """Compute total unencrypted file checksum and encrypted part checksums"""
    total_sha256_checksum = hashlib.sha256()
    encrypted_md5_part_checksums = []
    encrypted_sha256_part_checksums = []

    # buffers to account for non part/cipher segment size aligned blocks
    partial_ciphersegment = b""

    async for part in retrieve_parts(
        url=download_url, object_size=object_size, part_size=PART_SIZE, offset=offset
    ):
        md5sum, sha256sum = get_part_checksums(file_part=part)
        encrypted_md5_part_checksums.append(md5sum)
        encrypted_sha256_part_checksums.append(sha256sum)

        if partial_ciphersegment:
            part = partial_ciphersegment + part
        ciphersegments, incomplete_ciphersegment = get_segments(file_part=part)
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


def get_segments(*, file_part: bytes) -> Tuple[List[bytes], bytes]:
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


def get_part_checksums(*, file_part: bytes):
    """Compute md5 and sha256 for encrypted part"""
    return (
        hashlib.md5(file_part, usedforsecurity=False).hexdigest(),
        hashlib.sha256(file_part).hexdigest(),
    )
