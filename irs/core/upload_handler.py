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

import base64
import codecs
import hashlib
import math
from typing import List, Sequence, Tuple

import requests
from crypt4gh.lib import CIPHER_SEGMENT_SIZE, decrypt_block
from hexkit.providers.akafka import KafkaEventPublisher
from hexkit.providers.s3.provider import S3Config, S3ObjectStorage

from irs.adapters.outbound.kafka_producer import (
    UploadValidationFailureEvent,
    UploadValidationSuccessEvent,
)
from irs.config import CONFIG
from irs.core import exceptions
from irs.core.http_translation import ResponseExceptionTranslator

PART_SIZE = 16 * 1024**2


async def process_new_upload(  # pylint: disable=too-many-locals
    *, object_id: str, object_size: int, public_key: str, checksum: str
):
    """
    Forwards first file part to encryption key store, retrieves file encryption
    secret(s) (K_data), decrypts file and computes checksums
    """
    download_url = await get_download_url(object_id=object_id)
    part = await retrieve_part(url=download_url, start=0, stop=PART_SIZE - 1)

    secret, secret_id, offset = await send_to_eks(
        file_part=part, public_key=public_key, eks_url=CONFIG.eks_url
    )
    part_checksums, total_checksum = await compute_checksums(
        download_url=download_url,
        secret=secret,
        object_size=object_size,
        offset=offset,
    )

    validation_success = total_checksum == checksum
    type_ = "irs_publisher"
    key = "irs"
    async with KafkaEventPublisher.construct(config=CONFIG) as publisher:
        if validation_success:
            topic = "upload_validation_success"
            data = UploadValidationSuccessEvent(
                content_id=total_checksum,
                secret_id=secret_id,
                offset=offset,
                part_size=PART_SIZE,
                part_checksums_md5=part_checksums[0],
                part_checksums_sha256=part_checksums[1],
            ).dict()
        else:
            topic = "upload_validation_failure"
            # implement exception handling, get cause from exception
            data = UploadValidationFailureEvent(
                file_id=object_id, cause="Checksum mismatch"
            ).dict()
        publisher.publish(paylod=data, type_=type_, key=key, topic=topic)


async def get_download_url(*, object_id: str):
    """Get object download URL frome"""
    config = S3Config(
        s3_endpoint_url=CONFIG.s3_endpoint_url,
        s3_access_key_id=CONFIG.s3_access_key_id,
        s3_secret_access_key=CONFIG.s3_secret_access_key,
    )
    storage = S3ObjectStorage(config=config)

    return storage.get_object_download_url(
        bucket_id=CONFIG.bucket_id, object_id=object_id
    )


async def send_to_eks(
    *, file_part: bytes, public_key: str, eks_url: str
) -> Tuple[bytes, str, int]:
    """Get encryption secret and file content offset from envelope"""
    data = base64.b64encode(file_part).hex()
    request_body = {"public_key": public_key, "file_part": data}
    try:
        response = requests.post(url=eks_url, json=request_body, timeout=60)
    except requests.exceptions.RequestException as request_error:
        raise exceptions.RequestFailedError(url=eks_url) from request_error

    status_code = response.status_code
    # implement httpyexpect error conversion
    if status_code != 200:
        spec = {
            400: {
                "malformedOrMissingEnvelopeError": exceptions.MalformedOrMissingEnvelope()
            },
            403: {"envelopeDecryptionError": exceptions.EnvelopeDecryptionError()},
        }
        ResponseExceptionTranslator(spec=spec).handle(response=response)
        raise exceptions.BadResponseCodeError(url=eks_url, response_code=status_code)

    body = response.json()
    secret = base64.b64decode(codecs.decode(body["secret"], "hex"))
    secret_id = body["secret_id"]
    offset = body["offset"]

    return secret, secret_id, offset


async def compute_checksums(  # pylint: disable=too-many-locals
    *, download_url: str, secret: bytes, object_size: int, offset: int
) -> Tuple[Tuple[List[str], List[str]], str]:
    """Compute total unencrypted file checksum and encrypted part checksums"""
    total_sha256_checksum = hashlib.sha256()
    encrypted_md5_part_checksums = []
    encrypted_sha256_part_checksums = []

    # buffers to account for non part/cipher segment size aligned blocks
    incomplete_part_buffer = bytearray()
    partial_chunk = b""
    async for part in retrieve_parts(
        url=download_url, object_size=object_size, offset=offset
    ):

        if partial_chunk:
            part = partial_chunk + part
        chunks, incomplete_chunk = make_chunks(file_part=part)
        partial_chunk = incomplete_chunk

        for chunk in chunks:
            incomplete_part_buffer.extend(chunk)

            # compute part checksum, when we reach the actual part size
            if len(incomplete_part_buffer) >= PART_SIZE:
                file_part = incomplete_part_buffer[:PART_SIZE]
                md5sum, sha256sum = get_part_checksums(file_part=file_part)
                encrypted_md5_part_checksums.append(md5sum)
                encrypted_sha256_part_checksums.append(sha256sum)
                incomplete_part_buffer = incomplete_part_buffer[PART_SIZE:]

            decrypted = decrypt_block(ciphersegment=part, session_keys=[secret])
            total_sha256_checksum.update(decrypted)

        if partial_chunk:
            raise exceptions.UnprocessedBytesError(chunk_length=len(partial_chunk))
        # Compute checksum for last part
        if len(incomplete_part_buffer):
            md5sum, sha256sum = get_part_checksums(file_part=file_part)
            encrypted_md5_part_checksums.append(md5sum)
            encrypted_sha256_part_checksums.append(sha256sum)

    return (
        encrypted_md5_part_checksums,
        encrypted_sha256_part_checksums,
    ), total_sha256_checksum.hexdigest()


async def retrieve_parts(*, url: str, object_size: int, offset: int = 0):
    """Get all parts from inbox, starting with file content at offset"""
    for start, stop in calc_part_ranges(
        part_size=PART_SIZE, object_size=object_size, byte_offset=offset
    ):
        yield retrieve_part(url=url, start=start, stop=stop)


async def retrieve_part(*, url: str, start: int, stop: int):
    """Get one part from inbox by range"""
    try:
        response = requests.get(
            url=url, headers={"Range": f"bytes={start}-{stop}"}, timeout=60
        )
    except requests.exceptions.RequestException as request_error:
        raise exceptions.RequestFailedError(url=url) from request_error

    return response.content


def calc_part_ranges(
    *, part_size: int, object_size: int, byte_offset: int
) -> Sequence[tuple[int, int]]:
    """
    Calculate and return the ranges (start, end) of file parts as a list of tuples.
    """
    # calc the ranges for the parts that have the full part_size:
    full_part_number = math.floor(object_size / part_size)

    part_ranges = [
        (
            byte_offset + part_size * (part_no - 1),
            byte_offset + part_size * (part_no) - 1,
        )
        for part_no in range(1, full_part_number + 1)
    ]

    if (object_size % part_size) > 0:
        # if the last part is smaller than the part_size, calculate it range separately:
        part_ranges.append(
            (byte_offset + part_size * full_part_number, object_size - 1)
        )

    return part_ranges


def make_chunks(*, file_part: bytes) -> Tuple[List[bytes], bytes]:
    """Chunk file part into decryptable chunks"""

    num_chunks = len(file_part) / CIPHER_SEGMENT_SIZE
    full_chunks = int(num_chunks)
    chunks = [
        file_part[i * CIPHER_SEGMENT_SIZE : (i + 1) * CIPHER_SEGMENT_SIZE]
        for i in range(full_chunks)
    ]

    # check if we have a remainder of bytes that we need to handle,
    # i.e. non-matching boundaries between part and cipher segment size
    incomplete_chunk = b""
    partial_chunk_idx = math.ceil(num_chunks)
    if partial_chunk_idx != full_chunks:
        incomplete_chunk = file_part[full_chunks * CIPHER_SEGMENT_SIZE :]
    return chunks, incomplete_chunk


def get_part_checksums(*, file_part: bytes):
    """Compute md5 and sha256 for encrypted part"""
    return (
        hashlib.md5(file_part, usedforsecurity=False).hexdigest(),
        hashlib.sha256(file_part).hexdigest(),
    )
