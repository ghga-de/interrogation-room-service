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
"""HTTP calls to other service APIs happen here"""

import base64

import requests

from irs.adapters.outbound.http import exceptions
from irs.adapters.outbound.http.exception_translation import ResponseExceptionTranslator


def call_eks_api(
    *, file_part: bytes, public_key: str, api_url: str
) -> tuple[bytes, bytes, str, int]:
    """Call EKS to get encryption secret and file content offset from envelope"""
    data = base64.b64encode(file_part).decode("utf-8")
    request_body = {"public_key": public_key, "file_part": data}
    try:
        response = requests.post(url=api_url, json=request_body, timeout=60)
    except requests.exceptions.RequestException as request_error:
        raise exceptions.RequestFailedError(url=api_url) from request_error

    status_code = response.status_code
    # implement httpyexpect error conversion
    if status_code != 200:
        spec = {
            400: {
                "malformedOrMissingEnvelopeError": exceptions.MalformedOrMissingEnvelope
            },
            403: {"envelopeDecryptionError": exceptions.EnvelopeDecryptionError},
            502: {"secretInsertionError": exceptions.SecretInsertionError},
            504: {"vaultConnectionError": exceptions.VaultConnectionError},
        }
        ResponseExceptionTranslator(spec=spec).handle(response=response)
        raise exceptions.BadResponseCodeError(url=api_url, response_code=status_code)

    body = response.json()
    submitter_secret = base64.b64decode(body["submitter_secret"])
    new_secret = base64.b64decode(body["new_secret"])
    secret_id = body["secret_id"]
    offset = body["offset"]

    return submitter_secret, new_secret, secret_id, offset
