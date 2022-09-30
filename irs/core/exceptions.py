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
"""Service specific exceptions"""


class BadResponseCodeError(RuntimeError):
    """Thrown, when a request returns an unexpected response code (e.g. 500)"""

    def __init__(self, *, url: str, response_code: int):
        self.response_code = response_code
        message = f"The request to {url} failed with response code {response_code}"
        super().__init__(message)


class EnvelopeDecryptionError(RuntimeError):
    """
    Thrown when the encryption key store could not decrypt the envelope with the provided
    keypairs
    """

    def __init__(self):
        message = ""
        super().__init__(message)


class MalformedOrMissingEnvelope(RuntimeError):
    """Thrown when the when the encryption key store could not find a valid envelope"""

    def __init__(self):
        message = ""
        super().__init__(message)


class RequestFailedError(RuntimeError):
    """Thrown when a request fails without returning a response code"""

    def __init__(self, *, url: str):
        message = f"The request to {url} failed."
        super().__init__(message)


class UnprocessedBytesError(RuntimeError):
    """Raised when a byte chunk remains after processing all file parts"""

    def __init__(self, *, chunk_length: int):
        message = f"{chunk_length} unprocessed bytes encountered at at the file end"
        super().__init__(message)
