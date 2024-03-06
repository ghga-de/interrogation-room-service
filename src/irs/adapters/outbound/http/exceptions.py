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
"""Service specific exceptions"""


class EnvelopeError(RuntimeError):
    """Base class for custom errors encountered"""


class TransientError(RuntimeError):
    """Base class for intermittent errors"""


class BadResponseCodeError(TransientError):
    """Thrown, when a request returns an unexpected response code (e.g. 500)"""

    def __init__(self, *, url: str, response_code: int):
        self.response_code = response_code
        message = f"The request to {url} failed with response code {response_code}"
        super().__init__(message)


class EnvelopeDecryptionError(EnvelopeError):
    """
    Thrown when the encryption key store could not decrypt the envelope with the provided
    keypairs
    """

    def __init__(self):
        message = "Envelope could not be decrypted with provided keys."
        super().__init__(message)


class MalformedOrMissingEnvelope(EnvelopeError):
    """Thrown when the when the encryption key store could not find a valid envelope"""

    def __init__(self):
        message = "The crypt4GH envelope is either malformed or missing."
        super().__init__(message)


class RequestFailedError(TransientError):
    """Thrown when a request fails without returning a response code"""

    def __init__(self, *, url: str):
        message = f"The request to {url} failed."
        super().__init__(message)


class SecretInsertionError(TransientError):
    """Thrown when an internal vault error causes secret storage to fail"""

    def __init__(self):
        message = "An internal error occurred when trying to store the secret."
        super().__init__(message)


class VaultConnectionError(TransientError):
    """Thrown when a connection error causes secret storage to fail"""

    def __init__(self):
        message = "Could not save secret due to connection issues."
        super().__init__(message)
