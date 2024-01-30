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
"""Contains a test fixture for generating keypairs"""

import os
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from tempfile import mkstemp

import pytest
from crypt4gh.keys import get_private_key, get_public_key
from crypt4gh.keys.c4gh import generate


@dataclass
class KeypairFixture:
    """Fixture containing a keypair"""

    public: bytes
    private: bytes

    def regenerate(self):
        """Discard old keypair values and generate new ones"""
        new_keypair = generate_keypair()
        self.public = new_keypair.public
        self.private = new_keypair.private


@pytest.fixture(scope="session")
def keypair_fixture() -> Generator[KeypairFixture, None, None]:
    """Creates a keypair using crypt4gh"""
    yield generate_keypair()


def generate_keypair() -> KeypairFixture:
    """Create a keypair using crypt4gh"""
    # Crypt4GH always writes to file and tmp_path fixture causes permission issues
    sk_file, sk_path = mkstemp(prefix="private", suffix=".key")
    pk_file, pk_path = mkstemp(prefix="public", suffix=".key")

    # Crypt4GH does not reset the umask it sets, so we need to deal with it
    original_umask = os.umask(0o022)
    generate(seckey=sk_file, pubkey=pk_file)
    public_key = get_public_key(pk_path)
    private_key = get_private_key(sk_path, lambda: None)
    os.umask(original_umask)

    Path(pk_path).unlink()
    Path(sk_path).unlink()

    return KeypairFixture(public=public_key, private=private_key)
