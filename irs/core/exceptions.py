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
"""Custom exceptions for errors encountered during ciphersegment processing"""


class SegmentCorruptedError(Exception):
    """Thrown when any of the full-length ciphersegments could not be decrypted"""

    def __init__(self, *, part_number: int):
        message = f"Failed decrypting ciphersegment in current file part {part_number}."
        super().__init__(message)


class LastSegmentCorruptedError(Exception):
    """Thrown when the last, possibly incomplete ciphersegment could not be decrypted"""

    def __init__(self):
        message = "Failed decrypting the last ciphersegment."
        super().__init__(message)
