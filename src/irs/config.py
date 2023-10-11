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

"""Config Parameter Modeling and Parsing"""
from hexkit.config import config_from_yaml
from hexkit.providers.akafka import KafkaConfig
from hexkit.providers.s3 import S3Config
from pydantic import Field

from irs.adapters.inbound.event_sub import EventSubTanslatorConfig
from irs.adapters.outbound.event_pub import EventPubTanslatorConfig


@config_from_yaml(prefix="irs")
class Config(KafkaConfig, S3Config, EventSubTanslatorConfig, EventPubTanslatorConfig):
    """Config parameters and their defaults."""

    service_name: str = "interrogation_room"
    staging_bucket: str = Field(
        ...,
        example="staging",
        description=("Bucket ID representing the staging area for re-encrypted files."),
    )
    eks_url: str = Field(
        ...,
        example="http://127.0.0.1/eks",
        description=("URL pointing to the Encryption Key Store service."),
    )


CONFIG = Config()  # type: ignore [call-arg]
