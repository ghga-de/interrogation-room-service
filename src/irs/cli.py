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

"""Entrypoint of the package"""

import asyncio

import typer

from irs.main import check_staging_buckets, consume_events

cli = typer.Typer()


@cli.command(name="consume-events")
def sync_consume_events():
    """Synchronous entrypoint for Docker container, etc."""
    asyncio.run(consume_events())


@cli.command(name="check-staging-buckets")
def sync_check_staging_buckets():
    """Run a job to check all objects no longer needed have been deleted"""
    asyncio.run(check_staging_buckets())
