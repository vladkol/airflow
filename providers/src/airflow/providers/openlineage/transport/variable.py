# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from typing import TYPE_CHECKING

from openlineage.client.serde import Serde
from openlineage.client.transport import Transport

from airflow.models.variable import Variable
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from openlineage.client.client import Event


class VariableTransport(Transport, LoggingMixin):
    """
    This transport sends OpenLineage events to Variables.

    Key schema is <DAG_ID>.<TASK_ID>.event.<EVENT_TYPE>.
    It's made to be used in system tests, stored data read by OpenLineageTestOperator.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        ...

    def emit(self, event: Event):
        key = f"{event.job.name}.event.{event.eventType.value.lower()}"  # type: ignore[union-attr]
        self.log.error("SETTING KEY %s", key)
        event_str = Serde.to_json(event)
        if (var := Variable.get(key, default_var=None, deserialize_json=True)) is not None:
            Variable.set(key, value=var + [event_str], serialize_json=True)
        else:
            Variable.set(key=key, value=[event_str], serialize_json=True)
