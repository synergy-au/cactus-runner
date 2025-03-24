import http
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
from typing import Any

from cactus_test_definitions import (
    Event,
    TestProcedure,
)
from dataclass_wizard import JSONWizard


@dataclass
class Listener:
    step: str
    event: Event
    enabled: bool
    actions: list[Any]


class StepStatus(Enum):
    PENDING = 0
    RESOLVED = auto()


@dataclass
class ActiveTestProcedure:
    name: str
    definition: TestProcedure
    listeners: list[Listener]
    step_status: dict[str, StepStatus]


@dataclass
class RunnerState:
    """Represents the current state of the Runner.

    This tracks the state of an active test procedure if there is one.

    aiohttp uses the app instance as a means for sharing global data using AppKeys. We use
    this mechanism to share the active test procedure between different requests.

    However aiohttp (rightly) complains when replacing objects pointed to by AppKeys with different
    instances after the app has been started; in other words the app gets frozen.
    The reason for this, is that blindly mutating global state in async handlers could
    get someone into a mess.

    We are a special case in this regard,
    - Each runner will have only one client.
    - Even those the app supports asynchronous handling of requests, it is a reasonable
      expectation that the client will mostly interact synchronously i.e.
      they will wait for a response from the runner before issuing subsequent requests.
    - Finally care has been taken to handle requests in their entirety before returning control back
      to the async loop. We do this by not calling await on subtasks but calling them instead
      synchronously. Examples include,
        1. In 'start_test_procedure' the database operations ('register_aggregator' and 'apply_db_precondition') are handled via synchronous function calls.
        2. In 'handle_all_request_types' we update the active test procedure with the synchronous functions 'apply_action' and 'handle_event'.

    By wrapping the ActiveTestProcedure object within a RunnerState object we are
    free to mutate the `active_test_procedure` when needed and even set it to None
    when no test procedure is active without aiohttp "seeing" the mutation and complaining.
    """

    active_test_procedure: ActiveTestProcedure | None = None


@dataclass
class ActiveTestProcedureStatus(JSONWizard):
    summary: str
    step_status: dict[str, StepStatus]


@dataclass
class RunnerCapabilities(JSONWizard):
    runner_version: str
    supported_test_procedures: list[str]


@dataclass
class LastProxiedRequest(JSONWizard):
    endpoint: str
    status: http.HTTPStatus
    timestamp: datetime
