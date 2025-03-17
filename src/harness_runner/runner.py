import atexit
import http
import json
import logging
import logging.config
import os
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Any

from aiohttp import client, web
from dataclass_wizard import JSONWizard

from harness_runner import __version__, precondition
from harness_runner.config import (
    Action,
    Event,
    TestProcedure,
    TestProcedureConfig,
    TestProcedures,
)

# SERVER_URL is the URL of envoy or another CSIP-AUS compliant server.
DEFAULT_SERVER_URL = "http://localhost:8000"
SERVER_URL = os.getenv("SERVER_URL", DEFAULT_SERVER_URL)

# APP_HOST is the IP address of harness runner (aiohttp) application
# See https://docs.aiohttp.org/en/stable/web_reference.html#aiohttp.web.run_app
DEFAULT_APP_HOST = "0.0.0.0"  # This is the aiohttp default
APP_HOST = os.getenv("APP_HOST", DEFAULT_APP_HOST)

# APP_PORT is the port the harness runner application listens on.
DEFAULT_APP_PORT = 8080  # This is the aiohttp default
APP_PORT = os.getenv("APP_PORT", DEFAULT_APP_PORT)

# MOUNT_POINT is the base path for all endpoints
MOUNT_POINT = "/"

DEV_AGGREGATOR_PREREGISTERED = os.getenv("DEV_AGGREGATOR_PREREGISTERED", "false").lower() in ["true", "1", "t"]


logger = logging.getLogger(__name__)


class UnknownActionError(Exception):
    """Unknown harness runner action"""

    pass


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
    """Represents the current state of the Harness Runner.

    This tracks the state of an active test procedure if there is one.

    aiohttp uses the app instance as a means for sharing global data using AppKeys. We use
    this mechanism to share the active test procedure between different requests.

    However aiohttp (rightly) complains when replacing objects pointed to by AppKeys with different
    instances after the app has been started; in other words the app gets frozen.
    The reason for this, is that blindly mutating global state in async handlers could
    get someone into a mess.

    We are a special case in this regard,
    - Each harness runner will have only one client.
    - Even those the app supports asynchronous handling of requests, it is a reasonable
      expectation that the client will mostly interact synchronously i.e.
      they will wait for a response from the harness runner before issuing subsequent requests.
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
class HarnessCapabilities(JSONWizard):
    harness_runner_version: str
    supported_test_procedures: list[str]


# aiohttp AppKeys are used to share global state between request handlers
test_procedures_key = web.AppKey("test-procedures", TestProcedures)
runner_state_key = web.AppKey("runner-state", RunnerState)


async def start_test_procedure(request: web.Request):
    active_test_procedure = request.app[runner_state_key].active_test_procedure
    test_procedures = request.app[test_procedures_key]

    # We cannot start another test procedure if one is already running
    if active_test_procedure is not None:
        return web.Response(
            status=http.HTTPStatus.CONFLICT,
            text=f"Test Procedure ({active_test_procedure.name}) already in progress. Starting another test procedure is not permitted.",
        )

    # Get the name of the test procedure from the query parameter
    requested_test_procedure = request.query["test"]
    if requested_test_procedure is None:
        return web.Response(status=http.HTTPStatus.BAD_REQUEST, text="Missing 'test' query parameter.")

    # Get the lfdi of the aggregator to register
    aggregator_lfdi = request.query["lfdi"]
    if aggregator_lfdi is None:
        return web.Response(status=http.HTTPStatus.BAD_REQUEST, text="Missing 'lfdi' query parameter.")
    if not DEV_AGGREGATOR_PREREGISTERED:
        precondition.register_aggregator(lfdi=aggregator_lfdi)
    else:
        logger.warning("Skipping aggregator registration ('DEV_AGGREGATOR_PREREGISTERED' environment variable is True)")

    # Get the definition of the test procedure
    try:
        definition = test_procedures.test_procedures[requested_test_procedure]
    except KeyError:
        return web.Response(
            status=http.HTTPStatus.BAD_REQUEST,
            text=f"Expected valid test procedure for 'test' query parameter. Received '/start=?test={requested_test_procedure}'",
        )

    # Create listeners for all test procedure events
    listeners = []
    for step_name, step in definition.steps.items():
        listeners.append(
            Listener(step=step_name, event=step.event, actions=step.actions, enabled=step.listener_enabled)
        )

    # Set 'active_test_procedure' to the requested test procedure
    active_test_procedure = ActiveTestProcedure(
        name=requested_test_procedure,
        definition=definition,
        listeners=listeners,
        step_status={step: StepStatus.PENDING for step in definition.steps.keys()},
    )

    # Get the database into the correct state for the test procedure
    db_precondition = active_test_procedure.definition.preconditions.db
    precondition.apply_db_precondition(precondition=db_precondition)

    logger.info(
        f"Test Procedure '{active_test_procedure.name}' started",
        extra={"test_procedure": active_test_procedure.name},
    )

    request.app[runner_state_key].active_test_procedure = active_test_procedure

    return web.Response(status=http.HTTPStatus.CREATED, text="Test Procedure Started")


async def finalize_test_procedure(request):
    active_test_procedure = request.app[runner_state_key].active_test_procedure

    if active_test_procedure is not None:
        finalized_test_procedure_name = active_test_procedure.name
        # active_test_procedure = None
        request.app[runner_state_key].active_test_procedure = None

        logger.info(
            f"Test Procedure '{finalized_test_procedure_name}' finalized",
            extra={"test_procedure": finalized_test_procedure_name},
        )

        return web.Response(status=http.HTTPStatus.OK, text="Test Procedure Finalized")
    else:
        return web.Response(
            status=http.HTTPStatus.BAD_REQUEST,
            text="ERROR: Unable to finalize test procedure. No test procedure in progress.",
        )


async def test_procedure_status(request):
    active_test_procedure = request.app[runner_state_key].active_test_procedure

    logger.info("Test procedure status requested.")

    if active_test_procedure is not None:
        name = active_test_procedure.name
        completed_steps = sum(s == StepStatus.RESOLVED for s in active_test_procedure.step_status.values())
        steps = len(active_test_procedure.step_status)
        status = f"{completed_steps}/{steps} steps complete."
        logger.info(
            f"Status of test procedure '{name}': {active_test_procedure.step_status}", extra={"test_procedure": name}
        )

        status = ActiveTestProcedureStatus(
            summary=f"Test procedure '{name}' running: {status}", step_status=active_test_procedure.step_status
        )

    else:
        logger.warning("Status of non-existent test procedure requested.")
        status = ActiveTestProcedureStatus(summary="No test procedure running", step_status={})

    return web.Response(status=http.HTTPStatus.OK, content_type="application/json", text=status.to_json())


async def harness_capabilities(request):
    test_procedures = request.app[test_procedures_key]

    logger.info("Test harness capabilities requested.")

    capabilities = HarnessCapabilities(
        harness_runner_version=test_procedures.version,
        supported_test_procedures=[
            test_procedure_name for test_procedure_name in test_procedures.test_procedures.keys()
        ],
    )

    return web.Response(status=http.HTTPStatus.OK, content_type="application/json", text=capabilities.to_json())


def apply_action(action: Action, active_test_procedure: ActiveTestProcedure):

    match action.type:
        case "enable-listeners":
            steps_to_enable = action.parameters["listeners"]
            for listener in active_test_procedure.listeners:
                if listener.step in steps_to_enable:
                    logger.info(f"Enabling listener: {listener}")
                    listener.enabled = True
                    steps_to_enable.remove(listener.step)

            # Warn about any unmatched steps
            if steps_to_enable:
                logger.warning(
                    f"Unable to enable the listeners for the following steps, ({steps_to_enable}). These are not recognised steps in the '{active_test_procedure.name} test procedure"
                )
        case "remove-listeners":
            steps_to_disable = action.parameters["listeners"]
            for listener in active_test_procedure.listeners:
                if listener.step in steps_to_disable:
                    logger.info(f"Remove listener: {listener}")
                    active_test_procedure.listeners.remove(listener)
                    steps_to_disable.remove(listener.step)

            # Warn about any unmatched steps
            if steps_to_disable:
                logger.warning(
                    f"Unable to remove the listener from the following steps, ({steps_to_disable}). These are not recognised steps in the '{active_test_procedure.name}' test procedure"
                )
        case _:
            raise UnknownActionError(f"Unrecognised action '{action}'")


def handle_event(event: Event, active_test_procedure: ActiveTestProcedure) -> Listener | None:

    # Check all listeners
    for listener in active_test_procedure.listeners:
        # Did any of the current listeners match?
        if listener.enabled and listener.event == event:
            logger.info(f"Event matched: {event=}")

            # Perform actions associated with event
            for action in listener.actions:
                logger.info(f"Executing action: {action=}")
                apply_action(action=action, active_test_procedure=active_test_procedure)

            return listener

    return None


async def handle_all_request_types(request):
    active_test_procedure = request.app[runner_state_key].active_test_procedure

    proxy_path = request.match_info.get("proxyPath", "No proxyPath placeholder defined")
    local_path = request.rel_url.path_qs
    remote_url = SERVER_URL + local_path

    logger.debug(f"{proxy_path=} {local_path=} {remote_url=}")

    if active_test_procedure is not None:
        # Update the progress of the test procedure
        request_event = Event(type="request-received", parameters={"endpoint": f"/{proxy_path}"})
        listener = handle_event(event=request_event, active_test_procedure=active_test_procedure)

        # The assumes each step only has one event and once the action associated with the event
        # has been handled the step is "complete"
        if listener is not None:
            active_test_procedure.step_status[listener.step] = StepStatus.RESOLVED

    # Forward the request to the reference server
    async with client.request(
        request.method, remote_url, headers=request.headers.copy(), allow_redirects=False, data=await request.read()
    ) as response:
        headers = response.headers.copy()
        body = await response.read()
        return web.Response(headers=headers, status=response.status, body=body)


def create_application():
    app = web.Application()

    # Add routes for Test Runner
    app.router.add_route("GET", MOUNT_POINT + "status", test_procedure_status)
    app.router.add_route("GET", MOUNT_POINT + "capability", harness_capabilities)
    app.router.add_route("POST", MOUNT_POINT + "start", start_test_procedure)
    app.router.add_route("POST", MOUNT_POINT + "finalize", finalize_test_procedure)

    # Add catch-all route for proxying all other requests to CSIP-AUS reference server
    app.router.add_route("*", MOUNT_POINT + "{proxyPath:.*}", handle_all_request_types)

    return app


def setup_logging(logging_config_file: Path):
    with open(logging_config_file) as f:
        config = json.load(f)

    logging.config.dictConfig(config)

    queue_handler = logging.getHandlerByName("queue_handler")
    if queue_handler is not None:
        queue_handler.listener.start()
        atexit.register(queue_handler.listener.stop)


def main():
    setup_logging(logging_config_file=Path("config/logging/config.json"))
    logger.info(f"Harness Runner (version={__version__})")
    logger.info(f"{APP_HOST=} {APP_PORT=}")
    logger.info(f"Proxying requests to '{SERVER_URL}'")

    app = create_application()
    app[runner_state_key] = RunnerState()
    app[test_procedures_key] = TestProcedureConfig.from_yamlfile(path=Path("config/test_procedure.yaml"))

    web.run_app(app, port=APP_PORT)


if __name__ == "__main__":
    main()
