import http
import logging
import logging.config
import os
from datetime import datetime, timezone
from pathlib import Path

from aiohttp import client, web
from cactus_test_definitions import (
    Action,
    Event,
)
from envoy.server.api.depends.lfdi_auth import LFDIAuthDepends

from cactus_runner import __version__
from cactus_runner.app import auth, finalize, precondition, status
from cactus_runner.app.env import (
    DEV_AGGREGATOR_PREREGISTERED,
    DEV_SKIP_AUTHORIZATION_CHECK,
    DEV_SKIP_DB_PRECONDITIONS,
    SERVER_URL,
)
from cactus_runner.app.shared import (
    APPKEY_AGGREGATOR,
    APPKEY_RUNNER_STATE,
    APPKEY_TEST_PROCEDURES,
)
from cactus_runner.models import (
    ActiveTestProcedure,
    ClientInteraction,
    ClientInteractionType,
    InitResponseBody,
    Listener,
    RequestEntry,
    StartResponseBody,
    StepStatus,
)

logger = logging.getLogger(__name__)


class UnknownActionError(Exception):
    """Unknown Cactus Runner Action"""


async def init_handler(request: web.Request):
    """Initializes a test procedure

    1. Register the aggregator (along with certificate)
    2. Apply database preconditions
    3. Start the envoy server with the correction configuration
    """
    active_test_procedure = request.app[APPKEY_RUNNER_STATE].active_test_procedure
    test_procedures = request.app[APPKEY_TEST_PROCEDURES]

    # We cannot initialise another test procedure if one is already active
    if active_test_procedure is not None:
        return web.Response(
            status=http.HTTPStatus.CONFLICT,
            text=f"Test Procedure ({active_test_procedure.name}) already active. Initialising another test procedure is not permitted.",
        )

    # Update last client interaction
    request.app[APPKEY_RUNNER_STATE].last_client_interaction = ClientInteraction(
        interaction_type=ClientInteractionType.TEST_PROCEDURE_INIT, timestamp=datetime.now(timezone.utc)
    )

    # Get the name of the test procedure from the query parameter
    requested_test_procedure = request.query["test"]
    if requested_test_procedure is None:
        return web.Response(status=http.HTTPStatus.BAD_REQUEST, text="Missing 'test' query parameter.")

    # Get the certificate of the aggregator to register
    aggregator_certificate = request.query["certificate"]
    if aggregator_certificate is None:
        return web.Response(status=http.HTTPStatus.BAD_REQUEST, text="Missing 'certificate' query parameter.")

    # Get the lfdi of the aggregator to register
    aggregator_lfdi = LFDIAuthDepends.generate_lfdi_from_pem(aggregator_certificate)
    if not DEV_AGGREGATOR_PREREGISTERED:
        precondition.register_aggregator(lfdi=aggregator_lfdi)
    else:
        logger.warning("Skipping aggregator registration ('DEV_AGGREGATOR_PREREGISTERED' environment variable is True)")

    # Save the aggregator details for later request validation
    request.app[APPKEY_AGGREGATOR].certificate = aggregator_certificate
    request.app[APPKEY_AGGREGATOR].lfdi = aggregator_lfdi

    logger.debug(f"{aggregator_certificate=}")
    logger.debug(f"{aggregator_lfdi=}")

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
        listeners.append(Listener(step=step_name, event=step.event, actions=step.actions))

    # Set 'active_test_procedure' to the requested test procedure
    active_test_procedure = ActiveTestProcedure(
        name=requested_test_procedure,
        definition=definition,
        listeners=listeners,
        step_status={step: StepStatus.PENDING for step in definition.steps.keys()},
    )

    # Apply preconditions (if present)
    precond = active_test_procedure.definition.preconditions
    if precond:
        # Get the database into the correct state for the test procedure
        if precond.db:
            if DEV_SKIP_DB_PRECONDITIONS:
                logger.warning(
                    "Skipping database preconditions ('DEV_SKIP_DB_PRECONDITIONS' environment variable is True)"
                )
            else:
                precondition.apply_db_precondition(precondition=precond.db)

    logger.info(
        f"Test Procedure '{active_test_procedure.name}' started",
        extra={"test_procedure": active_test_procedure.name},
    )

    request.app[APPKEY_RUNNER_STATE].active_test_procedure = active_test_procedure

    # Trigger the envoy server to be started
    DEFAULT_SHARED_VOLUME = "shared"
    SHARED_VOLUME = Path(os.getenv("SHARED_VOLUME", DEFAULT_SHARED_VOLUME))

    with open(SHARED_VOLUME / ".env", "w") as fp:
        env_vars = active_test_procedure.definition.envoy_environment_variables
        if env_vars:
            for env_var_name, env_var_value in env_vars.items():
                fp.write(f'{env_var_name}="{env_var_value}"')

    # Write an empty file to signal to cactus orchestrator that we are ready for envoy to be started
    with open(SHARED_VOLUME / "envoy.kickstart", "w") as fp:
        pass

    # TODO Should we put a sleep or ping an envoy server healthcheck here before returning a response?

    body = InitResponseBody(
        status="Test procedure initialised.",
        test_procedure=active_test_procedure.name,
        timestamp=datetime.now(timezone.utc),
    )
    return web.Response(status=http.HTTPStatus.CREATED, content_type="application/json", text=body.to_json())


async def start_handler(request: web.Request):
    active_test_procedure = request.app[APPKEY_RUNNER_STATE].active_test_procedure

    # We cannot start a test procedure if one hasn't been initialized
    if active_test_procedure is None:
        return web.Response(
            status=http.HTTPStatus.CONFLICT,
            text=f"Unable to start non-existent test procedure. Try initialising a test procedure before continuing.",
        )

    # We cannot start another test procedure if one is already running.
    # If there are active listeners then the test procedure must have already been started.
    listener_state = [listener.enabled for listener in active_test_procedure.listeners]
    if any(listener_state):
        return web.Response(
            status=http.HTTPStatus.CONFLICT,
            text=f"Test Procedure ({active_test_procedure.name}) already in progress. Starting another test procedure is not permitted.",
        )

    # Update last client interaction
    request.app[APPKEY_RUNNER_STATE].last_client_interaction = ClientInteraction(
        interaction_type=ClientInteractionType.TEST_PROCEDURE_START, timestamp=datetime.now(timezone.utc)
    )

    # Active the first listener
    if active_test_procedure.listeners:
        active_test_procedure.listeners[0].enabled = True

    logger.info(
        f"Test Procedure '{active_test_procedure.name}' started",
        extra={"test_procedure": active_test_procedure.name},
    )

    request.app[APPKEY_RUNNER_STATE].active_test_procedure = active_test_procedure

    body = StartResponseBody(
        status="Test procedure started.",
        test_procedure=active_test_procedure.name,
        timestamp=datetime.now(timezone.utc),
    )
    return web.Response(status=http.HTTPStatus.OK, content_type="application/json", text=body.to_json())


async def finalize_handler(request):
    active_test_procedure = request.app[APPKEY_RUNNER_STATE].active_test_procedure

    if active_test_procedure is not None:
        finalized_test_procedure_name = active_test_procedure.name
        json_status_summary = status.get_active_runner_status(
            active_test_procedure=active_test_procedure,
            request_history=request.app[APPKEY_RUNNER_STATE].request_history,
            last_client_interaction=request.app[APPKEY_RUNNER_STATE].last_client_interaction,
        ).to_json()

        # Clear the active test procedure and request history
        request.app[APPKEY_RUNNER_STATE].active_test_procedure = None
        request.app[APPKEY_RUNNER_STATE].request_history.clear()

        logger.info(
            f"Test Procedure '{finalized_test_procedure_name}' finalized",
            extra={"test_procedure": finalized_test_procedure_name},
        )

        return finalize.create_response(
            json_status_summary=json_status_summary, runner_logfile="logs/cactus_runner.jsonl"
        )
    else:
        return web.Response(
            status=http.HTTPStatus.BAD_REQUEST,
            text="ERROR: Unable to finalize test procedure. No test procedure in progress.",
        )


async def status_handler(request):
    active_test_procedure = request.app[APPKEY_RUNNER_STATE].active_test_procedure

    logger.info("Test procedure status requested.")

    if active_test_procedure is not None:
        runner_status = status.get_active_runner_status(
            active_test_procedure=active_test_procedure,
            request_history=request.app[APPKEY_RUNNER_STATE].request_history,
            last_client_interaction=request.app[APPKEY_RUNNER_STATE].last_client_interaction,
        )
        logger.info(
            f"Status of test procedure '{runner_status.test_procedure_name}': {runner_status.step_status}",
            extra={"test_procedure": runner_status.test_procedure_name},
        )

    else:
        runner_status = status.get_runner_status(
            last_client_interaction=request.app[APPKEY_RUNNER_STATE].last_client_interaction
        )
        logger.warning("Status of non-existent test procedure requested.")

    return web.Response(status=http.HTTPStatus.OK, content_type="application/json", text=runner_status.to_json())


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


async def proxied_request_handler(request):
    # Store when request received
    request_timestamp = datetime.now(timezone.utc)

    # Only proceed if authorized
    if not (DEV_SKIP_AUTHORIZATION_CHECK or auth.request_is_authorized(request=request)):
        return web.Response(
            status=http.HTTPStatus.FORBIDDEN, text="Forwarded certificate does not match for registered aggregator"
        )

    # Update last client interaction
    request.app[APPKEY_RUNNER_STATE].last_client_interaction = ClientInteraction(
        interaction_type=ClientInteractionType.PROXIED_REQUEST, timestamp=request_timestamp
    )

    active_test_procedure = request.app[APPKEY_RUNNER_STATE].active_test_procedure

    proxy_path = request.match_info.get("proxyPath", "No proxyPath placeholder defined")
    local_path = request.rel_url.path_qs
    remote_url = SERVER_URL + local_path
    method = request.method

    logger.debug(f"{proxy_path=} {local_path=} {remote_url=} {method=}")

    # 'IGNORED' indicates request wasn't recognised by the test procedure and didn't progress it any further
    step_name = "IGNORED"
    if active_test_procedure is not None:
        # Update the progress of the test procedure
        request_event = Event(type=f"{method}-request-received", parameters={"endpoint": f"/{proxy_path}"})
        listener = handle_event(event=request_event, active_test_procedure=active_test_procedure)

        # The assumes each step only has one event and once the action associated with the event
        # has been handled the step is "complete"
        if listener is not None:
            active_test_procedure.step_status[listener.step] = StepStatus.RESOLVED
            step_name = listener.step

    # Forward the request to the reference server
    async with client.request(
        request.method, remote_url, headers=request.headers.copy(), allow_redirects=False, data=await request.read()
    ) as response:
        headers = response.headers.copy()
        status = http.HTTPStatus(response.status)
        body = await response.read()

    if active_test_procedure is not None:
        # Record in request history
        request_entry = RequestEntry(
            url=remote_url,
            path=local_path,
            method=http.HTTPMethod(method),
            status=status,
            timestamp=request_timestamp,
            step_name=step_name,
        )
        request.app[APPKEY_RUNNER_STATE].request_history.append(request_entry)

    return web.Response(headers=headers, status=status, body=body)
