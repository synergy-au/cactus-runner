import http
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from aiohttp import client, web

SERVER_URL = "http://localhost:8000"
MOUNT_POINT = "/"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class Event:
    event_type: str
    parameters: dict


@dataclass
class Listener:
    step: str
    event: Event
    enabled: bool
    actions: list[Any]


@dataclass
class TestProcedure:
    name: str
    definition: dict
    listeners: list[Listener]


def apply_db_precondition(precondition):
    print(f"Applying {precondition=} to the CSIP-AUS database")


async def start_test_procedure(request: web.Request):
    global current_test_procedure

    # We cannot start another test procedure if one is already running
    if current_test_procedure is not None:
        return web.Response(
            status=http.HTTPStatus.CONFLICT,
            text=f"Test Procedure ({current_test_procedure.name}) already in progress. Starting another test procedure is not permitted.",
        )

    # Get the name of the test procedure from the query parameter
    requested_test_procedure = request.query["test"]
    if requested_test_procedure is None:
        return web.Response(status=http.HTTPStatus.BAD_REQUEST, text="Missing 'test' query parameter.")

    # Get the definition of the test procedure
    try:
        definition = test_procedures["TestProcedures"][requested_test_procedure]
    except KeyError:
        return web.Response(
            status=http.HTTPStatus.BAD_REQUEST,
            text=f"Expected valid test procedure for 'test' query parameter. Received '/start=?test={requested_test_procedure}'",
        )

    # Create listeners for all test procedure events
    raw_listeners = definition["Preconditions"]["runner"]["event-listeners"]
    listeners = []
    for l in raw_listeners:
        step_name = list(l.keys())[0]
        step = definition["Steps"][step_name]
        step_event = step["event"]
        event = Event(event_type=step_event["type"], parameters=step_event["parameters"])
        actions = step["actions"]
        enabled = list(l.values())[0] == "enabled"
        listeners.append(Listener(step=step_name, event=event, actions=actions, enabled=enabled))

    # Set 'current_test_procedure' to the requested test procedure
    current_test_procedure = TestProcedure(name=requested_test_procedure, definition=definition, listeners=listeners)

    # Get the database into the correct state for the test procedure
    db_precondition = current_test_procedure.definition["Preconditions"]["db"]
    apply_db_precondition(precondition=db_precondition)

    return web.Response(status=http.HTTPStatus.CREATED, text="Test Procedure Started")


async def finalize_test_procedure(request):
    global current_test_procedure

    current_test_procedure = None

    return web.Response(status=http.HTTPStatus.OK, text="Test Procedure Finalized")


async def test_procedure_status(request):
    if current_test_procedure is not None:
        text = f"Test procedure '{current_test_procedure.name}' running"
    else:
        text = "No test procedure running"
    return web.Response(status=http.HTTPStatus.OK, text=text)


async def handle_all_request_types(request):
    proxy_path = request.match_info.get("proxyPath", "No proxyPath placeholder defined")
    local_path = request.rel_url.path_qs
    remote_url = SERVER_URL + local_path

    print(f"{proxy_path=} {local_path=} {remote_url=}")

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
    app.router.add_route("POST", MOUNT_POINT + "start", start_test_procedure)
    app.router.add_route("POST", MOUNT_POINT + "finalize", finalize_test_procedure)
    app.router.add_route("GET", MOUNT_POINT + "status", test_procedure_status)

    # Add catch-all route for proxying all other requests to CSIP-AUS reference server
    app.router.add_route("*", MOUNT_POINT + "{proxyPath:.*}", handle_all_request_types)

    return app


def read_test_procedure_definitions(path: Path) -> dict:
    with path.open() as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    test_procedures: dict = read_test_procedure_definitions(path=Path("config/test_procedure.yaml"))

    current_test_procedure = None

    app = create_application()
    web.run_app(app, port=8080)
