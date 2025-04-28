from unittest.mock import MagicMock, Mock

import pytest
from aiohttp.web import Response

from cactus_runner.app import handler
from cactus_runner.app.shared import APPKEY_RUNNER_STATE
from cactus_runner.models import ActiveTestProcedure, ClientInteraction, RunnerStatus


@pytest.mark.asyncio
async def test_status_handler(mocker):
    """
    `mocker` is a fixture provided by the `pytest-mock` plugin
    """
    request = MagicMock()
    get_active_runner_status_spy = mocker.spy(handler.status, "get_active_runner_status")

    response = await handler.status_handler(request=request)
    assert isinstance(response, Response)
    assert response.status == 200
    assert response.content_type == "application/json"
    print(f">>>{response.text}")
    get_active_runner_status_spy.assert_called_once()


@pytest.mark.asyncio
async def test_status_handler_handles_no_active_test_procedure(example_client_interaction: ClientInteraction, mocker):
    """
    `mocker` is a fixture provided by the `pytest-mock` plugin
    """
    request = MagicMock()
    request.app[APPKEY_RUNNER_STATE].active_test_procedure = None
    request.app[APPKEY_RUNNER_STATE].last_client_interaction = example_client_interaction
    get_runner_status_spy = mocker.spy(handler.status, "get_runner_status")

    response = await handler.status_handler(request=request)
    runner_status = RunnerStatus.from_json(response.text)
    assert isinstance(response, Response)
    assert response.status == 200
    assert response.content_type == "application/json"
    assert runner_status.status_summary == "No test procedure running"
    get_runner_status_spy.assert_called_once()
