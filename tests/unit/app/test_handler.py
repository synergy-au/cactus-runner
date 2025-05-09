import http
from unittest.mock import MagicMock

import pytest
from aiohttp.web import Response

from cactus_runner.app import handler
from cactus_runner.app.shared import APPKEY_RUNNER_STATE
from cactus_runner.models import ClientInteraction, RunnerStatus


@pytest.mark.asyncio
async def test_finalize_handler(mocker):
    """
    `mocker` is a fixture provided by the `pytest-mock` plugin
    """

    request = MagicMock()
    get_active_runner_status_spy = mocker.spy(handler.status, "get_active_runner_status")
    create_response_spy = mocker.spy(handler.finalize, "create_response")
    mocker.patch("cactus_runner.app.finalize.get_zip_contents")

    response = await handler.finalize_handler(request=request)

    assert isinstance(response, Response)
    create_response_spy.assert_called_once()
    get_active_runner_status_spy.assert_called_once()


@pytest.mark.asyncio
async def test_finalize_handler_resets_runner_state(mocker):
    request = MagicMock()
    request.app[APPKEY_RUNNER_STATE].request_history = [None]  # a non-empty list stand-in
    mocker.patch("cactus_runner.app.finalize.create_response")

    _ = await handler.finalize_handler(request=request)

    assert request.app[APPKEY_RUNNER_STATE].active_test_procedure is None
    assert not request.app[APPKEY_RUNNER_STATE].request_history  # the list should be empty


@pytest.mark.asyncio
async def test_finalize_handler_handles_no_active_test_procedure():

    request = MagicMock()
    request.app[APPKEY_RUNNER_STATE].active_test_procedure = None

    response = await handler.finalize_handler(request=request)

    assert isinstance(response, Response)
    assert response.status == http.HTTPStatus.BAD_REQUEST


@pytest.mark.asyncio
async def test_status_handler(mocker):
    """
    `mocker` is a fixture provided by the `pytest-mock` plugin
    """
    request = MagicMock()
    get_active_runner_status_spy = mocker.spy(handler.status, "get_active_runner_status")

    response = await handler.status_handler(request=request)
    assert isinstance(response, Response)
    assert response.status == http.HTTPStatus.OK
    assert response.content_type == "application/json"
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
    assert response.status == http.HTTPStatus.OK
    assert response.content_type == "application/json"
    assert runner_status.status_summary == "No test procedure running"
    get_runner_status_spy.assert_called_once()


@pytest.mark.asyncio
async def test_proxied_request_handler_logs_error_with_no_active_test_procedure(mocker):
    # Arrange
    request = MagicMock()
    request.app[APPKEY_RUNNER_STATE].active_test_procedure = None
    mock_logger_warning = mocker.patch("cactus_runner.app.handler.logger.error")

    # Act
    response = await handler.proxied_request_handler(request=request)

    # Assert
    mock_logger_warning.assert_called_once()
    assert isinstance(response, Response)
    assert response.status == http.HTTPStatus.BAD_REQUEST
