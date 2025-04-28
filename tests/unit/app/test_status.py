from unittest.mock import Mock

import pytest

from cactus_runner.app import status
from cactus_runner.models import (
    ClientInteraction,
    RunnerStatus,
    StepStatus,
)


@pytest.mark.parametrize(
    "step_status,expected",
    [
        ({}, "0/0 steps complete."),
        ({"step_name": StepStatus.PENDING}, "0/1 steps complete."),
        ({"step_name": StepStatus.RESOLVED}, "1/1 steps complete."),
        (
            {"step_1": StepStatus.RESOLVED, "step_2": StepStatus.RESOLVED, "step_3": StepStatus.PENDING},
            "2/3 steps complete.",
        ),
    ],
)
def test_get_runner_status_summary(step_status, expected):
    assert status.get_runner_status_summary(step_status=step_status) == expected


def test_get_active_runner_status():
    # Arrange
    expected_test_name = "TEST_NAME"
    expected_step_status = {"step_name": StepStatus.PENDING}
    expected_status_summary = "0/1 steps complete."
    active_test_procedure = Mock()
    active_test_procedure.name = expected_test_name
    active_test_procedure.step_status = expected_step_status
    request_history = Mock()
    last_client_interaction = Mock()

    # Act
    runner_status = status.get_active_runner_status(
        active_test_procedure=active_test_procedure,
        request_history=request_history,
        last_client_interaction=last_client_interaction,
    )

    # Assert
    assert isinstance(runner_status, RunnerStatus)
    assert runner_status.last_client_interaction == last_client_interaction
    assert runner_status.request_history == request_history
    assert runner_status.test_procedure_name == expected_test_name
    assert runner_status.step_status == expected_step_status
    assert runner_status.status_summary == expected_status_summary


def test_get_active_runner_status_calls_get_runner_status_summary():
    status.get_runner_status_summary = Mock()

    expected_step_status = {"step_name": StepStatus.PENDING}
    active_test_procedure = Mock()
    active_test_procedure.step_status = expected_step_status
    request_history = Mock()
    last_client_interaction = Mock()

    _ = status.get_active_runner_status(
        active_test_procedure=active_test_procedure,
        request_history=request_history,
        last_client_interaction=last_client_interaction,
    )

    status.get_runner_status_summary.assert_called_once_with(step_status=expected_step_status)


def test_get_runner_status(example_client_interaction: ClientInteraction):
    runner_status = status.get_runner_status(last_client_interaction=example_client_interaction)

    assert isinstance(runner_status, RunnerStatus)
    assert runner_status.status_summary == "No test procedure running"
    assert runner_status.last_client_interaction == example_client_interaction
    assert runner_status.test_procedure_name == "-"
    assert runner_status.step_status is None
    assert runner_status.request_history == []
