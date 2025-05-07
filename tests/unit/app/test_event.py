from unittest.mock import MagicMock, Mock

import pytest
from cactus_test_definitions import Action, Event

from cactus_runner.app import event
from cactus_runner.models import Listener


@pytest.mark.parametrize(
    "action,apply_function_name",
    [
        (Action(type="enable-listeners", parameters={"listeners": []}), "_apply_enable_listeners"),
        (Action(type="remove-listeners", parameters={"listeners": []}), "_apply_remove_listeners"),
    ],
)
def test__apply_action(mocker, action: Action, apply_function_name: str):
    # Arrange
    active_test_procedure = MagicMock()
    mock_apply_function = mocker.patch(f"cactus_runner.app.event.{apply_function_name}")

    # Act
    event._apply_action(action=action, active_test_procedure=active_test_procedure)

    # Assert
    mock_apply_function.assert_called_once()


def test__apply_action_raise_exception_for_unknown_action_type():
    active_test_procedure = MagicMock()

    with pytest.raises(event.UnknownActionError):
        event._apply_action(
            action=Action(type="NOT-A-VALID-ACTION-TYPE", parameters={}), active_test_procedure=active_test_procedure
        )


@pytest.mark.parametrize(
    "test_event,listeners,matching_listener_index",
    [
        (
            Event(type="GET-request-received", parameters={"endpoint": "/dcap"}),
            [
                Listener(
                    step="step",
                    event=Event(type="GET-request-received", parameters={"endpoint": "/dcap"}),
                    actions=[],
                    enabled=True,
                )
            ],
            0,
        ),
        (
            Event(type="GET-request-received", parameters={"endpoint": "/dcap"}),
            [
                Listener(
                    step="step",
                    event=Event(type="GET-request-received", parameters={"endpoint": "/edev"}),
                    actions=[],
                    enabled=True,
                ),
                Listener(
                    step="step",
                    event=Event(type="GET-request-received", parameters={"endpoint": "/dcap"}),
                    actions=[],
                    enabled=True,
                ),
            ],
            1,
        ),
        (
            Event(type="GET-request-received", parameters={"endpoint": "/dcap"}),
            [
                Listener(
                    step="step",
                    event=Event(type="POST-request-received", parameters={"endpoint": "/dcap"}),
                    actions=[],
                    enabled=True,
                ),
                Listener(
                    step="step",
                    event=Event(type="GET-request-received", parameters={"endpoint": "/dcap"}),
                    actions=[],
                    enabled=True,
                ),
            ],
            1,
        ),
    ],
)
def test_handle_event_with_matching_listener(
    test_event: Event, listeners: list[Listener], matching_listener_index: int
):
    # Arrange
    active_test_procedure = MagicMock()
    active_test_procedure.listeners = listeners

    # Act
    matched_listener = event.handle_event(event=test_event, active_test_procedure=active_test_procedure)

    # Assert
    assert matched_listener == listeners[matching_listener_index]


@pytest.mark.parametrize(
    "test_event,listeners",
    [
        (
            Event(type="GET-request-received", parameters={"endpoint": "/dcap"}),
            [
                Listener(
                    step="step",
                    event=Event(type="GET-request-received", parameters={"endpoint": "/dcap"}),
                    actions=[],
                    enabled=True,
                )
            ],
        ),  # no actions for listener
        (
            Event(type="GET-request-received", parameters={"endpoint": "/dcap"}),
            [
                Listener(
                    step="step",
                    event=Event(type="GET-request-received", parameters={"endpoint": "/dcap"}),
                    actions=[Action(type="enable-listeners", parameters={})],
                    enabled=True,
                )
            ],
        ),  # 1 action for listener
        (
            Event(type="GET-request-received", parameters={"endpoint": "/dcap"}),
            [
                Listener(
                    step="step",
                    event=Event(type="GET-request-received", parameters={"endpoint": "/dcap"}),
                    actions=[
                        Action(type="enable-listeners", parameters={}),
                        Action(type="remove-listeners", parameters={}),
                    ],
                    enabled=True,
                )
            ],
        ),  # 2 actions for listener
    ],
)
def test_handle_event_calls__apply_action_for_each_listener_action(
    mocker, test_event: Event, listeners: list[Listener]
):
    # Arrange
    active_test_procedure = MagicMock()
    active_test_procedure.listeners = listeners

    mock__apply_action = mocker.patch("cactus_runner.app.event._apply_action")

    # Act
    matched_listener = event.handle_event(event=test_event, active_test_procedure=active_test_procedure)

    # Assert
    assert mock__apply_action.call_count == len(matched_listener.actions)


@pytest.mark.parametrize(
    "test_event,listeners",
    [
        (
            Event(type="GET-request-received", parameters={"endpoint": "/dcap"}),
            [
                Listener(
                    step="step",
                    event=Event(type="GET-request-received", parameters={"endpoint": "/dcap"}),
                    actions=[],
                    enabled=False,
                )
            ],
        ),  # Events match but the listener is disabled
        (
            Event(type="GET-request-received", parameters={"endpoint": "/dcap"}),
            [
                Listener(
                    step="step",
                    event=Event(type="POST-request-received", parameters={"endpoint": "/dcap"}),
                    actions=[],
                    enabled=True,
                )
            ],
        ),  # Parameters match but event types differ
        (
            Event(type="POST-request-received", parameters={"endpoint": "/mup"}),
            [
                Listener(
                    step="step",
                    event=Event(type="POST-request-received", parameters={"endpoint": "/edev"}),
                    actions=[],
                    enabled=True,
                )
            ],
        ),  # Event types match but parameters differ
    ],
)
def test_handle_event_with_no_matches(test_event: Event, listeners: list[Listener]):
    # Arrange
    active_test_procedure = MagicMock()
    active_test_procedure.listeners = listeners

    # Act
    listener = event.handle_event(event=test_event, active_test_procedure=active_test_procedure)

    # Assert
    assert listener is None
