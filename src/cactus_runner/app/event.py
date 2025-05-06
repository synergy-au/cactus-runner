import logging

from cactus_test_definitions import (
    Action,
    Event,
)

from cactus_runner.models import (
    ActiveTestProcedure,
    Listener,
)

logger = logging.getLogger(__name__)


class UnknownActionError(Exception):
    """Unknown Cactus Runner Action"""


def _apply_action(action: Action, active_test_procedure: ActiveTestProcedure):
    """Applies the action to the active test procedure.

    Actions describe operations such as activate or disabling listeners.

    Args:
        action (Action): The Action to apply to the active test procedure.
        active_test_procedure (ActiveTestProcedure): The currently active test procedure.

    Raises:
        UnknownActionError: Raised if this function has no implementation for the provided `action.type`.
    """
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
    """Triggers the action associated with any enabled listeners that match then event.

    Args:
        event (Event): An Event to be matched against the test procedures enabled listeners.
        active_test_procedure (ActiveTestProcedure): The currently active test procedure.

    Returns:
        Listener: If successful return the listener that matched the event.
        None: If no listener matched.
    """
    # Check all listeners
    for listener in active_test_procedure.listeners:
        # Did any of the current listeners match?
        if listener.enabled and listener.event == event:
            logger.info(f"Event matched: {event=}")

            # Perform actions associated with event
            for action in listener.actions:
                logger.info(f"Executing action: {action=}")
                _apply_action(action=action, active_test_procedure=active_test_procedure)

            return listener

    return None
