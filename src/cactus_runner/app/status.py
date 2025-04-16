from cactus_runner.models import (
    ActiveTestProcedure,
    ClientInteraction,
    RequestEntry,
    RunnerStatus,
    StepStatus,
)


def get_runner_status_summary(step_status: dict[str, StepStatus]):
    """Returns"""
    completed_steps = sum(s == StepStatus.RESOLVED for s in step_status.values())
    steps = len(step_status)
    return f"{completed_steps}/{steps} steps complete."


def get_active_runner_status(
    active_test_procedure: ActiveTestProcedure,
    request_history: list[RequestEntry],
    last_client_interaction: ClientInteraction,
) -> RunnerStatus:

    step_status = active_test_procedure.step_status

    return RunnerStatus(
        test_procedure_name=active_test_procedure.name,
        last_client_interaction=last_client_interaction,
        status_summary=get_runner_status_summary(step_status=step_status),
        step_status=step_status,
        request_history=request_history,
    )


def get_runner_status(last_client_interaction: ClientInteraction) -> RunnerStatus:
    return RunnerStatus(status_summary="No test procedure running", last_client_interaction=last_client_interaction)
