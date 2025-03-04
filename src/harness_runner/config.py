from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dataclass_wizard import YAMLWizard


class TestProcedureDefinitionError(Exception):
    __test__ = False  # Prevent pytest from picking up this class


@dataclass
class Event:
    type: str
    parameters: dict


@dataclass
class Action:
    type: str
    parameters: dict[str, Any]


@dataclass
class Step:
    listener_enabled: bool
    event: Event
    actions: list[Action]


@dataclass
class Preconditions:
    db: str


@dataclass
class TestProcedure:
    description: str
    category: str
    classes: list[str]
    preconditions: Preconditions
    steps: dict[str, Step]


@dataclass
class TestProcedures(YAMLWizard):
    """Represents a collection of CSIP-AUS test procedure descriptions/specifications

    By sub-classing the YAMLWizard mixin, we get access to the class method `from_yaml`
    which we can use to create an instances of `TestProcedures`.
    """

    description: str
    version: str
    test_procedures: dict[str, TestProcedure]

    def _validate_actions(self):
        """Validate actions of test procedure steps

        Ensure,
        - action has the correct parameters
        - if parameters refer to steps then those steps are defined for the test procedure
        """

        for test_procedure_name, test_procedure in self.test_procedures.items():
            step_names = test_procedure.steps.keys()
            for step in test_procedure.steps.values():
                for action in step.actions:
                    match action.type:
                        case "enable-listeners" | "remove-listeners":
                            try:
                                listeners = action.parameters["listeners"]
                            except KeyError:
                                raise TestProcedureDefinitionError(
                                    f"[{test_procedure_name}] Action '{action.type}' missing parameters 'listeners'."
                                )

                            for listener_step_name in listeners:
                                if listener_step_name not in step_names:
                                    raise TestProcedureDefinitionError(
                                        f"[{test_procedure_name}] Action '{action.type}' refers to unknown step '{listener_step_name}'."
                                    )

    def validate(self):
        self._validate_actions()


class TestProcedureConfig:
    @staticmethod
    def from_yamlfile(path: Path) -> TestProcedures:
        with open(path, "r") as f:
            test_procedures: TestProcedures = TestProcedures.from_yaml(f.read())  # type: ignore

        test_procedures.validate()

        return test_procedures
