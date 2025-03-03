from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dataclass_wizard import YAMLWizard


@dataclass
class RawEvent:
    type: str
    parameters: dict


@dataclass
class Action:
    action: str
    parameters: dict[str, Any]


@dataclass
class Step:
    event: RawEvent
    actions: list[Action]


@dataclass
class Preconditions:
    db: str
    runner: dict


@dataclass
class TestProcedure:
    description: str
    category: str
    classes: list[str]
    preconditions: Preconditions
    steps: dict[str, Step]


@dataclass
class TestProcedures(YAMLWizard):
    description: str
    version: str
    test_procedures: dict[str, TestProcedure]


class TestProcedureConfig:
    @staticmethod
    def from_yamlfile(path: Path) -> TestProcedures:
        with open(path, "r") as f:
            return TestProcedures.from_yaml(f.read())
