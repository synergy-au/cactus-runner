import logging
from enum import StrEnum

from aiohttp import ClientSession, ConnectionTimeoutError

from cactus_runner.app.runner import (
    ActiveTestProcedureStatus,
    RunnerCapabilities,
)

logger = logging.getLogger(__name__)


class RunnerClientException(Exception): ...


class CsipAusTestProcedureCodes(StrEnum):
    ALL01 = "ALL-01"


class RunnerClient:
    @staticmethod
    async def start(session: ClientSession, test_id: TestProcedureId, aggregator_certificate: str) -> None:
        try:
            await session.post(url="/start", params={"test": test_id.value, "certificate": aggregator_certificate})
        except ConnectionTimeoutError as e:
            logger.debug(e)
            raise RunnerClientException("Unexpected failure while starting test.")

    @staticmethod
    async def finalize(session: ClientSession) -> str:
        try:
            async with session.post(url="/finalize") as response:
                return await response.text()
        except ConnectionTimeoutError as e:
            logger.debug(e)
            raise RunnerClientException("Unexpected failure while finalizing test procedure.")

    @staticmethod
    async def capabilities(session: ClientSession):
        try:
            async with session.get(url="/capability") as response:
                json = await response.text()
                return RunnerCapabilities.from_json(json)
        except ConnectionTimeoutError as e:
            logger.debug(e)
            raise RunnerClientException("Unexpected failure while requesting runner capabilities.")

    @staticmethod
    async def status(session: ClientSession):
        try:
            async with session.get(url="/status") as response:
                json = await response.text()
                return ActiveTestProcedureStatus.from_json(json)
        except ConnectionTimeoutError as e:
            logger.debug(e)
            raise RunnerClientException("Unexpected failure while requesting test procedure status.")
