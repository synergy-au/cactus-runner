import logging
from enum import StrEnum

import aiohttp

logger = logging.getLogger(__name__)


class RunnerClientException(Exception): ...


class CsipAusTestProcedureCodes(StrEnum):
    ALL01 = "ALL-01"


# TODO: logging, retries, exception handling for all requests
class RunnerClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.timeout = aiohttp.ClientTimeout(total=30)

    async def start_test_procedure(self, test_code: CsipAusTestProcedureCodes, aggregator_certificate: str) -> None:
        ENDPOINT = "/start"
        params = {"test": test_code.value, "certificate": aggregator_certificate}
        try:
            async with aiohttp.ClientSession(base_url=self.base_url, timeout=self.timeout) as session:
                await session.post(url=ENDPOINT, params=params)
        except aiohttp.ConnectionTimeoutError as e:
            logger.debug(e)
            raise RunnerClientException("Unexpected failure while starting test.")

    async def finalize_test_procedure(self) -> str:
        ENDPOINT = "/finalize"
        try:
            async with aiohttp.ClientSession(base_url=self.base_url, timeout=self.timeout) as session:
                async with session.post(url=ENDPOINT) as response:
                    return await response.text()
        except aiohttp.ConnectionTimeoutError as e:
            logger.debug(e)
            raise RunnerClientException("Unexpected failure while finalizing test procedure.")

    async def get_runner_capability(self):
        ENDPOINT = "/capability"
        try:
            async with aiohttp.ClientSession(base_url=self.base_url, timeout=self.timeout) as session:
                async with session.get(url=ENDPOINT) as response:
                    return await response.text()
        except aiohttp.ConnectionTimeoutError as e:
            logger.debug(e)
            raise RunnerClientException("Unexpected failure while requesting runner capabilities.")

    async def get_status(self):
        ENDPOINT = "/status"
        try:
            async with aiohttp.ClientSession(base_url=self.base_url, timeout=self.timeout) as session:
                async with session.get(url=ENDPOINT) as response:
                    return await response.text()
        except aiohttp.ConnectionTimeoutError as e:
            logger.debug(e)
            raise RunnerClientException("Unexpected failure while requesting test procedure status.")
