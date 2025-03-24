import asyncio

from cactus_test_definitions import TestProcedureId

from cactus_runner.client import (
    ClientSession,
    ClientTimeout,
    RunnerClient,
)
from cactus_runner.models import (
    ActiveTestProcedureStatus,
    LastProxiedRequest,
    RunnerCapabilities,
)


async def main():
    timeout = ClientTimeout(total=30)
    base_url = "http://localhost:8080/"
    async with ClientSession(base_url=base_url, timeout=timeout) as session:
        status: ActiveTestProcedureStatus = await RunnerClient.status(session=session)
        print(status)

        capabilities: RunnerCapabilities = await RunnerClient.capabilities(session=session)
        print(capabilities)

        last_request: LastProxiedRequest = await RunnerClient.last_request(session=session)
        print(last_request)

        test_id = TestProcedureId.ALL_01
        # await RunnerClient.start(session=session, test_id=test_id, aggregator_certificate=None)
        # await RunnerClient.finalize(session=session)


if __name__ == "__main__":
    asyncio.run(main())
