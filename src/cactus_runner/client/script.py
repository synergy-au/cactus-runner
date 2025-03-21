import asyncio

import aiohttp

from cactus_runner.client import RunnerClient


async def main():
    timeout = aiohttp.ClientTimeout(total=30)
    base_url = "http://localhost:8080/"
    async with aiohttp.ClientSession(base_url=base_url, timeout=timeout) as session:
        print(await RunnerClient.status(session=session))
        print(await RunnerClient.capabilities(session=session))
        print(await RunnerClient.last_request(session=session))


if __name__ == "__main__":
    asyncio.run(main())
