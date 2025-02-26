from pathlib import Path

import yaml
from aiohttp import client, web

SERVER_URL = "http://localhost:8000"
MOUNT_POINT = "/"


async def handle_all_request_types(request):
    proxy_path = request.match_info.get("proxyPath", "No proxyPath placeholder defined")
    local_path = request.rel_url.path_qs
    remote_url = SERVER_URL + local_path

    print(f"{proxy_path=} {local_path=} {remote_url=}")

    # Forward the request to the reference server
    async with client.request(
        request.method, remote_url, headers=request.headers.copy(), allow_redirects=False, data=await request.read()
    ) as response:
        headers = response.headers.copy()
        body = await response.read()
        return web.Response(headers=headers, status=response.status, body=body)


def create_application():
    app = web.Application()

    # Add catch-all route for proxying all other requests to CSIP-AUS reference server
    app.router.add_route("*", MOUNT_POINT + "{proxyPath:.*}", handle_all_request_types)

    return app


def read_test_procedure_definitions(path: Path) -> dict:
    with path.open() as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    test_procedures: dict = read_test_procedure_definitions(path=Path("config/test_procedure.yaml"))

    app = create_application()
    web.run_app(app, port=8080)
