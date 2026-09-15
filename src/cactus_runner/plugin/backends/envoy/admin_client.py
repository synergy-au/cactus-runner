"""envoy integration
- admin client
"""

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from http import HTTPStatus

from aiohttp import BasicAuth, ClientResponse, ClientSession, ClientTimeout, TCPConnector
from aiohttp.typedefs import StrOrURL
from envoy_schema.admin.schema.aggregator import AggregatorPageResponse
from envoy_schema.admin.schema.base import BatchCreateResponse
from envoy_schema.admin.schema.config import (
    RuntimeServerConfigRequest,
    RuntimeServerConfigResponse,
)
from envoy_schema.admin.schema.pricing import (
    TariffComponentRequest,
    TariffComponentResponse,
    TariffGeneratedRatePageResponse,
    TariffGeneratedRateRequest,
    TariffGeneratedRateResponse,
    TariffPageResponse,
    TariffRequest,
    TariffResponse,
)
from envoy_schema.admin.schema.site import SiteResponse, SiteUpdateRequest
from envoy_schema.admin.schema.site_control import (
    SiteControlGroupDefaultRequest,
    SiteControlGroupDefaultResponse,
    SiteControlGroupPageResponse,
    SiteControlGroupRequest,
    SiteControlGroupResponse,
    SiteControlPageResponse,
    SiteControlRequest,
    SiteControlResponse,
)
from envoy_schema.admin.schema.site_group import SiteGroupAssignmentRequest, SiteGroupRequest, SiteGroupResponse
from envoy_schema.admin.schema.uri import (
    AggregatorListUri,
    ServerConfigRuntimeUri,
    SiteControlGroupDefaultUri,
    SiteControlGroupListUri,
    SiteControlGroupUri,
    SiteControlRangeUri,
    SiteControlUri,
    SiteGroupAssignmentsListUri,
    SiteGroupListUri,
    SiteGroupUri,
    SiteUri,
    TariffComponentCreateUri,
    TariffComponentListUri,
    TariffComponentUpdateUri,
    TariffGeneratedRateCreateUri,
    TariffGeneratedRateListUri,
    TariffGeneratedRateUpdateUri,
    TariffListUri,
)

from cactus_runner.app.envoy_common import EnvoyConfigurationError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class SecretString:
    def __init__(self, secret: str) -> None:
        self._secret = secret

    def __str__(self) -> str:
        return "REDACTED"

    def __repr__(self) -> str:
        return "SecretString(REDACTED)"

    def reveal(self) -> str:
        """Explicitly return"""
        return self._secret


@dataclass(frozen=True)
class EnvoyAdminClientAuthParams:
    username: str  # admin username
    password: str  # admin password


class EnvoyAdminClient:
    """
    Client for interacting with the Envoy Admin API.

    This class is designed to be used as a dependency that gets injected at application startup.
    It internally manages the lifecycle of an aiohttp.ClientSession and expects a call close_session() during
    application cleanup to ensure proper session teardown.
    """

    def __init__(self, base_url: StrOrURL, auth_params: EnvoyAdminClientAuthParams, timeout: int = 30) -> None:
        self._base_url = base_url
        self._timeout = ClientTimeout(total=timeout)
        self._session: ClientSession = ClientSession(
            base_url=self._base_url,
            timeout=self._timeout,
            connector=TCPConnector(limit=10),
            auth=BasicAuth(login=auth_params.username, password=auth_params.password),
        )

    async def close_session(self) -> None:
        await self._session.close()

    async def paginate_list[ItemT](
        self,
        uri: str,
        handle_page: Callable[[ClientResponse], Awaitable[list[ItemT]]],
        filter_params: dict[str, str | int] | None = None,
    ) -> list[ItemT]:
        all_items: list[ItemT] = []
        current_start = 0
        page_limit = 100
        filter_params = filter_params or {}

        while True:
            async with self._session.get(
                uri,
                params={"start": current_start, "limit": page_limit} | filter_params,
            ) as resp:
                resp.raise_for_status()
                new_items = await handle_page(resp)

            if new_items:
                all_items.extend(new_items)

            # Check if we've retrieved all items (i.e. we couldn't get enough to fill a page)
            if len(new_items) < page_limit:
                break

            current_start += page_limit

        return all_items

    async def get_aggregators(self) -> AggregatorPageResponse:
        async with self._session.get(AggregatorListUri) as resp:
            resp.raise_for_status()
            json = await resp.json()
            return AggregatorPageResponse(**json)

    async def get_single_site(self, site_id: int) -> SiteResponse:
        async with self._session.get(SiteUri.format(site_id=site_id)) as resp:
            resp.raise_for_status()
            json = await resp.json()
            return SiteResponse(**json)

    async def delete_single_site(self, site_id: int) -> HTTPStatus:
        resp = await self._session.delete(SiteUri.format(site_id=site_id))
        resp.raise_for_status()
        return HTTPStatus(resp.status)

    async def update_single_site(self, site_id: int, update_request: SiteUpdateRequest) -> HTTPStatus:
        resp = await self._session.post(SiteUri.format(site_id=site_id), json=update_request.model_dump())
        resp.raise_for_status()
        return HTTPStatus(resp.status)

    async def post_site_control_group(self, site_control_group: SiteControlGroupRequest) -> int:
        resp = await self._session.post(SiteControlGroupListUri, json=site_control_group.model_dump())
        resp.raise_for_status()
        href = resp.headers["Location"]
        return int(href.split("/")[-1])

    async def put_site_control_group(
        self, group_id: int, site_control_group: SiteControlGroupRequest
    ) -> SiteControlGroupResponse:
        resp = await self._session.put(
            SiteControlGroupUri.format(group_id=group_id), json=site_control_group.model_dump()
        )
        resp.raise_for_status()
        json = await resp.json()
        return SiteControlGroupResponse(**json)

    async def post_site_control_default(
        self, group_id: int, control_default: SiteControlGroupDefaultRequest
    ) -> HTTPStatus:
        resp = await self._session.post(
            SiteControlGroupDefaultUri.format(group_id=group_id),
            data=control_default.model_dump_json(),
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        return HTTPStatus(resp.status)

    async def get_site_control_group(self, group_id: int) -> SiteControlGroupResponse:
        async with self._session.get(SiteControlGroupUri.format(group_id=group_id)) as resp:
            resp.raise_for_status()
            json = await resp.json()
            return SiteControlGroupResponse(**json)

    async def get_all_site_control_groups(
        self, start: int = 0, limit: int = 100, after: datetime | None = None
    ) -> SiteControlGroupPageResponse:
        async with self._session.get(
            SiteControlGroupListUri,
            params={"start": start, "limit": limit} | {"after": after.isoformat()} if after else {},
        ) as resp:
            resp.raise_for_status()
            json = await resp.json()
            return SiteControlGroupPageResponse(**json)

    async def create_site_controls(self, group_id: int, control_list: list[SiteControlRequest]) -> list[int]:
        """Creates the specified controls - returns the ID's of the created site_control (will correspond 1-1 with
        control_list)"""
        resp = await self._session.post(
            SiteControlUri.format(group_id=group_id),
            data="[" + ",".join([site_control.model_dump_json() for site_control in control_list]) + "]",
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        json = await resp.json()
        return BatchCreateResponse(**json).ids

    async def get_all_site_controls(
        self,
        group_id: int,
        after: datetime | None = None,
    ) -> list[SiteControlResponse]:
        """Fetch all site controls for a group, handling pagination automatically."""

        async def _get_items(resp: ClientResponse) -> list[SiteControlResponse]:
            json = await resp.json()
            return SiteControlPageResponse(**json).controls

        return await self.paginate_list(
            SiteControlUri.format(group_id=group_id),
            _get_items,
            filter_params={"after": after.isoformat()} if after else None,
        )

    async def delete_site_controls_in_range(
        self, group_id: int, period_start: datetime, period_end: datetime
    ) -> HTTPStatus:
        resp = await self._session.delete(
            SiteControlRangeUri.format(group_id=group_id, period_start=period_start, period_end=period_end),
        )
        resp.raise_for_status()
        return HTTPStatus(resp.status)

    async def update_runtime_config(self, config: RuntimeServerConfigRequest) -> HTTPStatus:
        resp = await self._session.post(ServerConfigRuntimeUri, json=config.model_dump())
        resp.raise_for_status()
        return HTTPStatus(resp.status)

    async def get_runtime_config(self) -> RuntimeServerConfigResponse:
        async with self._session.get(
            ServerConfigRuntimeUri,
        ) as resp:
            resp.raise_for_status()
            json = await resp.json()
            return RuntimeServerConfigResponse(**json)

    async def get_site_control_default(self, group_id: int) -> SiteControlGroupDefaultResponse:
        async with self._session.get(
            SiteControlGroupDefaultUri.format(group_id=group_id),
        ) as resp:
            resp.raise_for_status()
            json = await resp.json()
            return SiteControlGroupDefaultResponse(**json)

    async def delete_all_site_control_groups(self) -> HTTPStatus:
        """
        Deletes all site control groups and downstream values with proper notifications.
        Used for partial database reset between playlist tests.

        Archives: site control groups (DERPrograms) DOEs (DERControls/Site controls), DefaultDERControls, and FSA's.
        Notifications are sent for these actions.
        """
        resp = await self._session.delete(SiteControlGroupListUri)
        resp.raise_for_status()
        return HTTPStatus(resp.status)

    async def get_site_group(self, group_name: str) -> SiteGroupResponse | None:
        """Fetches the SiteGroup with the specified group_name - returns None if it DNE"""
        async with self._session.get(SiteGroupUri.format(group_name=group_name)) as resp:
            if resp.status == HTTPStatus.NOT_FOUND:
                return None
            resp.raise_for_status()
            json = await resp.json()
            return SiteGroupResponse(**json)

    async def try_create_site_group(self, group_name: str, default_group: bool) -> str | None:
        """Tries to create a site group with the specified group_name - returns the SiteGroup href on success.

        Can return None if the site group already exists - raises on other kinds of HTTP errors"""
        body = SiteGroupRequest(name=group_name, default_group=default_group)

        resp = await self._session.post(SiteGroupListUri, json=body.model_dump())
        if resp.status == HTTPStatus.BAD_REQUEST:
            # Already exists is returned as a BadRequest
            return None

        resp.raise_for_status()
        return resp.headers["Location"]

    async def try_create_site_group_assignment(self, group_name: str, site_id: int) -> None:
        """Tries to create a site group assignment from site to the specified group with group_name.

        If the site is already assigned to the SiteGroup with group_name - this has no effect. raises an error if the
        SiteGroup is missing or on other HTTP/connection errors."""
        body = SiteGroupAssignmentRequest(site_id=site_id)
        resp = await self._session.post(
            SiteGroupAssignmentsListUri.format(group_name=group_name), json=body.model_dump()
        )
        if resp.status == HTTPStatus.BAD_REQUEST:
            # Already exists is returned as a BadRequest
            return

        resp.raise_for_status()

    async def get_all_tariffs(
        self,
        group_name: str | None = None,
    ) -> list[TariffResponse]:
        """Fetch all tariffs (optionally for a group with specified name), handling pagination automatically."""

        async def _get_items(resp: ClientResponse) -> list[TariffResponse]:
            json = await resp.json()
            return TariffPageResponse(**json).tariffs

        return await self.paginate_list(
            TariffListUri,
            _get_items,
            filter_params={"group": group_name} if group_name is not None else None,
        )

    async def create_tariff(self, tariff: TariffRequest) -> int:
        resp = await self._session.post(TariffListUri, json=tariff.model_dump())
        resp.raise_for_status()

        json = await resp.json()
        return BatchCreateResponse(**json).ids[0]

    async def get_all_tariff_components(
        self,
        tariff_id: int,
    ) -> list[TariffComponentResponse]:
        """Fetch all tariff components for a specific Tariff, handling pagination automatically."""

        async def _get_items(resp: ClientResponse) -> list[TariffComponentResponse]:
            json = await resp.json()
            return [TariffComponentResponse.model_validate(item) for item in json]

        return await self.paginate_list(TariffComponentListUri.format(tariff_id=tariff_id), _get_items)

    async def create_tariff_component(self, tariff_component: TariffComponentRequest) -> int:
        resp = await self._session.post(TariffComponentCreateUri, json=tariff_component.model_dump())
        resp.raise_for_status()

        json = await resp.json()
        return BatchCreateResponse(**json).ids[0]

    async def get_all_tariff_generated_rates(
        self,
        tariff_component_id: int,
        group_name: str | None = None,
        start_time_since: datetime | None = None,
        start_time_until: datetime | None = None,
        site_id: int | None = None,
    ) -> list[TariffGeneratedRateResponse]:
        """Fetch all tariff rates for a specific TariffComponent (with filters), handling pagination automatically."""

        async def _get_items(resp: ClientResponse) -> list[TariffGeneratedRateResponse]:
            json = await resp.json()
            return TariffGeneratedRatePageResponse(**json).rates

        filters: dict[str, str | int] = {}
        if group_name is not None:
            filters["group"] = group_name
        if start_time_since is not None:
            filters["start_time_since"] = start_time_since.isoformat()
        if start_time_until is not None:
            filters["start_time_until"] = start_time_until.isoformat()
        if site_id is not None:
            filters["site_id"] = site_id

        return await self.paginate_list(
            TariffGeneratedRateListUri.format(tariff_component_id=tariff_component_id),
            _get_items,
            filter_params=filters,
        )

    async def create_tariff_generated_rate(self, rate: TariffGeneratedRateRequest) -> int:
        resp = await self._session.post(
            TariffGeneratedRateCreateUri,
            data="[" + rate.model_dump_json() + "]",
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()

        json = await resp.json()
        return BatchCreateResponse(**json).ids[0]

    async def delete_tariff_component(self, tariff_component_id: int) -> None:
        resp = await self._session.delete(
            TariffComponentUpdateUri.format(tariff_component_id=tariff_component_id),
        )
        resp.raise_for_status()

    async def delete_tariff_generated_rate(self, tariff_generated_rate_id: int) -> None:
        resp = await self._session.delete(
            TariffGeneratedRateUpdateUri.format(tariff_generated_rate_id=tariff_generated_rate_id),
        )
        resp.raise_for_status()


async def get_exclusive_site_group(client: EnvoyAdminClient, site_id: int) -> SiteGroupResponse:
    """Gets the SiteGroup which site has exclusive access to - that is, anything added to the returned SiteGroup will
    ONLY be visible to site (no other sites will have membership).

    This method will create SiteGroup if none exists via the admin client"""
    exclusive_site_name = f"exclusive_site_{site_id}"

    # There is a unique constraint underneath this - we should be safe from a race condition perspective
    created_site_group_href = await client.try_create_site_group(group_name=exclusive_site_name, default_group=False)
    if created_site_group_href is not None:
        # If the creation succeeded - we will need to add assignments from site to it
        await client.try_create_site_group_assignment(group_name=exclusive_site_name, site_id=site_id)

    site_group = await client.get_site_group(group_name=exclusive_site_name)
    if site_group is None:
        raise EnvoyConfigurationError(
            f"Couldn't find SiteGroup with name '{exclusive_site_name}' - this is likely a bug with envoy admin API"
        )
    return site_group
