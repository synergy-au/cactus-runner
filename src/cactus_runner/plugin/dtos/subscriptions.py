from dataclasses import dataclass

from envoy.server.model import SubscriptionResource

__all__ = ["Subscription", "SubscriptionHref", "TransmitNotificationLog"]


@dataclass(slots=True, frozen=True)
class Subscription:
    subscription_id: str
    scoped_site_id: str | None  # Scoped site if any
    client_aggregator_id: str | None  # Client id or similar to facilitate appropriate subscription querying.
    # Envoy utilises aggregator only for subscription model.
    resource_type: SubscriptionResource  # What resource type is being subscribed to
    resource_id: (
        str | None
    )  # Represents the ID of a single resource being subscribed or if NULL all resources for the resource type
    resource_parent_id: (
        str | None
    )  # Like resource_id but only for subscriptions with a multi part ID - this represents an ID of a parent list
    notification_uri: str  # remote URI where notifications will be sent


@dataclass(slots=True, frozen=True)
class SubscriptionHref:
    """Represents the various parts of a sep2 Subscription to a specific Resource (eg a DERControl list under
    DERProgram X) derived soley from a subscribed Resource's href.

    It's up to the plugin to interpret these parts into an unambiguous reference to a specific Resource or List."""

    resource_type: SubscriptionResource
    scoped_site_id: str | None  # If set - this subscription is scoped to this specific site_id
    resource_id: (
        str | None
    )  # Represents the ID of a single resource being subscribed or if NULL, the list of all available resources
    resource_parent_id: (
        str | None
    )  #  Like resource_id but only for subscriptions with a multi part ID - this represents an ID of a parent list


@dataclass(slots=True, frozen=True)
class TransmitNotificationLog:
    subscription_id: str  # Corresponding subscription
    http_status_code: int  # 4xx, 2xx etc
