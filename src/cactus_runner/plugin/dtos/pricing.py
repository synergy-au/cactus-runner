from dataclasses import dataclass
from datetime import datetime

from envoy_schema.server.schema.sep2.types import (
    AccumulationBehaviourType,
    CommodityType,
    CurrencyCode,
    DataQualifierType,
    FlowDirectionType,
    KindType,
    PhaseCode,
    RoleFlagsType,
    UomType,
)


@dataclass(slots=True, frozen=True)
class TariffWrite:
    """Basic attributes for the creation of a new tariff."""

    name: str
    dnsp_code: str
    currency_code: CurrencyCode
    price_power_of_ten_multiplier: int
    primacy: int
    fsa_id: int = 1  # The function set assignment ID that this Tariff will be grouped under
    required_site_group_id: int | None = (
        None  # If set - only sites in this SiteGroup will "see" this Tariff. Globally visible otherwise
    )


@dataclass(slots=True, frozen=True)
class Tariff:
    """Response model for Tariff including id and modification time."""

    tariff_id: str

    name: str
    dnsp_code: str
    currency_code: CurrencyCode
    price_power_of_ten_multiplier: int
    primacy: int

    fsa_id: str
    required_site_group_id: int | None

    created_time: datetime
    changed_time: datetime


@dataclass(slots=True, frozen=True)
class TariffComponentWrite:
    """Basic attributes for the creation of a new tariff component that sits underneath a specific Tariff"""

    tariff_id: str

    role_flags: RoleFlagsType
    description: str | None = None

    # ReadingType fields
    accumulation_behaviour: AccumulationBehaviourType | None = None
    commodity: CommodityType | None = None
    data_qualifier: DataQualifierType | None = None
    flow_direction: FlowDirectionType | None = None
    kind: KindType | None = None
    phase: PhaseCode | None = None
    power_of_ten_multiplier: int | None = None
    uom: UomType | None = None


@dataclass(slots=True, frozen=True)
class TariffComponent:
    """Basic attributes for the creation of a new tariff component that sits underneath a specific Tariff"""

    tariff_component_id: str

    tariff_id: str

    role_flags: RoleFlagsType
    description: str | None

    created_time: datetime
    changed_time: datetime

    # ReadingType fields
    accumulation_behaviour: AccumulationBehaviourType | None
    commodity: CommodityType | None
    data_qualifier: DataQualifierType | None
    flow_direction: FlowDirectionType | None
    kind: KindType | None
    phase: PhaseCode | None
    power_of_ten_multiplier: int | None
    uom: UomType | None


@dataclass(slots=True, frozen=True)
class TariffGeneratedRateWrite:
    """Time of use tariff pricing - represents a price for a specific site group for a specific range of time as defined
    by the parent TariffComponent."""

    tariff_component_id: str  # The TariffComponent ID that this price entry sits underneath
    site_group_id: str  # The SiteGroup id whose members will have this price available to them
    calculation_log_id: str | None  # The ID of the CalculationLog that created this rate (or NULL if no link)
    start_time: datetime
    duration_seconds: int
    price_pow10_encoded: int  # Price encoded as per parent Tariff.price_power_of_ten_multiplier
    block_1_start_pow10_encoded: int | None = None  # This much consumption of TariffComponent triggers a new price
    price_pow10_encoded_block_1: int | None = None  # Price used after price_pow10_encoded_block_1 consumption


@dataclass(slots=True, frozen=True)
class TariffGeneratedRate:
    """Time of use tariff pricing - represents a price for a specific site group for a specific range of time as defined
    by the parent TariffComponent."""

    tariff_generated_rate_id: str
    tariff_id: str
    tariff_component_id: str  # The TariffComponent ID that this price entry sits underneath
    site_group_id: str  # The SiteGroup id whose members will have this price available to them
    calculation_log_id: str | None  # The ID of the CalculationLog that created this rate (or NULL if no link)
    start_time: datetime
    duration_seconds: int
    price_pow10_encoded: int  # Price encoded as per parent Tariff.price_power_of_ten_multiplier
    block_1_start_pow10_encoded: int | None
    price_pow10_encoded_block_1: int | None

    created_time: datetime
    changed_time: datetime

    deleted_time: datetime | None
    archive_time: datetime | None
