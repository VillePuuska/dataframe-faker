import datetime
from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(kw_only=True)
class Constraint:
    null_chance: float = 0.0
    allowed_values: list[Any] | None = None


##################
### Numeric types
##################


@dataclass(kw_only=True)
class ByteConstraint(Constraint):
    min_value: int = 0
    max_value: int = 100
    allowed_values: list[int] | None = None


# short


@dataclass(kw_only=True)
class IntegerConstraint(Constraint):
    min_value: int = 0
    max_value: int = 100
    allowed_values: list[int] | None = None


# long


@dataclass(kw_only=True)
class FloatConstraint(Constraint):
    min_value: float = 0.0
    max_value: float = 100.0
    allowed_values: list[float] | None = None


# double

# decimal


##################
### String types
##################


@dataclass(kw_only=True)
class StringConstraint(Constraint):
    string_type: Literal[
        "uuid4",
        "name",
        "first_name",
        "last_name",
        "phone_number",
        "address",
        "email",
        "any",
    ] = "any"
    min_length: int = 0
    max_length: int = 16
    allowed_values: list[str] | None = None


# varchar

# char

##################
### Binary type
##################

# binary

##################
### Boolean type
##################


@dataclass(kw_only=True)
class BooleanConstraint(Constraint):
    true_chance: float = 0.5
    allowed_values: list[bool] | None = None


##################
### Datetime types
##################


@dataclass(kw_only=True)
class DateConstraint(Constraint):
    min_value: datetime.date = datetime.date(year=2020, month=1, day=1)
    max_value: datetime.date = datetime.date(year=2024, month=12, day=31)
    allowed_values: list[datetime.date] | None = None


@dataclass(kw_only=True)
class TimestampConstraint(Constraint):
    min_value: datetime.datetime = datetime.datetime(year=2020, month=1, day=1)
    max_value: datetime.datetime = datetime.datetime(year=2024, month=12, day=31)
    tzinfo: datetime.tzinfo | None = None
    allowed_values: list[datetime.datetime] | None = None


# timestampntz

##################
### Interval types
##################

# yearmonthinterval

# daytimeinterval

##################
### Complex types
##################


@dataclass(kw_only=True)
class ArrayConstraint(Constraint):
    element_constraint: Constraint | None = None
    min_length: int = 0
    max_length: int = 5
    allowed_values: list[list[Any]] | None = None


# map


@dataclass(kw_only=True)
class StructConstraint(Constraint):
    element_constraints: dict[str, Constraint | None] = field(default_factory=dict)
    allowed_values: list[dict[str, Any]] | None = None
