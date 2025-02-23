import datetime
from typing import Any, overload

from faker import Faker
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DateType,
    FloatType,
    IntegerType,
    StringType,
    StructType,
    TimestampType,
)

from .constraints import (
    ArrayConstraint,
    Constraint,
    DateConstraint,
    FloatConstraint,
    IntegerConstraint,
    StringConstraint,
    StructConstraint,
    TimestampConstraint,
)


def generate_fake_dataframe(
    schema: str | StructType,
    constraints: dict[str, Constraint | None],
    spark: SparkSession,
    rows: int = 100,
    faker: Faker | None = None,
) -> DataFrame:
    if isinstance(schema, str):
        schema = convert_schema_string_to_schema(schema=schema, spark=spark)

    raise NotImplementedError


def convert_schema_string_to_schema(schema: str, spark: SparkSession) -> StructType:
    return spark.createDataFrame([], schema=schema).schema


def generate_fake_row(schema: StructType, faker: Faker) -> dict[str, Any]:
    raise NotImplementedError


@overload
def generate_fake_value(
    dtype: StructType,
    faker: Faker,
    constraint: Constraint | None = None,
) -> dict[str, Any]: ...


@overload
def generate_fake_value(
    dtype: StringType,
    faker: Faker,
    constraint: Constraint | None = None,
) -> str: ...


@overload
def generate_fake_value(
    dtype: IntegerType,
    faker: Faker,
    constraint: Constraint | None = None,
) -> int: ...


@overload
def generate_fake_value(
    dtype: FloatType,
    faker: Faker,
    constraint: Constraint | None = None,
) -> float: ...


@overload
def generate_fake_value(
    dtype: ArrayType,
    faker: Faker,
    constraint: Constraint | None = None,
) -> list[Any]: ...


@overload
def generate_fake_value(
    dtype: BooleanType,
    faker: Faker,
    constraint: Constraint | None = None,
) -> bool: ...


@overload
def generate_fake_value(
    dtype: DateType,
    faker: Faker,
    constraint: Constraint | None = None,
) -> datetime.date: ...


@overload
def generate_fake_value(
    dtype: TimestampType,
    faker: Faker,
    constraint: Constraint | None = None,
) -> datetime.datetime: ...


def generate_fake_value(
    dtype: DataType,
    faker: Faker,
    constraint: Constraint | None = None,
) -> Any:
    if not _check_dtype_and_constraint_match(dtype=dtype, constraint=constraint):
        raise ValueError(
            f"Constraint type does not match dtype: constraint {constraint.__class__}, dtype: {dtype.__class__}"
        )

    match dtype:
        case ArrayType():
            ...
        case BooleanType():
            ...
        case DateType():
            ...
        case FloatType():
            ...
        case IntegerType():
            ...
        case StringType():
            ...
        case StructType():
            ...
        case TimestampType():
            ...
        case _:
            raise ValueError("Unsupported dtype")
    raise NotImplementedError


def _check_dtype_and_constraint_match(
    dtype: DataType, constraint: Constraint | None
) -> bool:
    """
    Helper to check that a DataType and Constraint match.

    NOTE: Only checks at top-level, i.e. does not check that element Constraints of
    Arrays and Structs match the element DataTypes.
    """
    match dtype:
        case ArrayType():
            return isinstance(constraint, ArrayConstraint)
        case BooleanType():
            return constraint is None
        case DateType():
            return isinstance(constraint, DateConstraint)
        case FloatType():
            return isinstance(constraint, FloatConstraint)
        case IntegerType():
            return isinstance(constraint, IntegerConstraint)
        case StringType():
            return isinstance(constraint, StringConstraint)
        case StructType():
            return isinstance(constraint, StructConstraint)
        case TimestampType():
            return isinstance(constraint, TimestampConstraint)
        case _:
            raise ValueError("Unsupported dtype")
