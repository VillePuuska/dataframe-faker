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


def generate_fake_dataframe(
    schema: str | StructType,
    # TODO: need actual types for specifying constraints for different dtypes
    constraints: dict[str, Any],
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
    constraint: Any,
) -> dict[str, Any]: ...


@overload
def generate_fake_value(
    dtype: StringType,
    faker: Faker,
    constraint: Any,
) -> str: ...


@overload
def generate_fake_value(
    dtype: IntegerType,
    faker: Faker,
    constraint: Any,
) -> int: ...


@overload
def generate_fake_value(
    dtype: FloatType,
    faker: Faker,
    constraint: Any,
) -> float: ...


@overload
def generate_fake_value(
    dtype: ArrayType,
    faker: Faker,
    constraint: Any,
) -> list[Any]: ...


@overload
def generate_fake_value(
    dtype: BooleanType,
    faker: Faker,
    constraint: Any,
) -> bool: ...


@overload
def generate_fake_value(
    dtype: DateType,
    faker: Faker,
    constraint: Any,
) -> datetime.date: ...


@overload
def generate_fake_value(
    dtype: TimestampType,
    faker: Faker,
    constraint: Any,
) -> datetime.datetime: ...


def generate_fake_value(
    dtype: DataType,
    faker: Faker,
    constraint: Any,
) -> Any:
    raise NotImplementedError
