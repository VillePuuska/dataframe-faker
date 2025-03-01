import datetime
import random
import string
from typing import Any, cast, overload

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

ALPHABET = string.ascii_letters + string.digits + " "


def generate_fake_dataframe(
    schema: str | StructType,
    spark: SparkSession,
    constraints: dict[str, Constraint | None] | None = None,
    rows: int = 100,
    fake: Faker | None = None,
) -> DataFrame:
    if isinstance(schema, str):
        schema = convert_schema_string_to_schema(schema=schema, spark=spark)

    if constraints is None:
        constraint = None
    else:
        constraint = StructConstraint(element_constraints=constraints)

    if fake is None:
        fake = Faker()

    # Somehow pyright thinks list[dict[str, Any]] does not match any of the `spark.createDataFrame()`
    # overloads, but list[Any] does. Go figure...
    data: list[Any] = [
        generate_fake_value(dtype=schema, fake=fake, constraint=constraint)
        for _ in range(rows)
    ]
    return spark.createDataFrame(data=data, schema=schema)


def convert_schema_string_to_schema(schema: str, spark: SparkSession) -> StructType:
    return spark.createDataFrame([], schema=schema).schema


@overload
def generate_fake_value(
    dtype: StructType,
    fake: Faker,
    nullable: bool = False,
    constraint: Constraint | None = None,
) -> dict[str, Any]: ...


@overload
def generate_fake_value(
    dtype: StringType,
    fake: Faker,
    nullable: bool = False,
    constraint: Constraint | None = None,
) -> str: ...


@overload
def generate_fake_value(
    dtype: IntegerType,
    fake: Faker,
    nullable: bool = False,
    constraint: Constraint | None = None,
) -> int: ...


@overload
def generate_fake_value(
    dtype: FloatType,
    fake: Faker,
    nullable: bool = False,
    constraint: Constraint | None = None,
) -> float: ...


@overload
def generate_fake_value(
    dtype: ArrayType,
    fake: Faker,
    nullable: bool = False,
    constraint: Constraint | None = None,
) -> list[Any]: ...


@overload
def generate_fake_value(
    dtype: BooleanType,
    fake: Faker,
    nullable: bool = False,
    constraint: Constraint | None = None,
) -> bool: ...


@overload
def generate_fake_value(
    dtype: DateType,
    fake: Faker,
    nullable: bool = False,
    constraint: Constraint | None = None,
) -> datetime.date: ...


@overload
def generate_fake_value(
    dtype: TimestampType,
    fake: Faker,
    nullable: bool = False,
    constraint: Constraint | None = None,
) -> datetime.datetime: ...


@overload
def generate_fake_value(
    dtype: DataType,
    fake: Faker,
    nullable: bool = False,
    constraint: Constraint | None = None,
) -> Any: ...


def generate_fake_value(
    dtype: DataType,
    fake: Faker,
    nullable: bool = False,
    constraint: Constraint | None = None,
) -> Any:
    if constraint is not None and not _check_dtype_and_constraint_match(
        dtype=dtype, constraint=constraint
    ):
        error_msg = (
            "Constraint type does not match dtype: "
            + f"constraint {constraint.__class__}, "
            + f"dtype: {dtype.__class__}"
        )
        raise ValueError(error_msg)

    if nullable and constraint is not None and constraint.null_chance > 0.0:
        if random.random() < constraint.null_chance:
            return None

    match dtype:
        case ArrayType():
            if constraint is None:
                constraint = ArrayConstraint()
            constraint = cast(ArrayConstraint, constraint)

            size = random.randrange(
                start=constraint.min_length, stop=constraint.max_length + 1
            )
            return [
                generate_fake_value(
                    dtype=dtype.elementType,
                    fake=fake,
                    nullable=dtype.containsNull,
                    constraint=constraint.element_constraint,
                )
                for _ in range(size)
            ]
        case BooleanType():
            return random.random() > 0.5
        case DateType():
            if constraint is None:
                constraint = DateConstraint()
            constraint = cast(DateConstraint, constraint)

            return fake.date_between_dates(
                date_start=constraint.min_value, date_end=constraint.max_value
            )
        case FloatType():
            if constraint is None:
                constraint = FloatConstraint()
            constraint = cast(FloatConstraint, constraint)

            return random.uniform(a=constraint.min_value, b=constraint.max_value)
        case IntegerType():
            if constraint is None:
                constraint = IntegerConstraint()
            constraint = cast(IntegerConstraint, constraint)

            return random.randrange(
                start=constraint.min_value, stop=constraint.max_value + 1
            )
        case StringType():
            if constraint is None:
                constraint = StringConstraint()
            constraint = cast(StringConstraint, constraint)

            return _generate_fake_string(fake=fake, constraint=constraint)
        case StructType():
            if constraint is None:
                constraint = StructConstraint()
            constraint = cast(StructConstraint, constraint)

            faked_data = {}
            for field in dtype.fields:
                data = generate_fake_value(
                    dtype=field.dataType,
                    fake=fake,
                    nullable=field.nullable,
                    constraint=constraint.element_constraints.get(field.name),
                )
                faked_data[field.name] = data
            return faked_data
        case TimestampType():
            if constraint is None:
                constraint = TimestampConstraint()
            constraint = cast(TimestampConstraint, constraint)

            dt = fake.date_time_between(
                start_date=constraint.min_value,
                end_date=constraint.max_value,
                tzinfo=constraint.tzinfo,
            )
            # NOTE: For some reason Faker does not respect limits when generating
            # microseconds so the datetime can fall out of the given range.
            # The following replace is done to fix it.
            if dt < constraint.min_value:
                dt = dt.replace(microsecond=constraint.min_value.microsecond)
            elif dt > constraint.max_value:
                dt = dt.replace(microsecond=constraint.max_value.microsecond)
            return dt
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


def _generate_fake_string(fake: Faker, constraint: StringConstraint) -> str:
    match constraint.string_type:
        case "address":
            return ""
        case "any":
            size = random.randrange(
                start=constraint.min_length, stop=constraint.max_length + 1
            )
            return "".join(random.choices(population=ALPHABET, k=size))
        case "email":
            return fake.email()
        case "first_name":
            return fake.first_name()
        case "last_name":
            return fake.last_name()
        case "name":
            return fake.name()
        case "phone_number":
            return fake.phone_number()
        case "uuid4":
            return fake.uuid4()
        case _:
            raise ValueError(f"Unknown string type: {constraint.string_type}")
