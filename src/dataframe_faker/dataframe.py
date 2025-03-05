import random
import string
from typing import Any, cast

from faker import Faker
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructType,
    TimestampType,
)

from .constraints import (
    ArrayConstraint,
    BooleanConstraint,
    ByteConstraint,
    Constraint,
    DateConstraint,
    FloatConstraint,
    IntegerConstraint,
    LongConstraint,
    ShortConstraint,
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
    """
    Function to generate a PySpark DataFrame with schema matching `schema`
    filled with fake data conforming to constraints specified by `constraints`.

    Parameters
    ----------
    schema
        Either a string that PySpark can parse, e.g. "id: int, name: string, arr: array<float>"
        or a `StructType` schema definition.

    spark
        A SparkSession to use for creating the DataFrame.

    constraints : optional
        A dictionary mapping column names to `Constraint`s.

    rows
        How many rows should the result DataFrame contain.

    fake : optional
        A `Faker` object to use when generating fake dates, strings, or timestamps.
    """
    if isinstance(schema, str):
        schema = _convert_schema_string_to_schema(schema=schema, spark=spark)

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


def _convert_schema_string_to_schema(schema: str, spark: SparkSession) -> StructType:
    return spark.createDataFrame([], schema=schema).schema


def generate_fake_value(
    dtype: DataType,
    fake: Faker,
    nullable: bool = False,
    constraint: Constraint | None = None,
) -> Any:
    """
    Function to generate a fake value with type/schema matching `dtype`
    and conforming to constraints specified by `constraint`.

    Parameters
    ----------
    dtype
        A PySpark `DataType`.

    fake
        A `Faker` object to use when generating fake dates, strings, or timestamps.

    nullable
        Whether the values can be null. In the case when `dtype` is `StructType`,
        the nullability of the struct's fields are passed down with this parameter.
        If this is manually specified, it only applies at the top-level.

        NOTE: This only specifies that the field is nullable. The probability of a
        value being null needs to be specified in the `constraint`.

    constraint : optional
        A `Constraint` to specify what kind of value should be generated.
    """
    if constraint is not None:
        _validate_dtype_and_constraint(dtype=dtype, constraint=constraint)

    if nullable and constraint is not None and constraint.null_chance > 0.0:
        if random.random() < constraint.null_chance:
            return None

    if constraint is not None and constraint.allowed_values is not None:
        if len(constraint.allowed_values) == 0:
            raise ValueError(
                "Empty list of allowed values specified; can't return anything."
            )
        return random.choice(constraint.allowed_values)

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
            if constraint is None:
                constraint = BooleanConstraint()
            constraint = cast(BooleanConstraint, constraint)

            return random.random() >= 1 - constraint.true_chance
        case ByteType():
            if constraint is None:
                constraint = ByteConstraint()
            constraint = cast(ByteConstraint, constraint)

            return random.randrange(
                start=constraint.min_value, stop=constraint.max_value + 1
            )
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
        case LongType():
            if constraint is None:
                constraint = LongConstraint()
            constraint = cast(LongConstraint, constraint)

            return random.randrange(
                start=constraint.min_value, stop=constraint.max_value + 1
            )
        case ShortType():
            if constraint is None:
                constraint = ShortConstraint()
            constraint = cast(ShortConstraint, constraint)

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

            tzinfo = constraint.tzinfo
            if tzinfo is None:
                tzinfo = constraint.min_value.tzinfo
            dt = fake.date_time_between(
                start_date=constraint.min_value,
                end_date=constraint.max_value,
                tzinfo=tzinfo,
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


def _validate_dtype_and_constraint(
    dtype: DataType, constraint: Constraint | None
) -> None:
    """
    Helper to check that a DataType and Constraint match and validate a Constraint.

    Raises a ValueError if validation fails.

    NOTE: Only checks at top-level, i.e. does not validate the Constraints of elements of
    complex types.
    """
    type_mismatch_error_msg = (
        "Constraint type does not match dtype: "
        + f"constraint {constraint.__class__}, "
        + f"dtype: {dtype.__class__}"
    )

    match dtype:
        case ArrayType():
            if not isinstance(constraint, ArrayConstraint):
                raise ValueError(type_mismatch_error_msg)
        case BooleanType():
            if not isinstance(constraint, BooleanConstraint):
                raise ValueError(type_mismatch_error_msg)
        case ByteType():
            if not isinstance(constraint, ByteConstraint):
                raise ValueError(type_mismatch_error_msg)
            if not constraint.min_value >= -128 and constraint.max_value <= 127:
                raise ValueError(
                    "ByteConstraint min_value has to be >= -128 and max_value has to be <= 127."
                )
        case DateType():
            if not isinstance(constraint, DateConstraint):
                raise ValueError(type_mismatch_error_msg)
        case FloatType():
            if not isinstance(constraint, FloatConstraint):
                raise ValueError(type_mismatch_error_msg)
        case IntegerType():
            if not isinstance(constraint, IntegerConstraint):
                raise ValueError(type_mismatch_error_msg)
            if not (
                constraint.min_value >= -2147483648
                and constraint.max_value <= 2147483647
            ):
                raise ValueError(
                    "IntegerConstraint min_value has to be >= -2147483648 "
                    "and max_value has to be <= 2147483647."
                )
        case LongType():
            if not isinstance(constraint, LongConstraint):
                raise ValueError(type_mismatch_error_msg)
            if not (
                constraint.min_value >= -9223372036854775808
                and constraint.max_value <= 9223372036854775807
            ):
                raise ValueError(
                    "LongConstraint min_value has to be >= -9223372036854775808 "
                    "and max_value has to be <= 9223372036854775807."
                )
        case ShortType():
            if not isinstance(constraint, ShortConstraint):
                raise ValueError(type_mismatch_error_msg)
            if not (constraint.min_value >= -32768 and constraint.max_value <= 32767):
                raise ValueError(
                    "ShortConstraint min_value has to be >= -32768 and max_value has to be <= 32767."
                )
        case StringType():
            if not isinstance(constraint, StringConstraint):
                raise ValueError(type_mismatch_error_msg)
        case StructType():
            if not isinstance(constraint, StructConstraint):
                raise ValueError(type_mismatch_error_msg)
        case TimestampType():
            if not isinstance(constraint, TimestampConstraint):
                raise ValueError(type_mismatch_error_msg)
        case _:
            raise ValueError(f"Unsupported dtype: {dtype.__class__}")


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
