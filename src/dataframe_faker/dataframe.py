import datetime
import random
import string
from dataclasses import fields, is_dataclass
from decimal import Decimal
from typing import Any, cast

from faker import Faker
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructType,
    TimestampNTZType,
    TimestampType,
)

from .constraints import (
    ArrayConstraint,
    BinaryConstraint,
    BooleanConstraint,
    ByteConstraint,
    Constraint,
    DateConstraint,
    DayTimeIntervalConstraint,
    DecimalConstraint,
    DoubleConstraint,
    FloatConstraint,
    IntegerConstraint,
    LongConstraint,
    ShortConstraint,
    StringConstraint,
    StructConstraint,
    TimestampConstraint,
    TimestampNTZConstraint,
)

ALPHABET = string.ascii_letters + string.digits + " "


def generate_fake_dataframe(
    schema: str | StructType,
    spark: SparkSession,
    constraints: dict[str, Constraint | dict[str, Any] | None] | None = None,
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
        A dictionary mapping column names to `Constraint`s or dictionaries
        that can be converted to `Constraint`s.

    rows
        How many rows should the result DataFrame contain.

    fake : optional
        A `Faker` object to use when generating fake dates, strings, or timestamps.
    """
    if isinstance(schema, str):
        schema = _convert_schema_string_to_schema(schema=schema, spark=spark)

    if constraints is None:
        dataframe_constraint = None
    else:
        element_constraints: dict[str, Constraint | None] = {}
        for field in schema.fields:
            if field.name not in constraints:
                element_constraints[field.name] = None
                continue
            constraint = constraints[field.name]
            if isinstance(constraint, dict):
                element_constraints[field.name] = _convert_dict_to_constraint(
                    constraint=constraint, dtype=field.dataType
                )
            else:
                element_constraints[field.name] = constraint
        dataframe_constraint = StructConstraint(element_constraints=element_constraints)

    if fake is None:
        fake = Faker()

    # Somehow pyright thinks list[dict[str, Any]] does not match any of the `spark.createDataFrame()`
    # overloads, but list[Any] does. Go figure...
    data: list[Any] = [
        generate_fake_value(dtype=schema, fake=fake, constraint=dataframe_constraint)
        for _ in range(rows)
    ]
    return spark.createDataFrame(data=data, schema=schema)


def _convert_schema_string_to_schema(schema: str, spark: SparkSession) -> StructType:
    return spark.createDataFrame([], schema=schema).schema


def generate_fake_value(
    dtype: DataType,
    fake: Faker,
    nullable: bool = False,
    constraint: Constraint | dict[str, str | Any] | None = None,
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
        A `Constraint` or a dictionary that can be converted to a `Constraint` to
        specify what kind of value should be generated.
    """
    if isinstance(constraint, dict):
        constraint = _convert_dict_to_constraint(constraint=constraint, dtype=dtype)

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
        case BinaryType():
            if constraint is None:
                constraint = BinaryConstraint()
            constraint = cast(BinaryConstraint, constraint)

            str_constraint = StringConstraint(
                null_chance=constraint.null_chance,
                string_type="any",
                min_length=constraint.min_length,
                max_length=constraint.max_length,
            )
            fake_str = _generate_fake_string(fake=fake, constraint=str_constraint)
            return bytearray(fake_str.encode())
        case BooleanType():
            if constraint is None:
                constraint = BooleanConstraint()
            constraint = cast(BooleanConstraint, constraint)

            return random.random() >= 1 - constraint.true_chance
        case ByteType() | IntegerType() | LongType() | ShortType():
            if constraint is None:
                constraint = ByteConstraint()
            constraint = cast(
                ByteConstraint | IntegerConstraint | LongConstraint | ShortConstraint,
                constraint,
            )

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
        case DayTimeIntervalType():
            if constraint is None:
                constraint = DayTimeIntervalConstraint()
            constraint = cast(DayTimeIntervalConstraint, constraint)

            fake_timedelta = (
                fake.time_delta(
                    end_datetime=constraint.max_value - constraint.min_value
                )
                + constraint.min_value
            )
            fake_timedelta -= datetime.timedelta(
                microseconds=fake_timedelta.microseconds
            )

            match dtype.endField:
                case DayTimeIntervalType.MINUTE:
                    fake_timedelta -= datetime.timedelta(
                        seconds=fake_timedelta.seconds % 60
                    )
                case DayTimeIntervalType.HOUR:
                    fake_timedelta -= datetime.timedelta(
                        seconds=fake_timedelta.seconds % (60 * 60)
                    )
                case DayTimeIntervalType.DAY:
                    fake_timedelta -= datetime.timedelta(
                        seconds=fake_timedelta.seconds % (60 * 60 * 24)
                    )

            return fake_timedelta
        case DecimalType():
            if constraint is None:
                constraint = DecimalConstraint()
            constraint = cast(DecimalConstraint, constraint)

            scaling = 10**dtype.scale
            min_int = int(constraint.min_value * scaling)
            max_int = int(constraint.max_value * scaling)
            random_int = random.randrange(start=min_int, stop=max_int + 1)
            return Decimal(random_int) / scaling
        case DoubleType() | FloatType():
            if constraint is None:
                constraint = FloatConstraint()
            constraint = cast(DoubleConstraint | FloatConstraint, constraint)

            return random.uniform(a=constraint.min_value, b=constraint.max_value)
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
        case TimestampType() | TimestampNTZType():
            if constraint is None:
                constraint = TimestampConstraint()
            constraint = cast(TimestampConstraint, constraint)

            if dtype == TimestampNTZType():
                tzinfo = None
                if constraint.min_value.tzinfo is not None:
                    constraint.min_value = constraint.min_value.replace(tzinfo=None)
                if constraint.max_value.tzinfo is not None:
                    constraint.max_value = constraint.max_value.replace(tzinfo=None)
            else:
                # tzinfo for generated value is picked in the order
                # constraint.tzinfo
                # > constraint.min_value.tzinfo
                # > constraint.max_value.tzinfo
                # > utc
                tzinfo = [
                    tz
                    for tz in [
                        constraint.tzinfo,
                        constraint.min_value.tzinfo,
                        constraint.max_value.tzinfo,
                        datetime.timezone.utc,
                    ]
                    if tz is not None
                ][0]
                if constraint.min_value.tzinfo is None:
                    constraint.min_value = constraint.min_value.replace(
                        tzinfo=datetime.timezone.utc
                    )
                if constraint.max_value.tzinfo is None:
                    constraint.max_value = constraint.max_value.replace(
                        tzinfo=datetime.timezone.utc
                    )
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


def _convert_dict_to_constraint(
    constraint: dict[str, Any] | None, dtype: DataType
) -> Constraint | None:
    """
    Helper to convert a dictionary to a Constraint.

    Raises a ValueError if the dtype is unsupported.
    """
    if constraint is None:
        return None

    if not isinstance(constraint, dict):
        raise ValueError(
            "Constraint must be a dictionary or None. "
            + f"Got {constraint.__class__} instead."
        )

    result_constraint: Constraint
    match dtype:
        case ArrayType():
            result_constraint = ArrayConstraint()
        case BinaryType():
            result_constraint = BinaryConstraint()
        case BooleanType():
            result_constraint = BooleanConstraint()
        case ByteType():
            result_constraint = ByteConstraint()
        case DateType():
            result_constraint = DateConstraint()
        case DayTimeIntervalType():
            result_constraint = DayTimeIntervalConstraint()
        case DecimalType():
            result_constraint = DecimalConstraint()
        case DoubleType():
            result_constraint = DoubleConstraint()
        case FloatType():
            result_constraint = FloatConstraint()
        case IntegerType():
            result_constraint = IntegerConstraint()
        case LongType():
            result_constraint = LongConstraint()
        case ShortType():
            result_constraint = ShortConstraint()
        case StringType():
            result_constraint = StringConstraint()
        case StructType():
            result_constraint = StructConstraint()
        case TimestampType():
            result_constraint = TimestampConstraint()
        case TimestampNTZType():
            result_constraint = TimestampNTZConstraint()
        case _:
            raise ValueError(f"Unsupported dtype: {dtype.__class__}")

    # Pyrefly does not understand that `result_constraint` is a dataclass instance
    # so we need to help it out.
    assert is_dataclass(result_constraint)
    for field in fields(result_constraint):
        if field.name in constraint:
            value: dict[str, str | Constraint | None] | Constraint | None

            if field.name == "element_constraints":
                assert isinstance(dtype, StructType)
                element_constraints = constraint.get("element_constraints")
                if element_constraints is None:
                    element_constraints = {}
                if not isinstance(element_constraints, dict):
                    raise ValueError(
                        "element_constraints must be a dictionary or None. "
                        + f"Got {element_constraints.__class__} instead."
                    )
                # Just pyrefly things
                value_dict: dict[str, Constraint | str | None] = {}
                for dtype_field in dtype.fields:
                    if dtype_field.name in element_constraints:
                        value_dict[dtype_field.name] = _convert_dict_to_constraint(
                            constraint=constraint[field.name][dtype_field.name],
                            dtype=dtype_field.dataType,
                        )
                value = value_dict

            elif field.name == "element_constraint":
                assert isinstance(dtype, ArrayType)
                value = _convert_dict_to_constraint(
                    constraint=constraint.get("element_constraint"),
                    dtype=dtype.elementType,
                )

            else:
                value = constraint[field.name]

            setattr(result_constraint, field.name, value)

    return result_constraint


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
        case BinaryType():
            if not isinstance(constraint, BinaryConstraint):
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
        case DayTimeIntervalType():
            if not isinstance(constraint, DayTimeIntervalConstraint):
                raise ValueError(type_mismatch_error_msg)
        case DecimalType():
            if not isinstance(constraint, DecimalConstraint):
                raise ValueError(type_mismatch_error_msg)
        case DoubleType():
            if not isinstance(constraint, DoubleConstraint):
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
        case TimestampNTZType():
            if not isinstance(constraint, TimestampNTZConstraint):
                raise ValueError(type_mismatch_error_msg)
        case _:
            raise ValueError(f"Unsupported dtype: {dtype.__class__}")


def _generate_fake_string(fake: Faker, constraint: StringConstraint) -> str:
    match constraint.string_type:
        case "address":
            return fake.address()
        case "any":
            size = random.randrange(
                start=constraint.min_length, stop=constraint.max_length + 1
            )
            alphabet = (
                constraint.alphabet if constraint.alphabet is not None else ALPHABET
            )
            return "".join(random.choices(population=alphabet, k=size))
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
