import datetime
import zoneinfo
from decimal import Decimal
from string import ascii_lowercase, digits

import pytest
from faker import Faker
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

from dataframe_faker.constraints import (
    ArrayConstraint,
    BinaryConstraint,
    BooleanConstraint,
    ByteConstraint,
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
from dataframe_faker.dataframe import (
    ALPHABET,
    _convert_schema_string_to_schema,
    _validate_dtype_and_constraint,
    generate_fake_dataframe,
    generate_fake_value,
)

from .helpers import assert_schema_equal, is_valid_email

UUID_ALPHABET = ascii_lowercase + digits + "-"


def test_convert_schema_string_to_schema(spark: SparkSession) -> None:
    schema_str = (
        "id: int not null, str_col: string, struct_col: struct<arr: array<float>>"
    )

    actual = _convert_schema_string_to_schema(schema=schema_str, spark=spark)
    expected = StructType(
        [
            StructField(name="id", dataType=IntegerType(), nullable=False),
            StructField(name="str_col", dataType=StringType(), nullable=True),
            StructField(
                name="struct_col",
                dataType=StructType(
                    [
                        StructField(
                            name="arr",
                            dataType=ArrayType(elementType=FloatType()),
                            nullable=True,
                        )
                    ]
                ),
                nullable=True,
            ),
        ]
    )

    assert_schema_equal(actual=actual, expected=expected)


def test__validate_dtype_and_constraint() -> None:
    dtypes = [
        ArrayType(elementType=IntegerType()),
        BinaryType(),
        BooleanType(),
        ByteType(),
        DateType(),
        DayTimeIntervalType(),
        DecimalType(),
        DoubleType(),
        FloatType(),
        IntegerType(),
        LongType(),
        ShortType(),
        StringType(),
        StructType(),
        TimestampType(),
        TimestampNTZType(),
    ]
    constraints = [
        ArrayConstraint(),
        BinaryConstraint(),
        BooleanConstraint(),
        ByteConstraint(),
        DateConstraint(),
        DayTimeIntervalConstraint(),
        DecimalConstraint(),
        DoubleConstraint(),
        FloatConstraint(),
        IntegerConstraint(),
        LongConstraint(),
        ShortConstraint(),
        StringConstraint(),
        StructConstraint(),
        TimestampConstraint(),
        TimestampNTZConstraint(),
    ]
    for dtype, constraint in zip(dtypes, constraints):
        _validate_dtype_and_constraint(dtype=dtype, constraint=constraint)

    with pytest.raises(ValueError):
        _validate_dtype_and_constraint(
            dtype=ArrayType(elementType=IntegerType()),
            constraint=IntegerConstraint(),
        )
    with pytest.raises(ValueError):
        _validate_dtype_and_constraint(
            dtype=ArrayType(elementType=IntegerType()),
            constraint=StructConstraint(),
        )
    with pytest.raises(ValueError):
        _validate_dtype_and_constraint(
            dtype=StructType(),
            constraint=IntegerConstraint(),
        )
    with pytest.raises(ValueError):
        _validate_dtype_and_constraint(
            dtype=StructType(),
            constraint=ArrayConstraint(),
        )
    with pytest.raises(ValueError):
        _validate_dtype_and_constraint(
            dtype=IntegerType(),
            constraint=StringConstraint(),
        )
    with pytest.raises(ValueError):
        _validate_dtype_and_constraint(
            dtype=IntegerType(),
            constraint=StructConstraint(),
        )
    with pytest.raises(ValueError):
        _validate_dtype_and_constraint(
            dtype=ByteType(),
            constraint=ByteConstraint(min_value=-200),
        )
    with pytest.raises(ValueError):
        _validate_dtype_and_constraint(
            dtype=ShortType(),
            constraint=ShortConstraint(max_value=9999999),
        )
    with pytest.raises(ValueError):
        _validate_dtype_and_constraint(
            dtype=IntegerType(),
            constraint=IntegerConstraint(max_value=9223372036854775),
        )
    with pytest.raises(ValueError):
        _validate_dtype_and_constraint(
            dtype=LongType(),
            constraint=LongConstraint(min_value=-9223372036854775809),
        )

    # only checks top-level
    _validate_dtype_and_constraint(
        dtype=ArrayType(elementType=StringType()),
        constraint=ArrayConstraint(element_constraint=IntegerConstraint()),
    )

    # works with fields inside StructType as well
    _validate_dtype_and_constraint(
        dtype=StructType(fields=[StructField(name="asd", dataType=StringType())]),
        constraint=StructConstraint(),
    )

    with pytest.raises(ValueError):
        _validate_dtype_and_constraint(dtype=StringType(), constraint=None)


def test_generate_fake_value(fake: Faker) -> None:
    for _ in range(100):
        actual_list = generate_fake_value(
            dtype=ArrayType(elementType=IntegerType()),
            fake=fake,
            nullable=False,
            constraint=ArrayConstraint(
                element_constraint=IntegerConstraint(min_value=1, max_value=1),
                min_length=2,
                max_length=2,
            ),
        )
        assert isinstance(actual_list, list)
        assert len(actual_list) == 2
        assert actual_list[0] == 1
        assert actual_list[1] == 1

        actual_binary = generate_fake_value(
            dtype=BinaryType(),
            nullable=False,
            fake=fake,
            constraint=BinaryConstraint(min_length=4, max_length=4),
        )
        assert isinstance(actual_binary, bytearray)
        assert len(actual_binary) == 4

        actual_bool = generate_fake_value(
            dtype=BooleanType(), nullable=False, fake=fake
        )
        assert isinstance(actual_bool, bool)

        actual_bool = generate_fake_value(
            dtype=BooleanType(),
            nullable=False,
            fake=fake,
            constraint=BooleanConstraint(true_chance=1.0),
        )
        assert isinstance(actual_bool, bool)
        assert actual_bool

        actual_bool = generate_fake_value(
            dtype=BooleanType(),
            nullable=False,
            fake=fake,
            constraint=BooleanConstraint(true_chance=0.0),
        )
        assert isinstance(actual_bool, bool)
        assert not actual_bool

        actual_byte = generate_fake_value(
            dtype=ByteType(),
            fake=fake,
            nullable=False,
            constraint=ByteConstraint(min_value=1, max_value=5),
        )
        assert isinstance(actual_byte, int)
        assert actual_byte in range(1, 6)

        actual_date = generate_fake_value(
            dtype=DateType(),
            fake=fake,
            nullable=False,
            constraint=DateConstraint(
                min_value=datetime.date(year=2024, month=3, day=2),
                max_value=datetime.date(year=2024, month=3, day=3),
            ),
        )
        assert isinstance(actual_date, datetime.date)
        assert actual_date in [
            datetime.date(year=2024, month=3, day=2),
            datetime.date(year=2024, month=3, day=3),
        ]

        actual_daytimeinterval = generate_fake_value(
            dtype=DayTimeIntervalType(),
            fake=fake,
            nullable=False,
            constraint=DayTimeIntervalConstraint(
                min_value=datetime.timedelta(minutes=2.1),
                max_value=datetime.timedelta(minutes=2.1),
            ),
        )
        assert isinstance(actual_daytimeinterval, datetime.timedelta)
        assert actual_daytimeinterval == datetime.timedelta(minutes=2.1)

        actual_decimal = generate_fake_value(
            dtype=DecimalType(scale=3),
            fake=fake,
            nullable=False,
            constraint=DecimalConstraint(
                min_value=Decimal("5.123"), max_value=Decimal("5.123")
            ),
        )
        assert isinstance(actual_decimal, Decimal)
        assert actual_decimal == Decimal("5.123")

        actual_double = generate_fake_value(
            dtype=DoubleType(),
            fake=fake,
            nullable=False,
            constraint=DoubleConstraint(min_value=5.0, max_value=5.0),
        )
        assert isinstance(actual_double, float)
        assert actual_double == 5.0

        actual_float = generate_fake_value(
            dtype=FloatType(),
            fake=fake,
            nullable=False,
            constraint=FloatConstraint(min_value=5.0, max_value=5.0),
        )
        assert isinstance(actual_float, float)
        assert actual_float == 5.0

        actual_float = generate_fake_value(
            dtype=FloatType(),
            fake=fake,
            nullable=False,
            constraint=FloatConstraint(min_value=-1.0, max_value=1.0),
        )
        assert isinstance(actual_float, float)
        assert actual_float >= -1.0
        assert actual_float <= 1.0

        actual_int = generate_fake_value(
            dtype=IntegerType(),
            fake=fake,
            nullable=False,
            constraint=IntegerConstraint(min_value=1, max_value=5),
        )
        assert isinstance(actual_int, int)
        assert actual_int in range(1, 6)

        actual_long = generate_fake_value(
            dtype=LongType(),
            fake=fake,
            nullable=False,
            constraint=LongConstraint(min_value=30000000000, max_value=30000000005),
        )
        assert isinstance(actual_long, int)
        assert actual_long in range(30000000000, 30000000006)

        actual_short = generate_fake_value(
            dtype=ShortType(),
            fake=fake,
            nullable=False,
            constraint=ShortConstraint(min_value=1, max_value=5),
        )
        assert isinstance(actual_short, int)
        assert actual_short in range(1, 6)

        actual_string = generate_fake_value(
            dtype=StringType(),
            fake=fake,
            nullable=False,
            constraint=StringConstraint(string_type="address"),
        )
        assert isinstance(actual_string, str)

        actual_string = generate_fake_value(
            dtype=StringType(),
            fake=fake,
            nullable=False,
            constraint=StringConstraint(
                string_type="any", min_length=16, max_length=16
            ),
        )
        assert isinstance(actual_string, str)
        assert len(actual_string) == 16
        for c in actual_string:
            assert c in ALPHABET

        actual_string = generate_fake_value(
            dtype=StringType(),
            fake=fake,
            nullable=False,
            constraint=StringConstraint(string_type="email"),
        )
        assert isinstance(actual_string, str)
        assert is_valid_email(email=actual_string)

        actual_string = generate_fake_value(
            dtype=StringType(),
            fake=fake,
            nullable=False,
            constraint=StringConstraint(string_type="first_name"),
        )
        assert isinstance(actual_string, str)

        actual_string = generate_fake_value(
            dtype=StringType(),
            fake=fake,
            nullable=False,
            constraint=StringConstraint(string_type="last_name"),
        )
        assert isinstance(actual_string, str)

        actual_string = generate_fake_value(
            dtype=StringType(),
            fake=fake,
            nullable=False,
            constraint=StringConstraint(string_type="name"),
        )
        assert isinstance(actual_string, str)

        actual_string = generate_fake_value(
            dtype=StringType(),
            fake=fake,
            nullable=False,
            constraint=StringConstraint(string_type="phone_number"),
        )
        assert isinstance(actual_string, str)

        actual_string = generate_fake_value(
            dtype=StringType(),
            fake=fake,
            nullable=False,
            constraint=StringConstraint(string_type="uuid4"),
        )
        assert isinstance(actual_string, str)
        assert len(actual_string) == 32 + 4
        for c in actual_string:
            assert c in UUID_ALPHABET
        assert actual_string.count("-") == 4
        assert [len(s) for s in actual_string.split("-")] == [8, 4, 4, 4, 12]

        actual_struct = generate_fake_value(
            dtype=StructType(
                fields=[
                    StructField(name="f1", dataType=IntegerType(), nullable=True),
                    StructField(name="g2", dataType=StringType()),
                ]
            ),
            fake=fake,
            nullable=False,
            constraint=StructConstraint(
                element_constraints={
                    "f1": IntegerConstraint(null_chance=1.0),
                    "g2": StringConstraint(string_type="email"),
                }
            ),
        )
        assert isinstance(actual_struct, dict)
        assert actual_struct["f1"] is None
        assert is_valid_email(actual_struct["g2"])

        actual_timestamp = generate_fake_value(
            dtype=TimestampType(),
            fake=fake,
            nullable=False,
            constraint=TimestampConstraint(
                min_value=datetime.datetime(
                    year=2020,
                    month=1,
                    day=1,
                    hour=3,
                    minute=1,
                    second=1,
                    microsecond=500000,
                    tzinfo=zoneinfo.ZoneInfo("Europe/Helsinki"),
                ),
                max_value=datetime.datetime(
                    year=2020,
                    month=1,
                    day=1,
                    hour=1,
                    minute=1,
                    second=10,
                    microsecond=500000,
                    tzinfo=zoneinfo.ZoneInfo("UTC"),
                ),
                tzinfo=zoneinfo.ZoneInfo("Europe/Helsinki"),
            ),
        )
        assert isinstance(actual_timestamp, datetime.datetime)
        assert actual_timestamp >= datetime.datetime(
            year=2020,
            month=1,
            day=1,
            hour=1,
            minute=1,
            second=1,
            microsecond=500000,
            tzinfo=zoneinfo.ZoneInfo("UTC"),
        )
        assert actual_timestamp <= datetime.datetime(
            year=2020,
            month=1,
            day=1,
            hour=1,
            minute=1,
            second=10,
            microsecond=500000,
            tzinfo=zoneinfo.ZoneInfo("UTC"),
        )

        actual_timestamp_ntz = generate_fake_value(
            dtype=TimestampNTZType(),
            fake=fake,
            nullable=False,
            constraint=TimestampNTZConstraint(
                min_value=datetime.datetime(
                    year=2020,
                    month=1,
                    day=1,
                    hour=1,
                    minute=1,
                    second=1,
                    microsecond=500000,
                ),
                max_value=datetime.datetime(
                    year=2020,
                    month=1,
                    day=1,
                    hour=1,
                    minute=1,
                    second=1,
                    microsecond=500000,
                ),
            ),
        )
        assert isinstance(actual_timestamp_ntz, datetime.datetime)
        assert actual_timestamp_ntz == datetime.datetime(
            year=2020,
            month=1,
            day=1,
            hour=1,
            minute=1,
            second=1,
            microsecond=500000,
        )

        actual_timestamp = generate_fake_value(dtype=TimestampType(), fake=fake)
        assert actual_timestamp.tzinfo is not None

        actual_timestamp_ntz = generate_fake_value(dtype=TimestampNTZType(), fake=fake)
        assert actual_timestamp_ntz.tzinfo is None

        actual_int = generate_fake_value(
            dtype=IntegerType(),
            fake=fake,
            nullable=False,
            constraint=IntegerConstraint(allowed_values=[3]),
        )
        expected_int = 3
        assert actual_int == expected_int

        actual_struct = generate_fake_value(
            dtype=StructType(),
            fake=fake,
            nullable=False,
            constraint=StructConstraint(allowed_values=[{"a": 1, "b": False}]),
        )
        expected_struct = {"a": 1, "b": False}
        assert actual_struct == expected_struct


def test_generate_fake_dataframe(spark: SparkSession, fake: Faker) -> None:
    schema_str = """
    array_col: array<integer>,
    binary_col: binary,
    boolean_col: boolean,
    byte_col: byte,
    date_col: date,
    daytimeinterval_col_1: interval day,
    daytimeinterval_col_2: interval hour to second,
    decimal_col_1: decimal(1,0),
    decimal_col_2: decimal(28,10),
    double_col: double,
    float_col: float,
    integer_col: integer,
    long_col: long,
    short_col: short,
    string_col: string,
    struct_col: struct<
        nested_integer: integer,
        nested_string: string
    >,
    timestamp_col_1: timestamp,
    timestamp_col_2: timestamp,
    timestamp_ntz_col: timestamp_ntz
    """
    rows = 100
    actual = generate_fake_dataframe(
        schema=schema_str,
        spark=spark,
        fake=fake,
        constraints={
            "array_col": ArrayConstraint(
                element_constraint=IntegerConstraint(min_value=1, max_value=1),
                min_length=2,
                max_length=2,
            ),
            "binary_col": BinaryConstraint(min_length=4, max_length=4),
            "boolean_col": BooleanConstraint(true_chance=1.0),
            "byte_col": ByteConstraint(min_value=1, max_value=1),
            "date_col": DateConstraint(
                min_value=datetime.date(year=2020, month=1, day=1),
                max_value=datetime.date(year=2020, month=1, day=1),
            ),
            "daytimeinterval_col_1": DayTimeIntervalConstraint(
                min_value=datetime.timedelta(days=2),
                max_value=datetime.timedelta(days=2, hours=2),
            ),
            "daytimeinterval_col_2": DayTimeIntervalConstraint(
                min_value=datetime.timedelta(hours=1, seconds=1),
                max_value=datetime.timedelta(hours=1, seconds=1),
            ),
            "decimal_col_1": DecimalConstraint(
                min_value=Decimal("2.1"), max_value=Decimal("2.1")
            ),
            "decimal_col_2": DecimalConstraint(
                min_value=Decimal("1111.2222"), max_value=Decimal("1111.2222")
            ),
            "double_col": DoubleConstraint(min_value=1.0, max_value=1.0),
            "float_col": FloatConstraint(min_value=1.0, max_value=1.0),
            "integer_col": IntegerConstraint(min_value=1, max_value=1),
            "long_col": LongConstraint(min_value=30000000005, max_value=30000000005),
            "short_col": ShortConstraint(min_value=1, max_value=1),
            "string_col": StringConstraint(
                string_type="any", min_length=5, max_length=5
            ),
            "struct_col": StructConstraint(
                element_constraints={
                    "nested_integer": IntegerConstraint(min_value=1, max_value=1),
                    "nested_string": StringConstraint(null_chance=1.0),
                }
            ),
            "timestamp_col_1": TimestampConstraint(
                min_value=datetime.datetime(
                    year=2020, month=1, day=1, hour=2, minute=3, second=4, microsecond=5
                ),
                max_value=datetime.datetime(
                    year=2020, month=1, day=1, hour=2, minute=3, second=4, microsecond=5
                ),
            ),
            "timestamp_col_2": TimestampConstraint(
                min_value=datetime.datetime(
                    year=2020,
                    month=1,
                    day=1,
                    hour=2,
                    minute=3,
                    second=4,
                    microsecond=5,
                    tzinfo=zoneinfo.ZoneInfo("Europe/Helsinki"),
                ),
                max_value=datetime.datetime(
                    year=2020,
                    month=1,
                    day=1,
                    hour=2,
                    minute=3,
                    second=4,
                    microsecond=5,
                    tzinfo=zoneinfo.ZoneInfo("Europe/Helsinki"),
                ),
            ),
            "timestamp_ntz_col": TimestampNTZConstraint(
                min_value=datetime.datetime(
                    year=2021, month=1, day=1, hour=2, minute=3, second=4, microsecond=5
                ),
                max_value=datetime.datetime(
                    year=2021, month=1, day=1, hour=2, minute=3, second=4, microsecond=5
                ),
            ),
        },
        rows=rows,
    )

    actual_schema = actual.schema
    expected_schema = spark.createDataFrame([], schema=schema_str).schema
    assert_schema_equal(
        actual=actual_schema,
        expected=expected_schema,
    )

    actual_collected = actual.collect()

    actual_array_col = [row.array_col for row in actual_collected]
    expected_array_col = [[1, 1] for _ in range(rows)]
    assert actual_array_col == expected_array_col

    actual_binary_col_lens = [len(row.binary_col) for row in actual_collected]
    expected_binary_col_lens = [4 for _ in range(rows)]
    assert actual_binary_col_lens == expected_binary_col_lens

    actual_boolean_col = [row.boolean_col for row in actual_collected]
    expected_boolean_col = [True for _ in range(rows)]
    assert actual_boolean_col == expected_boolean_col

    actual_byte_col = [row.byte_col for row in actual_collected]
    expected_byte_col = [1 for _ in range(rows)]
    assert actual_byte_col == expected_byte_col

    actual_date_col = [row.date_col for row in actual_collected]
    expected_date_col = [datetime.date(year=2020, month=1, day=1) for _ in range(rows)]
    assert actual_date_col == expected_date_col

    actual_daytimeinterval_col_1 = [
        row.daytimeinterval_col_1 for row in actual_collected
    ]
    expected_daytimeinterval_col_1 = [datetime.timedelta(days=2) for _ in range(rows)]
    assert actual_daytimeinterval_col_1 == expected_daytimeinterval_col_1

    actual_daytimeinterval_col_2 = [
        row.daytimeinterval_col_2 for row in actual_collected
    ]
    expected_daytimeinterval_col_2 = [
        datetime.timedelta(hours=1, seconds=1) for _ in range(rows)
    ]
    assert actual_daytimeinterval_col_2 == expected_daytimeinterval_col_2

    actual_decimal_col_1 = [row.decimal_col_1 for row in actual_collected]
    expected_decimal_col_1 = [Decimal("2") for _ in range(rows)]
    assert actual_decimal_col_1 == expected_decimal_col_1

    actual_decimal_col_2 = [row.decimal_col_2 for row in actual_collected]
    expected_decimal_col_2 = [Decimal("1111.2222") for _ in range(rows)]
    assert actual_decimal_col_2 == expected_decimal_col_2

    actual_double_col = [row.double_col for row in actual_collected]
    expected_double_col = [1.0 for _ in range(rows)]
    assert actual_double_col == expected_double_col

    actual_float_col = [row.float_col for row in actual_collected]
    expected_float_col = [1.0 for _ in range(rows)]
    assert actual_float_col == expected_float_col

    actual_integer_col = [row.integer_col for row in actual_collected]
    expected_integer_col = [1 for _ in range(rows)]
    assert actual_integer_col == expected_integer_col

    actual_long_col = [row.long_col for row in actual_collected]
    expected_long_col = [30000000005 for _ in range(rows)]
    assert actual_long_col == expected_long_col

    actual_short_col = [row.short_col for row in actual_collected]
    expected_short_col = [1 for _ in range(rows)]
    assert actual_short_col == expected_short_col

    actual_string_col = [row.string_col for row in actual_collected]
    for val in actual_string_col:
        assert isinstance(val, str)
        assert len(val) == 5
        for c in val:
            assert c in ALPHABET

    actual_struct_col = [row.struct_col for row in actual_collected]
    expected_struct_col = [
        Row(nested_integer=1, nested_string=None) for _ in range(rows)
    ]
    assert actual_struct_col == expected_struct_col

    actual_timestamp_col_1 = [row.timestamp_col_1 for row in actual_collected]
    expected_timestamp_col_1 = [
        datetime.datetime(
            year=2020, month=1, day=1, hour=2, minute=3, second=4, microsecond=5
        )
        for _ in range(rows)
    ]
    assert actual_timestamp_col_1 == expected_timestamp_col_1

    actual_timestamp_col_2 = [
        row.timestamp_col_2.replace(tzinfo=zoneinfo.ZoneInfo("UTC"))
        for row in actual_collected
    ]
    expected_timestamp_col_2 = [
        datetime.datetime(
            year=2020,
            month=1,
            day=1,
            hour=2,
            minute=3,
            second=4,
            microsecond=5,
            tzinfo=zoneinfo.ZoneInfo("Europe/Helsinki"),
        )
        for _ in range(rows)
    ]
    assert actual_timestamp_col_2 == expected_timestamp_col_2

    actual_timestamp_ntz_col = [row.timestamp_ntz_col for row in actual_collected]
    expected_timestamp_ntz_col = [
        datetime.datetime(
            year=2021, month=1, day=1, hour=2, minute=3, second=4, microsecond=5
        )
        for _ in range(rows)
    ]
    assert actual_timestamp_ntz_col == expected_timestamp_ntz_col
