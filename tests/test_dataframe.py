import datetime
import zoneinfo
from string import ascii_lowercase, digits

from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dataframe_faker.constraints import (
    ArrayConstraint,
    DateConstraint,
    FloatConstraint,
    IntegerConstraint,
    StringConstraint,
    StructConstraint,
    TimestampConstraint,
)
from dataframe_faker.dataframe import (
    ALPHABET,
    _check_dtype_and_constraint_match,
    convert_schema_string_to_schema,
    generate_fake_value,
)

from .helpers import assert_schema_equal, is_valid_email

UUID_ALPHABET = ascii_lowercase + digits + "-"


def test_convert_schema_string_to_schema(spark: SparkSession) -> None:
    schema_str = (
        "id: int not null, str_col: string, struct_col: struct<arr: array<float>>"
    )

    actual = convert_schema_string_to_schema(schema=schema_str, spark=spark)
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


def test_check_dtype_and_constraint_match() -> None:
    dtypes = [
        ArrayType(elementType=IntegerType()),
        BooleanType(),
        DateType(),
        FloatType(),
        IntegerType(),
        StringType(),
        StructType(),
        TimestampType(),
    ]
    constraints = [
        ArrayConstraint(),
        None,
        DateConstraint(),
        FloatConstraint(),
        IntegerConstraint(),
        StringConstraint(),
        StructConstraint(),
        TimestampConstraint(),
    ]
    for dtype, constraint in zip(dtypes, constraints):
        assert _check_dtype_and_constraint_match(dtype=dtype, constraint=constraint)

    assert not _check_dtype_and_constraint_match(
        dtype=ArrayType(elementType=IntegerType()),
        constraint=IntegerConstraint(),
    )
    assert not _check_dtype_and_constraint_match(
        dtype=ArrayType(elementType=IntegerType()),
        constraint=StructConstraint(),
    )
    assert not _check_dtype_and_constraint_match(
        dtype=StructType(),
        constraint=IntegerConstraint(),
    )
    assert not _check_dtype_and_constraint_match(
        dtype=StructType(),
        constraint=ArrayConstraint(),
    )
    assert not _check_dtype_and_constraint_match(
        dtype=IntegerType(),
        constraint=StringConstraint(),
    )
    assert not _check_dtype_and_constraint_match(
        dtype=IntegerType(),
        constraint=StructConstraint(),
    )

    # only checks top-level
    assert _check_dtype_and_constraint_match(
        dtype=ArrayType(elementType=StringType()),
        constraint=ArrayConstraint(element_constraint=IntegerConstraint()),
    )

    # works with fields inside StructType as well
    assert _check_dtype_and_constraint_match(
        dtype=StructType(fields=[StructField(name="asd", dataType=StringType())]),
        constraint=StructConstraint(),
    )

    assert not _check_dtype_and_constraint_match(dtype=StringType(), constraint=None)


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

        actual_bool = generate_fake_value(
            dtype=BooleanType(), nullable=False, fake=fake
        )
        assert isinstance(actual_bool, bool)

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
                    hour=1,
                    minute=1,
                    second=1,
                    tzinfo=zoneinfo.ZoneInfo("UTC"),
                ),
                max_value=datetime.datetime(
                    year=2020,
                    month=1,
                    day=1,
                    hour=1,
                    minute=1,
                    second=10,
                    tzinfo=zoneinfo.ZoneInfo("UTC"),
                ),
                tzinfo=zoneinfo.ZoneInfo("UTC"),
            ),
        )
        assert isinstance(actual_timestamp, datetime.datetime)
        assert actual_timestamp >= datetime.datetime(
            year=2020,
            month=1,
            day=1,
            hour=3,
            minute=1,
            second=1,
            tzinfo=zoneinfo.ZoneInfo("Europe/Helsinki"),
        )
        assert actual_timestamp <= datetime.datetime(
            year=2020,
            month=1,
            day=1,
            hour=3,
            minute=1,
            second=10,
            tzinfo=zoneinfo.ZoneInfo("Europe/Helsinki"),
        )
