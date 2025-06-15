import datetime
import json
import zoneinfo
from decimal import Decimal

import pytest
from pyspark.sql import Row, SparkSession

from dataframe_faker import (
    DATASOURCE_API_SUPPORTED,
    DataframeFaker,
)
from dataframe_faker.dataframe import ALPHABET

from .helpers import assert_schema_equal


@pytest.mark.skipif(
    not DATASOURCE_API_SUPPORTED,
    reason="Data Source API not supported with current package versions.",
)
def test_datasource_read(spark: SparkSession) -> None:
    spark.dataSource.register(DataframeFaker)

    schema_str = """
    array_col array<integer>,
    binary_col binary,
    boolean_col boolean,
    byte_col byte,
    date_col date,
    daytimeinterval_col_1 interval day,
    daytimeinterval_col_2 interval hour to second,
    decimal_col_1 decimal(1,0),
    decimal_col_2 decimal(28,10),
    double_col double,
    float_col float,
    integer_col integer,
    long_col long,
    short_col short,
    string_col string,
    struct_col struct<
        nested_integer integer,
        nested_string string
    >,
    timestamp_col_1 timestamp,
    timestamp_col_2 timestamp,
    timestamp_ntz_col timestamp_ntz
    """
    rows = 100

    constraints = {
        "array_col": {
            "element_constraint": {"min_value": 1, "max_value": 1},
            "min_length": 2,
            "max_length": 2,
        },
        "binary_col": {"min_length": 4, "max_length": 4},
        "boolean_col": {"true_chance": 1.0},
        "byte_col": {"min_value": 1, "max_value": 1},
        "date_col": {
            "min_value": "2020-01-01",
            "max_value": "2020-01-01",
        },
        "daytimeinterval_col_1": {
            "min_value": "2 days",
            "max_value": "50 hours",
        },
        "daytimeinterval_col_2": {
            "min_value": "3601 seconds",
            "max_value": "3601 seconds",
        },
        "decimal_col_1": {"min_value": "2.1", "max_value": "2.1"},
        "decimal_col_2": {"min_value": "1111.2222", "max_value": "1111.2222"},
        "double_col": {"min_value": 1.0, "max_value": 1.0},
        "float_col": {"min_value": 1.0, "max_value": 1.0},
        "integer_col": {"min_value": 1, "max_value": 1},
        "long_col": {"min_value": 30000000005, "max_value": 30000000005},
        "short_col": {"min_value": 1, "max_value": 1},
        "string_col": {"string_type": "any", "min_length": 5, "max_length": 5},
        "struct_col": {
            "element_constraints": {
                "nested_integer": {"min_value": 1, "max_value": 1},
                "nested_string": {"null_chance": 1.0},
            }
        },
        "timestamp_col_1": {
            "min_value": "2020-01-01T02:03:04.000005Z",
            "max_value": "2020-01-01T02:03:04.000005Z",
        },
        "timestamp_col_2": {
            "min_value": "2020-01-01T02:03:04.000005+02:00",
            "max_value": "2020-01-01T02:03:04.000005+02:00",
        },
        "timestamp_ntz_col": {
            "min_value": "2021-01-01T02:03:04.000005",
            "max_value": "2021-01-01T02:03:04.000005",
        },
    }

    # Then check that constraints actually work
    actual = (
        spark.read.format("dataframe-faker")
        .schema(schema_str)
        .option("constraints", json.dumps(constraints))
        .option("rows", rows)
        .load()
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
