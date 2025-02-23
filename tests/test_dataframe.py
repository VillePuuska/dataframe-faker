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
    _check_dtype_and_constraint_match,
    convert_schema_string_to_schema,
)

from .helpers import assert_schema_equal


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


# TODO: test `generate_fake_value`
