from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from dataframe_faker.dataframe import convert_schema_string_to_schema

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
