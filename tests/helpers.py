import json

from pyspark.sql.types import StructType


def assert_schema_equal(
    actual: StructType, expected: StructType, check_column_order: bool = False
) -> None:
    """
    Asserts that the PySpark schemas `actual` and `expected` match.

    If `check_column_order` is False, then top-level fields are first sorted by name.

    NOTE: `check_column_order` only applies to top-level fields. This does not apply to fields
    inside StructType-columns.
    """
    if not check_column_order:
        actual.fields = list(sorted(actual.fields, key=lambda field: field.name))
        expected.fields = list(sorted(expected.fields, key=lambda field: field.name))
    assert (
        actual.jsonValue() == expected.jsonValue()
    ), f"Schema mismatch.\nActual:\n{json.dumps(actual.jsonValue(), indent=2)}\nExpected:\n{json.dumps(expected.jsonValue(), indent=2)}"
