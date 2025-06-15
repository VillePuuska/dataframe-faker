import datetime

from pyspark.sql import SparkSession

from dataframe_faker import (
    FloatConstraint,
    StringConstraint,
    StructConstraint,
    TimestampConstraint,
    generate_fake_dataframe,
)

spark = (
    SparkSession.builder.appName("dataframe-faker-example")
    .config("spark.sql.session.timeZone", "UTC")
    .master("local[4]")
    .getOrCreate()
)

schema_str = """
machine_id: int,
uuid: string,
json_message: struct<
    measurement: float,
    dt: timestamp
>
"""

# Using dictionaries to specify constraints
df = generate_fake_dataframe(
    schema=schema_str,
    constraints={
        "uuid": {
            "string_type": "uuid4",
        },
        "json_message": {
            "null_chance": 0.5,
            "element_constraints": {
                "measurement": {
                    "min_value": 25.0,
                    "max_value": 100.0,
                },
                "dt": {
                    "min_value": datetime.datetime.fromisoformat(
                        "2025-01-01T00:00:00.000Z"
                    ),
                    "max_value": datetime.datetime.fromisoformat(
                        "2025-01-31T23:59:59.999Z"
                    ),
                },
            },
        },
    },
    rows=5,
    spark=spark,
)
print(df)
df.show(truncate=False)

# Using internal Constraint-types to specify constraints
df = generate_fake_dataframe(
    schema=schema_str,
    constraints={
        "uuid": StringConstraint(string_type="uuid4"),
        "json_message": StructConstraint(
            null_chance=0.5,
            element_constraints={
                "measurement": FloatConstraint(min_value=25.0, max_value=100.0),
                "dt": TimestampConstraint(
                    min_value=datetime.datetime.fromisoformat(
                        "2025-01-01T00:00:00.000Z"
                    ),
                    max_value=datetime.datetime.fromisoformat(
                        "2025-01-31T23:59:59.999Z"
                    ),
                ),
            },
        ),
    },
    rows=5,
    spark=spark,
)
print(df)
df.show(truncate=False)
