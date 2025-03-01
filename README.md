# DataFrame Faker

## What

A simple helper for generating PySpark DataFrames filled with fake data with the help of Faker.

## Why

This tool is built to allow quickly generating fake data for development of data pipelines etc. in situations where you don't have example data in your development environment and you don't want to work in production when iterating on your code.

## How

```python
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
df = generate_fake_dataframe(
    schema=schema_str,
    constraints={
        "uuid": StringConstraint(string_type="uuid4"),
        "json_message": StructConstraint(
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
            }
        ),
    },
    rows=5,
    spark=spark,
)

print(df)
# DataFrame[machine_id: int, uuid: string, json_message: struct<measurement:float,dt:timestamp>]

df.show(truncate=False)
# +----------+------------------------------------+---------------------------------------+
# |machine_id|uuid                                |json_message                           |
# +----------+------------------------------------+---------------------------------------+
# |36        |709e3210-896e-4337-9a8b-7fe2969d99a1|{53.12909, 2025-01-23 21:57:47.177554} |
# |98        |7c80c8e7-de0a-46fd-9bb4-61ce72c541e8|{52.056786, 2025-01-08 21:24:30.353171}|
# |31        |43585e16-8fd4-4e8f-b474-646b6a9678ff|{92.424286, 2025-01-02 21:55:08.503093}|
# |0         |2f00aee0-c7b4-4125-933a-55b87f55f156|{80.54238, 2025-01-10 06:57:59.352183} |
# |65        |531df313-e9b3-4f53-89cc-f95adcab94d4|{94.593094, 2025-01-07 03:56:48.097414}|
# +----------+------------------------------------+---------------------------------------+
```
