# DataFrame Faker

![CI badge](https://github.com/VillePuuska/dataframe-faker/actions/workflows/tests.yaml/badge.svg)
![PyPI - Version](https://img.shields.io/pypi/v/dataframe-faker)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dataframe-faker)

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

# Using dictionaries to specify constraints
df = generate_fake_dataframe(
    schema=schema_str,
    constraints={
        "uuid": {
            "string_type": "uuid4",
        },
        "json_message": {
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
    rows=5,
    spark=spark,
)

print(df)
# DataFrame[machine_id: int, uuid: string, json_message: struct<measurement:float,dt:timestamp>]

df.show(truncate=False)
# +----------+------------------------------------+---------------------------------------+
# |machine_id|uuid                                |json_message                           |
# +----------+------------------------------------+---------------------------------------+
# |30        |cc6ccc46-46d3-4ab6-a478-2457b8a99e3c|{60.841892, 2021-09-11 01:06:51.318414}|
# |24        |f193022d-2b33-46df-bc90-2770c712030e|{61.441093, 2024-02-09 05:56:04.528636}|
# |99        |fcb6cfe7-d0d4-4ad1-a4a1-4d62c0e0a135|{79.858315, 2022-03-13 01:05:24.035785}|
# |64        |b8cac746-0a16-4515-8ab4-14816738b570|{75.20326, 2022-11-07 09:15:46.34579}  |
# |39        |00b28bb7-2a75-4d98-8f18-202a73784e16|{89.13032, 2024-03-12 02:59:50.30243}  |
# +----------+------------------------------------+---------------------------------------+

# Using internal Constraint-types to specify constraints
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
# |59        |5013a622-6c2a-490e-a77d-62a8370358ca|{90.77279, 2025-01-15 11:11:44.65589}  |
# |52        |b79269ae-78fd-45f4-87fc-e8d4cfa73151|{29.733383, 2025-01-21 14:26:07.605592}|
# |23        |addf81d3-0e84-453a-a714-580427d8f48b|{95.43238, 2025-01-06 01:09:28.575261} |
# |77        |7008346a-bbfe-45f1-93de-8de25503c670|{32.72988, 2025-01-04 01:43:53.019097} |
# |69        |d3b3957b-c67b-4ef2-921f-b7e1ef1298cb|{30.71608, 2025-01-31 11:52:12.106597} |
# +----------+------------------------------------+---------------------------------------+
```
