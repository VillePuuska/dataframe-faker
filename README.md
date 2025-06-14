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
# DataFrame[machine_id: int, uuid: string, json_message: struct<measurement:float,dt:timestamp>]

df.show(truncate=False)
# +----------+------------------------------------+--------------------------------------+
# |machine_id|uuid                                |json_message                          |
# +----------+------------------------------------+--------------------------------------+
# |38        |084346b4-6590-43e9-95cc-74385a69d327|{94.84935, 2025-01-02 02:27:17.327557}|
# |81        |e3b813e6-320c-44de-8be6-3b74482e08cc|NULL                                  |
# |77        |a7d4d98c-cd46-4a0a-8988-3e3290ae70d5|{90.33641, 2025-01-13 18:12:17.554614}|
# |26        |abffcb3e-1cf5-4c3a-bbdd-152402d42b5d|{76.68725, 2025-01-03 16:05:33.269761}|
# |21        |e258bae8-f6ad-48b5-b279-a7a1fce8611f|{48.459206, 2025-01-25 07:51:53.3191} |
# +----------+------------------------------------+--------------------------------------+

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
# DataFrame[machine_id: int, uuid: string, json_message: struct<measurement:float,dt:timestamp>]

df.show(truncate=False)
# +----------+------------------------------------+---------------------------------------+
# |machine_id|uuid                                |json_message                           |
# +----------+------------------------------------+---------------------------------------+
# |35        |d94f34b4-8922-4070-98af-fcc71d6ae056|NULL                                   |
# |83        |5958e3fb-ceca-4074-92e7-5982eb797a15|{95.187164, 2025-01-30 18:40:31.879228}|
# |99        |2439e420-49f5-4181-ab24-b16027cef872|NULL                                   |
# |96        |105f2b78-72f9-449c-863c-6d30442d9fc1|{56.999123, 2025-01-11 16:15:54.228193}|
# |98        |b95835ae-96f6-42b3-9902-a5877c0460d2|NULL                                   |
# +----------+------------------------------------+---------------------------------------+
```
