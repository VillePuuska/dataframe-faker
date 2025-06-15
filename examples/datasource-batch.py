import datetime
import json
from typing import cast

from pyspark.sql import SparkSession

from dataframe_faker import DATASOURCE_API_SUPPORTED, DataframeFaker


def get_spark_session():
    builder = cast(SparkSession.Builder, SparkSession.builder)
    return (
        builder.appName(f"MyDataSourceApp-{datetime.datetime.now().isoformat()}")
        .master("local[2]")
        .getOrCreate()
    )


def main() -> None:
    if not DATASOURCE_API_SUPPORTED:
        print(
            "Data Source API is not supported. You need a newer version of PySpark or a newer DBR."
        )
        return

    spark = get_spark_session()
    spark.dataSource.register(DataframeFaker)

    schema_str = """
    machine_id int,
    uuid string,
    json_message struct<
        measurement float,
        dt timestamp
    >
    """

    constraints = {
        "uuid": {"string_type": "uuid4"},
        "json_message": {
            "null_chance": 0.5,
            "element_constraints": {
                "measurement": {"min_value": 25.0, "max_value": 100.0},
                "dt": {
                    "min_value": "2025-01-01T00:00:00.000Z",
                    "max_value": "2025-01-31T23:59:59.999Z",
                },
            },
        },
    }
    df_batch = (
        spark.read.format("dataframe-faker")
        .schema(schema_str)
        .option("rows", 5)
        .option("constraints", json.dumps(constraints))
        .load()
    )

    print(df_batch)
    df_batch.show(truncate=False)


if __name__ == "__main__":
    main()
