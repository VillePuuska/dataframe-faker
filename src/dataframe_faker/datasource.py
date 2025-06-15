import json
from typing import TYPE_CHECKING, Any, Iterator, Sequence, final

from faker import Faker
from pyspark.sql import Row
from pyspark.sql.types import StructType

from .constraints import Constraint
from .dataframe import _generate_fake_rows

DATASOURCE_API_SUPPORTED = True

if TYPE_CHECKING:
    from pyarrow import RecordBatch
    from pyspark.sql.datasource import (
        DataSource,
        DataSourceReader,
        InputPartition,
        SimpleDataSourceStreamReader,
    )
else:
    try:
        from pyarrow import RecordBatch
        from pyspark.sql.datasource import (
            DataSource,
            DataSourceReader,
            InputPartition,
            SimpleDataSourceStreamReader,
        )
    except ImportError:
        DATASOURCE_API_SUPPORTED = False

        class DataSourceAPINotAvailable(Exception):
            pass

        class Dummy:
            def __init__(self, *args, **kwargs):
                raise DataSourceAPINotAvailable(
                    "To use the DataSource API you need a new enough version of PySpark and PyArrow."
                )

        DataSource = Dummy
        DataSourceReader = Dummy
        SimpleDataSourceStreamReader = Dummy
        InputPartition = Dummy
        RecordBatch = Dummy


@final
class DataframeFakerStreamReader(SimpleDataSourceStreamReader):
    def __init__(
        self,
        schema: StructType,
        constraints: dict[str, Constraint | dict[str, Any] | None] | None = None,
        rows: int = 5,
        fake: Faker | None = None,
    ) -> None:
        self.schema = schema
        self.constraints = constraints or {}
        self.rows = rows
        self.fake = fake or Faker()

    def initialOffset(self) -> dict:
        return {"offset": 0}

    def read(self, start: dict) -> tuple[Iterator[tuple], dict]:
        data = _generate_fake_rows(
            schema=self.schema,
            constraints=self.constraints,
            rows=self.rows,
            fake=self.fake,
        )
        # Returning a `Row` is actually allowed, but the PySpark API typing is 'incorrect'.
        return iter([Row(**row) for row in data]), {
            "offset": start.get("offset", 0) + self.rows
        }  # pyrefly: ignore

    def readBetweenOffsets(self, start: dict, end: dict) -> Iterator[tuple]:
        data = _generate_fake_rows(
            schema=self.schema,
            constraints=self.constraints,
            rows=end.get("offset", 0) - start.get("offset", 0) + 1,
            fake=self.fake,
        )
        # Returning a `Row` is actually allowed, but the PySpark API typing is 'incorrect'.
        return iter([Row(**row) for row in data])  # pyrefly: ignore

    def commit(self, end: dict) -> None:
        pass


@final
class DataframeFakerReader(DataSourceReader):
    def __init__(
        self,
        schema: StructType,
        constraints: dict[str, Constraint | dict[str, Any] | None] | None = None,
        rows: int = 100,
        fake: Faker | None = None,
    ) -> None:
        self.schema = schema
        self.constraints = constraints or {}
        self.rows = rows
        self.fake = fake or Faker()

    def partitions(self) -> Sequence[InputPartition]:
        return [InputPartition(None)]

    def read(
        self, partition: InputPartition
    ) -> Iterator[tuple] | Iterator[RecordBatch]:
        data = _generate_fake_rows(
            schema=self.schema,
            constraints=self.constraints,
            rows=self.rows,
            fake=self.fake,
        )
        # Returning a `Row` is actually allowed, but the PySpark API typing is 'incorrect'.
        return iter([Row(**row) for row in data])  # pyrefly: ignore


@final
class DataframeFaker(DataSource):
    # TODO: docstring with example usage.
    @classmethod
    def name(cls) -> str:
        """The name of the datasource."""
        return "dataframe-faker"

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self.options = options

    def schema(self) -> StructType | str:
        """
        This Data Source does not allow a 'default' schema. If a schema is not provided, this
        method will be called and raises an error to indicate that a schema must be provided.

        Raises
        -------
        ValueError
            If no schema is provided in the options.
        """
        raise ValueError(
            "A schema must be provided; DataframeFaker does not support a default schema."
        )

    def simpleStreamReader(self, schema: StructType) -> SimpleDataSourceStreamReader:
        return DataframeFakerStreamReader(
            schema=schema,
            constraints=json.loads(self.options.get("constraints", "{}")),
            rows=int(self.options.get("rows", 100)),
        )

    def reader(self, schema: StructType) -> DataSourceReader:
        return DataframeFakerReader(
            schema=schema,
            constraints=json.loads(self.options.get("constraints", "{}")),
            rows=int(self.options.get("rows", 100)),
        )
