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
    def partitions(self) -> Sequence[InputPartition]:
        raise NotImplementedError

    def read(
        self, partition: InputPartition
    ) -> Iterator[tuple] | Iterator[RecordBatch]:
        raise NotImplementedError


@final
class DataframeFaker(DataSource):
    @classmethod
    def name(cls) -> str:
        """The name of the datasource."""
        return "dataframe-faker"

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self.options = options

    def schema(self) -> StructType | str:
        """
        Raises a value error
        """
        raise NotImplementedError

    def simpleStreamReader(self, schema: StructType) -> SimpleDataSourceStreamReader:
        return DataframeFakerStreamReader(
            schema=schema,
            constraints=json.loads(self.options.get("constraints", "{}")),
            rows=int(self.options.get("rows", 100)),
        )

    def reader(self, schema: StructType) -> DataSourceReader:
        raise NotImplementedError
