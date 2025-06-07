from typing import TYPE_CHECKING, Iterator, Sequence, final

from pyspark.sql.types import StructType

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
    def __init__(self, options: dict) -> None:
        raise NotImplementedError

    def initialOffset(self) -> dict:
        raise NotImplementedError

    def read(self, start: dict) -> tuple[Iterator[tuple], dict]:
        raise NotImplementedError

    def readBetweenOffsets(self, start: dict, end: dict) -> Iterator[tuple]:
        raise NotImplementedError

    def commit(self, end: dict) -> None:
        raise NotImplementedError


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
    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        raise NotImplementedError

    def schema(self) -> StructType | str:
        raise NotImplementedError

    def simpleStreamReader(self, schema: StructType) -> SimpleDataSourceStreamReader:
        raise NotImplementedError

    def reader(self, schema: StructType) -> DataSourceReader:
        raise NotImplementedError
