from __future__ import annotations
from pydantic import BaseModel
from typing import List
from pyiceberg.table import FileScanTask
from pyiceberg.typedef import Record


class ColumnFilter(BaseModel):
    name: str
    value: str

class TableFile(BaseModel):
    path: str
    format: str
    partition: List[ColumnFilter]
    records: int
    size_bytes: int

    @classmethod
    def from_task(cls, task: FileScanTask) -> TableFile:
        re: Record = task.file.partition
        partition = []
        for key, value in re.__dict__.items():
            partition.append(ColumnFilter(name=key, value=str(value)))
        return cls(            
            path = task.file.file_path,
            format = task.file.file_format,
            partition = partition,
            records = task.file.record_count,
            size_bytes = task.file.file_size_in_bytes
        )