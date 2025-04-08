import os
from typing import Any, Optional
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from  .api_interface import nested_get



schema = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("neo_reference_id", pa.int64(), nullable=False),
        pa.field("name", pa.string(), nullable=True),
        pa.field("name_limited", pa.string(), nullable=True),
        pa.field("designation", pa.int64(), nullable=True),
        pa.field("nasa_jpl_url", pa.string(), nullable=True),
        pa.field("absolute_magnitude_h", pa.float64(), nullable=True),
        pa.field("is_potentially_hazardous_asteroid", pa.bool_(), nullable=True),
        pa.field("estimated_diameter_min", pa.float64(), nullable=True),
        pa.field("estimated_diameter_max", pa.float64(), nullable=True),
    ]
)


def process_batch(obj: dict[str, Any], table_schema: pa.Schema) -> pa.Table:
    def to_int(value: Any):
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            print(f"Warning: Could not convert '{value}' to int. Setting to None.")
            return None

    data = {
        "id": [to_int(item.get("id")) for item in obj],
        "neo_reference_id": [to_int(item.get("neo_reference_id")) for item in obj],
        "name": [item.get("name") for item in obj],
        "name_limited": [item.get("name_limited") for item in obj],
        "designation": [to_int(item.get("designation")) for item in obj],
        "nasa_jpl_url": [item.get("nasa_jpl_url") for item in obj],
        "absolute_magnitude_h": [item.get("absolute_magnitude_h") for item in obj],
        "is_potentially_hazardous_asteroid": [
            item.get("is_potentially_hazardous_asteroid") for item in obj
        ],
        "estimated_diameter_min": [
            NeoAPI.nested_get(
                item, ["estimated_diameter", "meters", "estimated_diameter_min"]
            )
            for item in obj
        ],
        "estimated_diameter_max": [
            NeoAPI.nested_get(
                item, ["estimated_diameter", "meters", "estimated_diameter_max"]
            )
            for item in obj
        ],
    }
    table = pa.Table.from_pydict(data, schema=table_schema)
    return table


def store_batch(batch: pa.Table, destination_path: str, batch_number: int) -> None:
    pq.write_table(
        batch,
        os.path.join(destination_path, f"nasa_neo_data_{batch_number}.parquet"),
        compression="snappy",
    )