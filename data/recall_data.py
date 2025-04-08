import os
from dotenv import load_dotenv
from typing import Any, Optional
from datetime import datetime
import requests
from urllib.parse import urlencode
import pyarrow as pa
import pyarrow.parquet as pq

base_url = f"https://api.nasa.gov/neo/rest/v1/neo/browse"
params = {"api_key": get_api_key(), "page": None, "size": None}


def get_api_key(path: str) -> str:
    load_dotenv(dotenv_path=path)
    api_key = os.getenv("API_KEY")
    if api_key:
        return api_key

    raise ValueError(f"API_KEY not found in {path}.env file.")


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


def nested_get(d: dict[str, Any], keys: list[str], default: Any = None):
    for key in keys:
        if isinstance(d, dict) and key in d:
            d = d[key]
        else:
            return default
    return d


def process_batch(obj: dict[str, Any], table_schema: pa.Schema) -> pa.Table:
    data = {
        "id": [item.get("id") for item in obj],
        "neo_reference_id": [item.get("neo_reference_id") for item in obj],
        "name": [item.get("name") for item in obj],
        "name_limited": [item.get("name_limited") for item in obj],
        "designation": [item.get("designation") for item in obj],
        "nasa_jpl_url": [item.get("nasa_jpl_url") for item in obj],
        "absolute_magnitude_h": [item.get("absolute_magnitude_h") for item in obj],
        "is_potentially_hazardous_asteroid": [
            item.get("is_potentially_hazardous_asteroid") for item in obj
        ],
        "estimated_diameter_min": [
            nested_get(item, ["estimated_diameter", "meters", "estimated_diameter_min"])
            for item in obj
        ],
        "estimated_diameter_max": [
            nested_get(item, ["estimated_diameter", "meters", "estimated_diameter_max"])
            for item in obj
        ],
    }
    table = pa.Table.from_pydict(data, schema=table_schema)
    return table


def store_batch(batch: pa.Table, path: str, batch_number: int) -> None:
    pq.write_table(
        table, f"{path}/nasa_neo_data_{batch_number}.parquet", compression="snappy"
    )


def get_neo_data_batch(base_url: str, params: dict[str, Any], batch_size: int):

    out = requests.get(f"{base_url}?{urlencode(params)}")
    if out.ok:
        return out.json()
