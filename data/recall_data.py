from typing import Any, Optional
from datetime import datetime
import pyarrow as pa

schema = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("neo_reference_id", pa.int64(), nullable=False),
        pa.field("name", pa.string(), nullable=True),
        pa.field("name_limited", pa.float64(), nullable=False),
        pa.field("designation"),
    ]
)


def nested_get(d: dict[str, Any], keys: list[str], default: Any = None):
    for key in keys:
        if isinstance(d, dict) and key in d:
            d = d[key]
        else:
            return default
    return d


def process_rows(obj: dict[str, Any]):
    data_dict = {
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
    return data_dict