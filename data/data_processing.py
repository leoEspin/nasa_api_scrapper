import os
from typing import Any, Optional
from datetime import datetime
from collections import Counter
import pyarrow as pa
import pyarrow.parquet as pq
from api_interface import nested_get


main_schema = pa.schema(
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
        pa.field("first_observation_date", pa.timestamp("s"), nullable=False),
        pa.field("last_observation_date", pa.timestamp("s"), nullable=False),
        pa.field("observations_used", pa.int64(), nullable=True),
        pa.field("orbital_period", pa.float64(), nullable=True),
        pa.field("close_approach_miss_distance", pa.float64(), nullable=True),
        pa.field("close_approach_date", pa.timestamp("s"), nullable=True),
        pa.field("close_approach_speed", pa.float64(), nullable=True),
        pa.field("very_close_approaches", pa.int64(), nullable=True),
        pa.field(
            "close_approach_years",
            pa.list_(pa.string()),
            nullable=True,
        ),
    ]
)


def to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        print(f"Warning: Could not convert '{value}' to int. Setting to None.")
        return None


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        print(f"Warning: Could not convert '{value}' to int. Setting to None.")
        return None


def process_batch(
    obj: dict[str, Any], table_schema: pa.Schema = main_schema
) -> pa.Table:
    # pulling closest approach data for further processing.
    # minimizing a high-press double, so ok assuming uniqueness
    close_data = [
        (
            min(
                item["close_approach_data"],
                key=lambda x: nested_get(
                    x, ["miss_distance", "astronomical"], default=float("inf")
                ),
            )
            if len(item["close_approach_data"]) > 0
            else {}
        )
        for item in obj
    ]

    # TODO: reduce number of scans of the data
    data = {
        "id": [to_int(item.get("id")) for item in obj],
        "neo_reference_id": [to_int(item.get("neo_reference_id")) for item in obj],
        "name": [item.get("name") for item in obj],
        "name_limited": [item.get("name_limited") for item in obj],
        "designation": [to_int(item.get("designation")) for item in obj],
        "nasa_jpl_url": [item.get("nasa_jpl_url") for item in obj],
        "absolute_magnitude_h": [
            to_float(item.get("absolute_magnitude_h")) for item in obj
        ],
        "is_potentially_hazardous_asteroid": [
            item.get("is_potentially_hazardous_asteroid") for item in obj
        ],
        "estimated_diameter_min": [
            to_float(
                nested_get(
                    item, ["estimated_diameter", "meters", "estimated_diameter_min"]
                )
            )
            for item in obj
        ],
        "estimated_diameter_max": [
            nested_get(item, ["estimated_diameter", "meters", "estimated_diameter_max"])
            for item in obj
        ],
        "first_observation_date": [
            datetime.strptime(
                nested_get(item, ["orbital_data", "first_observation_date"]), "%Y-%m-%d"
            )
            for item in obj
        ],
        "last_observation_date": [
            datetime.strptime(
                nested_get(item, ["orbital_data", "last_observation_date"]), "%Y-%m-%d"
            )
            for item in obj
        ],
        "observations_used": [
            to_int(nested_get(item, ["orbital_data", "observations_used"]))
            for item in obj
        ],
        "orbital_period": [
            to_float(nested_get(item, ["orbital_data", "orbital_period"]))
            for item in obj
        ],
        "close_approach_miss_distance": [
            to_float(nested_get(x, ["miss_distance", "kilometers"])) for x in close_data
        ],
        "close_approach_date": [
            (
                datetime.strptime(x.get("close_approach_date_full"), "%Y-%b-%d %H:%M")
                if x.get("close_approach_date_full") is not None
                else None
            )
            for x in close_data
        ],
        "close_approach_speed": [
            to_float(nested_get(x, ["relative_velocity", "kilometers_per_second"]))
            for x in close_data
        ],
        "very_close_approaches": [
            (
                sum(
                    [
                        float(nested_get(x, ["miss_distance", "astronomical"])) < 0.2
                        for x in item["close_approach_data"]
                    ]
                )
                if len(item["close_approach_data"]) > 0
                else None
            )
            for item in obj
        ],
        "close_approach_years": [
            (
                [
                    x.get("close_approach_date").split("-")[0]
                    for x in item["close_approach_data"]
                    if x.get("close_approach_date") is not None
                ]
                if len(item["close_approach_data"]) > 0
                else []
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
