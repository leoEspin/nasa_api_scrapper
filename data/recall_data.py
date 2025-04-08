import os
from dotenv import load_dotenv
from typing import Any, Optional
from datetime import datetime
import requests
from urllib.parse import urlencode
import argparse
import pyarrow as pa
import pyarrow.parquet as pq


class NeoAPI:
    """class for interacting with the NASA Neo API"""

    base_url = "https://api.nasa.gov/neo/rest/v1/neo/browse"
    # top level keys to navigate/pull relevant portion of payload
    response_key_to_keep = "near_earth_objects"
    max_pages_keys = ["page", "total_pages"]

    def __init__(
        self,
        key_file_path: Optional[str] = None,
        batch_size: int = 100,
        request_size: int = 20,
    ):
        self.key = NeoAPI.get_api_key(key_file_path)
        self.page = 0
        self.request_size = request_size
        self.batch_size = batch_size
        self._params = {
            "api_key": self.key,
            "page": self.page,
            "size": self.request_size,
        }
        self._max_pages = None

    @property
    def batch_responses(self):
        return self.batch_size // self.request_size

    @staticmethod
    def get_api_key(path: Optional[str]) -> str:
        if path is not None:
            if not path.endswith(".env"):
                path = os.path.join(path, ".env")
            load_dotenv(dotenv_path=path)
        else:
            load_dotenv()

        api_key = os.getenv("API_KEY")
        if api_key:
            return api_key

        raise ValueError(f"API_KEY not found in {path}.env file.")

    @staticmethod
    def nested_get(d: dict[str, Any], keys: list[str], default: Any = None):
        for key in keys:
            if isinstance(d, dict) and key in d:
                d = d[key]
            else:
                return default
        return d

    @property
    def max_pages(self):
        if self._max_pages is None:
            params = {"api_key": self.key, "page": 0, "size": 1}
            out = requests.get(f"{NeoAPI.base_url}?{urlencode(params)}")
            if out.ok:
                response_data = out.json()
                self._max_pages = NeoAPI.nested_get(
                    response_data, NeoAPI.max_pages_keys
                )
            else:
                raise Exception(out.json())
        return self._max_pages

    def get_mini_batch(self):
        if self.page > self.max_pages:
            raise ValueError("Maximum number of available pages reached.")

        self._params["page"] = self.page
        out = requests.get(f"{NeoAPI.base_url}?{urlencode(self._params)}")
        if out.ok:
            self.page += 1
            return out.json().get(NeoAPI.response_key_to_keep)
        raise Exception(out.json())

    def get_batch(self):
        batch = []
        for _ in range(self.batch_responses):
            batch.extend(self.get_mini_batch())
        return batch


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


def parcero():
    """defines command line arguments"""
    huyparce = argparse.ArgumentParser()
    huyparce.add_argument(
        "--destination",
        type=str,
        help="Directory to store data in",
        default="data",
        metavar="data_dir",
    )
    huyparce.add_argument(
        "--api_key_location",
        type=str,
        help="Directory where .env file with api key is stored",
        default="~/.env",
        metavar="dir_name",
    )
    huyparce.add_argument(
        "--asteroids",
        type=int,
        help="Number of asteroids to pull data for",
        default=200,
    )
    huyparce.add_argument(
        "--file_batch_size",
        type=int,
        help="Number of asteroids data to store in a single file",
        default=100,
    )
    huyparce.add_argument(
        "--request_size",
        type=int,
        help="Number of asteroids to pull data for in a single request",
        default=20,
    )
    return huyparce.parse_args()


if __name__ == "__main__":
    arguments = parcero()
    client = NeoAPI(
        key_file_path=arguments.api_key_location,
        batch_size=arguments.file_batch_size,
        request_size=arguments.request_size,
    )
    nbatches = arguments.asteroids // client.batch_size
    for i in range(nbatches):
        raw_batch = client.get_batch()
        batch = process_batch(raw_batch, schema)
        store_batch(batch, arguments.destination, i)
