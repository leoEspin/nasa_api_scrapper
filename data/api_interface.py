import os
from dotenv import load_dotenv
from typing import Any, Optional
import requests
from urllib.parse import urlencode


def nested_get(d: dict[str, Any], keys: list[str], default: Any = None):
    for key in keys:
        if isinstance(d, dict) and key in d:
            d = d[key]
        else:
            return default
    return d


class NeoAPI:
    """
    class for interacting with the NASA Neo API
    To use the class first instanciate it:
        client = NeoAPI(key_file_path, batch_size, request_size, page)
    the  class assumes that the api key is stored in a .env file with the name API_KEY

    A batch,  of size batch_size is  formed by doing a series of requests of size request_size
    This  is  useful to have separate control of the number of asteroids' data stored in a single file.

    The initialization parameter page allows to skip a number of pages of size request_size. 
    this is useful for enabling making parallel calls to the API, and storing the corresponding data in separate files.
    """

    base_url = "https://api.nasa.gov/neo/rest/v1/neo/browse"
    # top level keys to navigate/pull relevant portion of payload
    response_key_to_keep = "near_earth_objects"
    max_pages_keys = ["page", "total_pages"]

    def __init__(
        self,
        key_file_path: Optional[str] = None,
        batch_size: int = 100,
        request_size: int = 20,
        page: int = 0,
    ):  
        self.key = NeoAPI.get_api_key(key_file_path)
        self.page = page
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

    @property
    def max_pages(self):
        if self._max_pages is None:
            params = {"api_key": self.key, "page": 0, "size": 1}
            out = requests.get(f"{NeoAPI.base_url}?{urlencode(params)}")
            if out.ok:
                response_data = out.json()
                self._max_pages = nested_get(response_data, NeoAPI.max_pages_keys)
            else:
                raise Exception(out.json())
        return self._max_pages

    def _get_mini_batch(self):
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
            batch.extend(self._get_mini_batch())
        return batch
