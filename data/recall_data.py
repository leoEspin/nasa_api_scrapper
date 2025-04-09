import argparse
from api_interface import NeoAPI
from data_processing import process_batch, schema, store_batch


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
