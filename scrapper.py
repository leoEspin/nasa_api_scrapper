import argparse
import asyncio
from api_interface import NeoAPI
from data_processing import process_batch, store_batch


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
    huyparce.add_argument(
        "--dry_run",
        action="store_true",  # This makes it a boolean flag
        help="Enable dry run mode (no actual API calls or file writes)",
    )
    return huyparce.parse_args()


async def batch_task(
    key_file_path: str,
    destination: str,
    batch_size: int,
    request_size: int,
    batch_number: int = 0,
    dry_run_mode: bool = False,
):
    client = NeoAPI(
        key_file_path=key_file_path,
        batch_size=batch_size,
        request_size=request_size,
        dry_run=dry_run_mode
    )
    client.page = batch_number * client.batch_responses
    raw_batch = client.get_batch()
    if not dry_run_mode:
        batch = process_batch(raw_batch)
    else:
        batch = []
    store_batch(batch, destination, batch_number=batch_number, dry_run=dry_run_mode)


# TODO: add checks that parameters  passed make sense
# TODO: add tests
# TODO: add code for final odd-sized batch
async def main():
    tasks = []
    arguments = parcero()
    nbatches = arguments.asteroids // arguments.file_batch_size
    for i in range(nbatches):
        task = asyncio.create_task(
            batch_task(
                arguments.api_key_location,
                arguments.destination,
                arguments.file_batch_size,
                arguments.request_size,
                batch_number=i,
                dry_run_mode=arguments.dry_run
            )
        )
        tasks.append(task)

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
