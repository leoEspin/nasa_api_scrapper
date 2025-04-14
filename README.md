# Readme

The python script `scrapper.py` pulls data from [NASA's Near Earth Object Web Service API](https://api.nasa.gov), and saves that data locally in Parquet format.

### How to use the script
- Create an account at [api.nasa.gov](https://api.nasa.gov) to get an API key
- Store the key in a `.env` file under the key `API_KEY`
- Example of how to use the scrapper:
    ```bash
    python  scrapper.py --destination=data --api_key_location=. --asteroids=20 --file_batch_size=20 --request_size=20
    ```
- The  scrapper has a `dry_run` debugging mode which  only prints  which requests would be made to the API and to what files would
    the payloads be saved
- The script stores the following columns in the parquet file(s):
    - id
    - neo_reference_id
    - name
    - name_limited
    - designation
    - nasa_jpl_url
    - absolute_magnitude_h
    - is_potentially_hazardous_asteroid
    - minimum estimated diameter in meters
    - maximum estimated diameter in meters
    - **closest** approach miss distance in kilometers
    - **closest** approach date
    - **closest** approach relative velocity in kilometers per second
    - first observation date
    - last observation date
    - observations used
    - orbital period
- The script stores also the following aggregated columns:
    - The total number of times our 200 near earth objects approached closer than 0.2 astronomical units (found as miss_distance.astronomical)
    - The number of close approaches recorded in each year present in the data
