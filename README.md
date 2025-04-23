# Readme

The python script `scrapper.py` pulls data from [NASA's Near Earth Object Web Service API](https://api.nasa.gov), and saves that data locally in Parquet format. There are 3 versions of the script

* the `master` branch  hosts an asyncio-based concurrency-capable version 
* the `serial_version` branch hosts a standard sequential version
* the `multi_proc_version` branch hosts a parallel-processing version based on  the `concurrent.futures` module


### How to use the script
- Create an account at [api.nasa.gov](https://api.nasa.gov) to get an API key
- Store the key in a `.env` file under the key `API_KEY`
- Example of how to use the scrapper:
    ```bash
    python  scrapper.py --destination=data --api_key_location=. --asteroids=20 --file_batch_size=20 --request_size=20
    ```
- The  scrapper has a `dry_run` debugging mode which  only prints  which requests would be made to the API and to what files would the payloads be saved
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
    - The years where close approaches were recorded 


### Notes
* current execution time in cloud shell (Intel(R) Xeon(R) CPU @ 2.20GHz, 4 CPUs), pulling data for 300 asteroids: 
    * sequential version: 13.748s
    * async version: 5.534s
    * multi-process version: 4.910s
* sample payload response from the [API](https://api.nasa.gov):
    ```json
    {
        "links": {
            "next": "http://api.nasa.gov/neo/rest/v1/neo/browse?page=1&size=20&api_key=******",
            "self": "http://api.nasa.gov/neo/rest/v1/neo/browse?page=0&size=20&api_key=******"
        },
        "page": {
            "size": 20,
            "total_elements": 39179,
            "total_pages": 1959,
            "number": 0
        },
        "near_earth_objects": [...]
    }
    ```
    where each of the elements of `near_earth_objects` looks  like this:
    ```json
    {
        "links": {
            "self": "http://api.nasa.gov/neo/rest/v1/neo/2000433?api_key=******"
        },
        "id": "2000433",
        "neo_reference_id": "2000433",
        "name": "433 Eros (A898 PA)",
        "name_limited": "Eros",
        "designation": "433",
        "nasa_jpl_url": "https: //ssd.jpl.nasa.gov/tools/sbdb_lookup.html#/?sstr=2000433",
        "absolute_magnitude_h": 10.41,
        "estimated_diameter": {
            "kilometers": {
                "estimated_diameter_min": 22.0067027115,
                "estimated_diameter_max": 49.2084832235
            },
            "meters": {
                "estimated_diameter_min": 22006.7027114738,
                "estimated_diameter_max": 49208.4832234845
            },
            "miles": {
                "estimated_diameter_min": 13.6743268705,
                "estimated_diameter_max": 30.5767244291
            },
            "feet": {
                "estimated_diameter_min": 72200.4705239119,
                "estimated_diameter_max": 161445.1600989368
            }
        },
        "is_potentially_hazardous_asteroid": False,
        "close_approach_data": [...],
        "orbital_data": {
            "orbit_id": "659",
            "orbit_determination_date": "2021-05-24 17: 55: 05",
            "first_observation_date": "1893-10-29",
            "last_observation_date": "2021-05-13",
            "data_arc_in_days": 46582,
            "observations_used": 9130,
            "orbit_uncertainty": "0",
            "minimum_orbit_intersection": ".148588",
            "jupiter_tisserand_invariant": "4.582",
            "epoch_osculation": "2460800.5",
            "eccentricity": ".2227480169011467",
            "semi_major_axis": "1.45815896084448",
            "inclination": "10.82830761253864",
            "ascending_node_longitude": "304.2718959654088",
            "orbital_period": "643.1403141999031",
            "perihelion_distance": "1.133356943989735",
            "perihelion_argument": "178.9225697371719",
            "aphelion_distance": "1.782960977699224",
            "perihelion_time": "2461088.844738645026",
            "mean_anomaly": "198.5980421063379",
            "mean_motion": ".5597534349061246",
            "equinox": "J2000",
            "orbit_class": {
                "orbit_class_type": "AMO",
                "orbit_class_description": "Near-Earth asteroid orbits similar to that of 1221 Amor",
                "orbit_class_range": "1.017 AU < q (perihelion) < 1.3 AU"
            }
        },
        "is_sentry_object": False
    }
    ```
    and each entry in  `close_approach_data` looks like this:
    ```json
    {
        "close_approach_date": "1900-12-27",
        "close_approach_date_full": "1900-Dec-27 01:30",
        "epoch_date_close_approach": -2177879400000,
        "relative_velocity": {
            "kilometers_per_second": "5.5786191875",
            "kilometers_per_hour": "20083.0290749201",
            "miles_per_hour": "12478.8132604691"
        },
        "miss_distance": {
            "astronomical": "0.3149291693",
            "lunar": "122.5074468577",
            "kilometers": "47112732.928149391",
            "miles": "29274494.7651919558"
        },
        "orbiting_body": "Earth"
    }
    ```