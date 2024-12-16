# vastdb_parq_schema_file

Usage:

```bash
pip3 install --upgrade --quiet git+https://github.com/snowch/vastdb_parq_schema_file.git --use-pep517
```

```bash
parquet_checker your-file.parquet
```

E.g.

```bash
$ parquet_checker ~/Downloads/fhvhv_tripdata_2024-01.parquet

Parquet schema:
hvfhs_license_num: large_string
dispatching_base_num: large_string
originating_base_num: large_string
request_datetime: timestamp[us]
on_scene_datetime: timestamp[us]
pickup_datetime: timestamp[us]
dropoff_datetime: timestamp[us]
PULocationID: int32
DOLocationID: int32
trip_miles: double
trip_time: int64
base_passenger_fare: double
tolls: double
bcf: double
sales_tax: double
congestion_surcharge: double
airport_fee: double
tips: double
driver_pay: double
shared_request_flag: large_string
shared_match_flag: large_string
access_a_ride_flag: large_string
wav_request_flag: large_string
wav_match_flag: large_string

Checking column types...
Column 'hvfhs_license_num' has unsupported type: LARGE_STRING
Column 'dispatching_base_num' has unsupported type: LARGE_STRING
Column 'originating_base_num' has unsupported type: LARGE_STRING
Column 'shared_request_flag' has unsupported type: LARGE_STRING
Column 'shared_match_flag' has unsupported type: LARGE_STRING
Column 'access_a_ride_flag' has unsupported type: LARGE_STRING
Column 'wav_request_flag' has unsupported type: LARGE_STRING
Column 'wav_match_flag' has unsupported type: LARGE_STRING
Column type check complete.

Verifying variable length column sizes using DuckDB...
Column 'hvfhs_license_num': 0.01 KB
Column 'dispatching_base_num': 0.01 KB
Column 'originating_base_num': 0.01 KB
Column 'shared_request_flag': 0.00 KB
Column 'shared_match_flag': 0.00 KB
Column 'access_a_ride_flag': 0.00 KB
Column 'wav_request_flag': 0.00 KB
Column 'wav_match_flag': 0.00 KB

Time taken for element size verification: 1.46 seconds
```
