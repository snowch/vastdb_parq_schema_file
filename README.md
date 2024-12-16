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

---

Column too wide example:

```python
import pyarrow as pa
import pyarrow.parquet as pq
import random
import string

def generate_random_string(length):
    letters = string.ascii_uppercase
    return ''.join(random.choice(letters) for i in range(length * 10))

# Create a PyArrow Table
data = {'id': [1],
        'data': [generate_random_string(25600)]}
table = pa.Table.from_pydict(data)

# Write the Table to a Parquet file
pq.write_table(table, 'data.parquet')
```

```bash
$ parquet_checker data.parquet

Parquet schema:
id: int64
data: string

Checking column types...
Column type check complete.

Verifying variable length column sizes using DuckDB...
Column 'data': 250.00 KB [EXCEEDS 126KB]

Time taken for element size verification: 0.01 seconds
```
