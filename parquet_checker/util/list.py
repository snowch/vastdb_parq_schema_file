import pyarrow as pa
import pyarrow.parquet as pq
import random
import string

def generate_random_string(length):
    letters = string.ascii_uppercase
    return ''.join(random.choice(letters) for i in range(length))

# Create a list of two 200KB random strings
list_data = [generate_random_string(20000), generate_random_string(20000)]

# Create a PyArrow Table
data = {'id': [1],
        'data': [list_data]}

table = pa.Table.from_pydict(data)

# Write the Table to a Parquet file
pq.write_table(table, 'data.parquet')
