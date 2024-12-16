import argparse
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import time
from tqdm import tqdm  # Import tqdm for the progress bar

class ParquetChecker:
    def __init__(self, file_path):
        self.file_path = file_path
        self.schema = pq.read_schema(file_path)
        self.allowed_types = [
            'UINT8', 'UINT16', 'UINT32', 'UINT64',
            'INT8', 'INT16', 'INT32', 'INT64',
            'STRING', 'LIST', 'STRUCT', 'MAP',
            'BOOL', 'FLOAT', 'DOUBLE', 'BINARY',
            'DECIMAL128', 'DATE32', 'TIMESTAMP',
            'TIME32', 'TIME64', 'NA'
        ]

    def check_column_types(self):
        print("\nChecking column types...")
        for i in range(len(self.schema)):
            field = self.schema[i]
            column_name = field.name
            column_type = field.type
            type_name = str(column_type).upper()

            if type_name not in self.allowed_types:
                print(f"Column '{column_name}' has unsupported type: {type_name}")
        print("Column type check complete.")

    def print_parquet_schema(self):
        print("Parquet schema:")
        print(self.schema)

    def check_element_sizes(self):
        print("\nVerifying overall element sizes (in KB):")
        start_time = time.time()  # Start timing
        dataset = ds.dataset(self.file_path, format='parquet')
        max_sizes = np.zeros(len(self.schema), dtype=np.int64)

        # Set up the progress bar
        total_batches = sum(1 for _ in dataset.to_batches())  # Count batches for progress bar
        pbar = tqdm(total=total_batches, desc="Processing batches", unit="batch")

        for batch in dataset.to_batches():
            for i, column in enumerate(batch.columns):
                # Calculate the size in bytes for each element in the column
                column_type = self.schema[i].type  # Get the schema type for the column
                if column_type.equals(pa.binary()) or column_type.equals(pa.string()):
                    # Binary and string types: measure the length of each element
                    sizes = column.to_pylist()
                    sizes = np.array([len(v) if v is not None else 0 for v in sizes], dtype=np.int64)
                elif pa.types.is_list(column_type) or pa.types.is_struct(column_type) or pa.types.is_map(column_type):
                    # Complex types: approximate size by encoding as a UTF-8 string
                    sizes = column.to_pylist()
                    sizes = np.array([len(str(v).encode('utf-8')) if v is not None else 0 for v in sizes], dtype=np.int64)
                else:
                    # Numeric or other primitive types: calculate size directly
                    sizes = np.array([v.nbytes if hasattr(v, 'nbytes') else len(str(v).encode('utf-8')) for v in column.to_pylist()], dtype=np.int64)

                max_sizes[i] = max(max_sizes[i], sizes.max())
            pbar.update(1)  # Update progress bar for each batch

        pbar.close()  # Close the progress bar
        print()

        for i, max_size in enumerate(max_sizes):
            column_name = self.schema[i].name
            size_kb = max_size / 1024
            flag = " [EXCEEDS 126KB]" if size_kb > 126 else ""
            print(f"Column '{column_name}': {size_kb:.2f} KB{flag}")
        
        end_time = time.time()  # End timing
        elapsed_time = end_time - start_time
        print(f"\nTime taken for element size verification: {elapsed_time:.2f} seconds")


def main():
    parser = argparse.ArgumentParser(description="Check Parquet file schema and element sizes.")
    parser.add_argument('file_path', help="Path to the Parquet file")
    parser.add_argument('--check-element-sizes', action='store_true', 
                        help="Check the sizes of elements in the Parquet file")
    args = parser.parse_args()

    checker = ParquetChecker(args.file_path)
    checker.print_parquet_schema()
    checker.check_column_types()

    if args.check_element_sizes:
        checker.check_element_sizes()

if __name__ == "__main__":
    main()
