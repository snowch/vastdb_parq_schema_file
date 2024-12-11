import argparse
import pyarrow.parquet as pq
import pyarrow.dataset as ds

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
        print("Checking column types...")
        for i in range(len(self.schema)):
            field = self.schema[i]
            column_name = field.name
            column_type = field.type
            type_name = str(column_type).upper()

            if type_name not in self.allowed_types:
                print(f"Column '{column_name}' has a non-matching type: {type_name}")
        print("Column type check complete.")

    def print_parquet_schema(self):
        print("Parquet schema:")
        print(self.schema)

    def check_element_sizes(self):
        print("\nVerifying overall element sizes (in KB):")
        dataset = ds.dataset(self.file_path, format='parquet')
        max_sizes = [0] * len(self.schema)

        for batch in dataset.to_batches():
            for i in range(len(self.schema)):
                column = batch.column(i)
                column_data = column.to_pandas()
                for value in column_data:
                    size_bytes = value.nbytes if hasattr(value, 'nbytes') else len(str(value).encode('utf-8'))
                    max_sizes[i] = max(max_sizes[i], size_bytes)

        for i in range(len(self.schema)):
            column_name = self.schema[i].name
            size_kb = max_sizes[i] / 1024
            flag = " [EXCEEDS 126KB]" if size_kb > 126 else ""
            print(f"Column '{column_name}': {size_kb:.2f} KB{flag}")

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
