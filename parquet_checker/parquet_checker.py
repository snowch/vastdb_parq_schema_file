import argparse
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
import re
import time


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

            type_name = re.sub(r"\[.*?\]", "", type_name)

            if type_name not in self.allowed_types:
                print(f"Column '{column_name}' has unsupported type: {type_name}")
        print("Column type check complete.")

    def print_parquet_schema(self):
        print("Parquet schema:")
        print(self.schema)

    def check_element_sizes(self):
        """
        Check element sizes using DuckDB, measuring sizes in bytes.
        """
        start_time = time.time()
        print("\nVerifying variable length column sizes using DuckDB...")

        conn = duckdb.connect()
        conn.execute(f"CREATE TABLE parquet_table AS SELECT * FROM '{self.file_path}'")

        fields = []
        max_size_queries = []
        for field in self.schema:
            column_name = field.name
            column_type = field.type

            if column_type.equals(pa.binary()) or column_type.equals(pa.large_binary()):
                max_size_queries.append(f"MAX(OCTET_LENGTH({column_name})) AS max_{column_name}")
                fields.append(column_name)
            elif column_type.equals(pa.string()) or column_type.equals(pa.large_string()) or column_type.equals(pa.large_utf8()):
                max_size_queries.append(f"MAX(STRLEN({column_name})) AS max_{column_name}")
                fields.append(column_name)
            elif pa.types.is_list(column_type) or pa.types.is_struct(column_type) or pa.types.is_map(column_type):
                continue # not yet implemented
                # max_size_queries.append(f"MAX(STRLEN(TO_JSON({column_name}))) AS max_{column_name}")
                # fields.append(column_name)

        if len(max_size_queries) > 0:
            query = f"SELECT {', '.join(max_size_queries)} FROM parquet_table"

            result = conn.execute(query).fetchone()  # Execute the query and fetch results
            conn.close()

            # Display the results
            for i, max_size in enumerate(result):
                column_name = fields[i]
                size_kb = max_size / 1024 if max_size else 0
                flag = " [EXCEEDS 126KB]" if size_kb > 126 else ""
                print(f"Column '{column_name}': {size_kb:.2f} KB{flag}")

            elapsed_time = time.time() - start_time
            print(f"\nTime taken for element size verification: {elapsed_time:.2f} seconds")


def main():
    parser = argparse.ArgumentParser(description="Check Parquet file schema and element sizes.")
    parser.add_argument('file_path', help="Path to the Parquet file")
    args = parser.parse_args()

    checker = ParquetChecker(args.file_path)
    checker.print_parquet_schema()
    checker.check_column_types()
    checker.check_element_sizes()

if __name__ == "__main__":
    main()
