import argparse
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
import re
import time

bold_on = "\033[1m"
red = "\033[31m"
yellow = "\033[33m"
reset = "\033[0m"

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
        print(f"\n{bold_on}Checking column types...{reset}")

        def check_type(field, path=""):
            column_name = path + field.name if path else field.name
            column_type = field.type
            type_name = re.sub(r"\[.*?\]", "", str(column_type).upper())

            if column_type.num_fields > 0:
                for i in range(column_type.num_fields):
                    nested_field = column_type.field(i)
                    new_path = column_name + "." + nested_field.name
                    check_type(nested_field, new_path)
            else:
                if type_name not in self.allowed_types:
                    print(f"{red}Column '{column_name}' has unsupported type: {type_name}{reset}")

        for field in self.schema:
            check_type(field)

        print("Column type check complete.")

    def print_parquet_schema(self):
        print(f"{bold_on}Parquet schema:{reset}")
        print(self.schema)

    def check_element_sizes(self):
        """
        Check element sizes using DuckDB, measuring sizes in bytes.
        """
        start_time = time.time()
        print(f"\n{bold_on}Verifying variable length column sizes using DuckDB...{reset}")

        conn = duckdb.connect()
        conn.execute(f"CREATE TABLE parquet_table AS SELECT * FROM '{self.file_path}'")

        fields = []
        max_size_queries = []
        has_nested = False
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
                max_size_queries.append(f"-1 AS max_{column_name}")
                fields.append(column_name)
                has_nested = True

        if len(max_size_queries) > 0:
            query = f"SELECT {', '.join(max_size_queries)} FROM parquet_table"

            result = conn.execute(query).fetchone()  # Execute the query and fetch results
            conn.close()

            # Display the results
            for i, max_size in enumerate(result):
                column_name = fields[i]
                size_kb = max_size / 1024 if max_size else 0
                flag = f" {red}[EXCEEDS 126KB]{reset}" if size_kb > 126 else ""
                print(f"Column '{column_name}': {size_kb:.2f} KB{flag}")

            if has_nested:
                print(f"{yellow}Checking size of nested columns is not supported.{reset}")


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
