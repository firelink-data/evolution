#!/usr/bin/env python3

import argparse
import pyarrow.parquet as pq

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="verify-parquet",
        description="Print metadata and some rows from a .parquet file.",
        epilog="Happy hacking! 👋🥳",
    )

    parser.add_argument(
        "file", help="The .parquet file to verify.",
    )
    parser.add_argument(
        "-p", "--print", action="store_true", help="Read the entire parquet file as a table and print it." 
    )

    args = parser.parse_args()
    file = args.file
    print_table = args.print

    parquet_file = pq.ParquetFile(file)
    print("="*60)
    print(parquet_file.metadata)
    print("="*60)
    print(parquet_file.schema)
    print("="*60)

    if print_table:
        table = pq.read_table(file)
        df = table.to_pandas()
        print(df.head())
        print("="*60)

    print("\nDone!\n")
