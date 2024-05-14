<<<<<<< HEAD
#!/usr/bin/env python3
=======
#!/usr/bin/python3
>>>>>>> main

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
<<<<<<< HEAD
    parser.add_argument(
        "-p", "--print", action="store_true", help="Read the entire parquet file as a table and print it." 
    )

    args = parser.parse_args()
    file = args.file
    print_table = args.print
=======

    args = parser.parse_args()
    file = args.file
>>>>>>> main

    parquet_file = pq.ParquetFile(file)
    print("="*60)
    print(parquet_file.metadata)
    print("="*60)
    print(parquet_file.schema)
    print("="*60)

<<<<<<< HEAD
    if print_table:
        table = pq.read_table(file)
        df = table.to_pandas()
        print(df.head())
        print("="*60)

=======
    table = pq.read_table(file)
    df = table.to_pandas()
    print(df.head())
    print("="*60)
>>>>>>> main
    print("\nDone!\n")
