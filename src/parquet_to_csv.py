
# Requirements: pip install pandas pyarrow
# Usage: python parquet_to_csv.py input.parquet output.csv

import pandas as pd
import argparse

def parquet_to_csv(input_file, output_file):
    # Read the Parquet file
    df = pd.read_parquet(input_file)
    
    # Write the DataFrame to a CSV file
    df.to_csv(output_file, index=False)
    print(f"Conversion complete. CSV file saved as {output_file}")

if __name__ == "__main__":
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description="Convert Parquet file to CSV")
    parser.add_argument("input", help="Input Parquet file path")
    parser.add_argument("output", help="Output CSV file path")
    args = parser.parse_args()

    # Call the conversion function
    parquet_to_csv(args.input, args.output)

