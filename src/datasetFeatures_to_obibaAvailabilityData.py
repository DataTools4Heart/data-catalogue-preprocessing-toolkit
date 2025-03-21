import pandas as pd
import argparse
import sys

BASE_COLUMNS = ['pid', 'encounterId', 'referenceTimePoint', 'eventTime', 'exitTime']

def validate_dictionary(parquet_columns, xls_path):
    """Validate Parquet columns against XLS dictionary"""
    try:
        dict_df = pd.read_excel(xls_path, sheet_name='Variables')
        dict_columns = dict_df['name'].tolist()
        
        # Get columns to validate (non-base columns)
        parquet_cols_to_check = [col for col in parquet_columns if col not in BASE_COLUMNS]
        
        missing_in_dict = set(parquet_cols_to_check) - set(dict_columns)
        missing_in_parquet = set(dict_columns) - set(parquet_cols_to_check)
        
        if missing_in_dict or missing_in_parquet:
            print("Validation errors:")
            if missing_in_dict:
                print(f"- Columns missing in XLS: {', '.join(missing_in_dict)}")
            if missing_in_parquet:
                print(f"- Columns missing in Parquet: {', '.join(missing_in_parquet)}")
            print("Proceeding with transformation despite validation errors.")
        else:
            print("Validation successful: All columns match")
    except Exception as e:
        print(f"Validation error: {str(e)}")
        print("Proceeding with transformation despite validation errors.")

def transform_data(input_parquet, output_csv, dictionary_path=None):
    """Main transformation function"""
    # Load Parquet file
    df = pd.read_parquet(input_parquet)
    
    # Perform validation if dictionary is provided
    if dictionary_path:
        validate_dictionary(df.columns.tolist(), dictionary_path)
    
    # Apply transformation: Convert non-base variables to 0 (if empty) or 1 (if filled)
    for col in df.columns:
        if col not in BASE_COLUMNS:
            df[col] = df[col].notna().astype(int)
    
    # Save to CSV
    df.to_csv(output_csv, index=False)
    print(f"Transformation completed. Output saved to {output_csv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Transform Parquet data to availability CSV',
        usage='%(prog)s input.parquet output.csv [--dictionary metadata.xls]'
    )
    parser.add_argument('input_parquet', help='Path to input Parquet file')
    parser.add_argument('output_csv', help='Path to output CSV file')
    parser.add_argument('--dictionary', help='Optional OBiBa dictionary XLS file')
    
    if len(sys.argv) == 1 or '-h' in sys.argv or '--help' in sys.argv:
        parser.print_help()
        sys.exit(1)
    
    args = parser.parse_args()
    
    transform_data(args.input_parquet, args.output_csv, args.dictionary)

