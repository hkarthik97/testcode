import pandas as pd
import json
import sys
import os

def convert_to_parquet(input_path, output_path):
    print(f"Reading {input_path}...")
    try:
        with open(input_path, 'r') as f:
            data = json.load(f)
        
        # Ensure data is a list of records
        if isinstance(data, dict):
            data = [data]
            
        df = pd.DataFrame(data)
        
        # Explicitly cast to match Redshift INT (int32) if columns exist
        # Redshift 'INT' is 4 bytes (int32), Pandas defaults to int64
        if 'id' in df.columns:
            df['id'] = df['id'].astype('int32')
        if 'age' in df.columns:
            df['age'] = df['age'].astype('int32')
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'])
        
        print(f"Writing to {output_path}...")
        df.to_parquet(output_path, engine='pyarrow')
        print("Done.")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python convert_parquet.py <input_json> <output_parquet>")
        sys.exit(1)
        
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    convert_to_parquet(input_path, output_path)
