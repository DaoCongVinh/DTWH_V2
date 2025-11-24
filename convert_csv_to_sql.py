#!/usr/bin/env python3
"""
Convert date_dim.csv to SQL INSERT statements.
This generates a SQL file that can be safely executed during Docker initialization.
"""

import csv
import sys
from pathlib import Path

def convert_csv_to_sql(csv_path, output_path, table_name="DateDim"):
    """Convert CSV file to SQL INSERT statements."""
    
    insert_statements = []
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header
        
        # Collect rows for batch insert (more efficient)
        rows = []
        for row in reader:
            # Escape single quotes in values
            escaped_row = [val.replace("'", "''") for val in row]
            # Create tuple of quoted values
            values_str = ", ".join([f"'{val}'" for val in escaped_row])
            rows.append(f"({values_str})")
        
        # Write SQL file with batch inserts (every 100 rows)
        batch_size = 100
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            insert_sql = f"INSERT INTO {table_name} VALUES\n" + ",\n".join(batch) + ";"
            insert_statements.append(insert_sql)
    
    # Write to output file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("USE dbStaging;\n\n")
        f.write("\n\n".join(insert_statements))
        f.write("\n")
    
    print(f"✓ Converted {len(rows)} rows from {csv_path}")
    print(f"✓ Generated {len(insert_statements)} INSERT batches")
    print(f"✓ Wrote to {output_path}")

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    csv_file = script_dir / "init_db" / "date_dim.csv"
    sql_file = script_dir / "init_db" / "05_load_datedim.sql"
    
    if not csv_file.exists():
        print(f"Error: {csv_file} not found", file=sys.stderr)
        sys.exit(1)
    
    convert_csv_to_sql(csv_file, sql_file)
