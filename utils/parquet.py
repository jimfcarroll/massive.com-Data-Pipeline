import datetime
import os
import shutil

import pyarrow as pa
import pyarrow.parquet as pq

from db.connection import DuckDBConnectionWrapper


def update_parquet_file(file_path, new_data, sort_key=None, schema=None, unique_field : str = None):
    """
    Updates or creates a Parquet file with new data.

    Args:
        file_path (str): Full path to an existing or new Parquet file
        new_data (dict or list[dict]): New data to append. Can be a single record (dict) or multiple records (list/tuple of dicts)
        sort_key (str, optional): Column name to sort data by before writing
        schema (pyarrow.Schema, optional): Schema to enforce for the Parquet file. Required if new_data is empty
        unique_field (str, optional): Field name to deduplicate records on

    Raises:
        ValueError: If new_data is empty and no schema is provided
        ValueError: If schemas don't match between existing file and new data
    """
    # Ensure the parent directory exists
    parent_dir = os.path.dirname(file_path)
    os.makedirs(parent_dir, exist_ok=True)

    # Temporary file path
    temp_file_path = f"{file_path}.tmp"

    # Ensure new_data is a list of records if it's a single record
    new_data = [new_data] if isinstance(new_data, dict) else new_data

    # Check if new_data is empty
    if not new_data:
        if schema is None:
            raise ValueError("Cannot have both new_data an empty list and yet define no schema.")
        # Create an empty PyArrow table with the given schema
        new_table = pa.Table.from_arrays(
            [pa.array([], type=field.type) for field in schema],
            schema=schema
        )
    else:
        # Flatten list of records into a dictionary of columns
        flattened_data = {key: [record[key] for record in new_data] for key in new_data[0]}

        # Convert new data to PyArrow Table using the schema
        if schema:
            new_table = pa.table(flattened_data, schema=schema)
        else:
            new_table = pa.table(flattened_data)

    # If the file exists, read and combine with new data
    if os.path.exists(file_path):
        with pq.ParquetFile(file_path) as parquet_file:
            existing_table_schema = parquet_file.schema_arrow

        # Validate schema consistency
        if schema and existing_table_schema != schema:
            raise ValueError(
                f"Schema mismatch: Existing schema {existing_table_schema} does not match predefined schema {schema} for {file_path}."
            )
        elif existing_table_schema != new_table.schema:
            raise ValueError(
                f"Schema mismatch: Existing schema {existing_table_schema} does not match new data schema {new_table.schema} for {file_path}."
            )

        existing_table = pq.read_table(file_path, schema=schema if schema else new_table.schema)
        combined_table = pa.concat_tables([existing_table, new_table])

    else:
        combined_table = new_table

    if unique_field is not None:
        print(f"Deduping based on {unique_field}. Starting count {combined_table.num_rows}")
        original_schema = combined_table.schema
        # Convert the table to a pandas DataFrame for deduplication
        combined_df = combined_table.to_pandas()

        # Remove duplicates based on the unique field
        deduplicated_df = combined_df.drop_duplicates(subset=unique_field, keep='first')

        # Reset the index to avoid the '__index_level_0__' column
        deduplicated_df.reset_index(drop=True, inplace=True)

        # Convert back to a PyArrow Table, preserving the original schema
        combined_table = pa.Table.from_pandas(deduplicated_df, schema=original_schema)
        print(f"Deduping based on {unique_field}. Ending count {combined_table.num_rows}", flush=True)

    # Optional sorting by the specified key(s)
    if sort_key:
        if isinstance(sort_key, str):
            sort_key = [(sort_key, "ascending")]  # Sort by this key in ascending order
        else:
            sort_key = [(key, "ascending") for key in sort_key]  # Sort all specified keys in ascending order
        combined_table = combined_table.sort_by(sort_key)
    try:
        # Remove any existing temporary file
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            print(f"Removed existing temporary file: {temp_file_path}")

        pq.write_table(combined_table, temp_file_path)
        print(f"Data written to temporary file: {temp_file_path}")
    except Exception as e:
        # Clean up the temporary file if something goes wrong
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            print(f"Cleaned up temporary file: {temp_file_path}")
        print(f"Error while saving Parquet file: {e}")
        raise

    # Safely rename the temporary file to the final file path
    if os.path.exists(file_path):
        os.remove(file_path)  # Remove the existing file
    # Move the temporary file to the final destination atomically
    os.rename(temp_file_path, file_path)  # Rename the temp file to the final file
    print(f"Temporary file moved to: {file_path}")

def validate_parquet_files(root_dir: str, schema):
    """
    Validates all Parquet files in a directory tree against a schema and identifies directories containing invalid files.
    
    Recursively walks through the directory tree starting from root_dir, checking each .parquet file's schema
    against the provided schema. If a file's schema doesn't match, its parent directory is marked for deletion.
    
    Args:
        root_dir (str): Root directory to recursively search for .parquet files
        schema (pyarrow.Schema): Reference schema to validate against
        
    Returns:
        list[str]: List of directory paths containing files with mismatched schemas that should be deleted.
                  Each directory appears only once, even if it contains multiple invalid files.
    """
    # Walk the directory tree and gather .parquet files
    all_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for file in filenames:
            if file.endswith(".parquet"):
                all_files.append(os.path.join(dirpath, file))

    # Sort files lexicographically
    all_files.sort()

    # Track directories already marked for deletion
    dirs_to_delete_set = set()
    dirs_to_delete_list = []

    # Validate each .parquet file
    for file_path in all_files:
        parent_dir = os.path.dirname(file_path)

        # Skip files in directories that have already been marked for deletion
        if parent_dir in dirs_to_delete_set:
            continue

        # Read only the schema of the parquet file without inferring hive partitioning
        parquet_file = pq.ParquetFile(file_path)
        file_schema = parquet_file.schema_arrow

        if file_schema != schema:
            dirs_to_delete_set.add(parent_dir)
            dirs_to_delete_list.append(parent_dir)

    return dirs_to_delete_list

def correct_parquet_files(root_dir: str, schema):
    """
    Validates and corrects schema mismatches in Parquet files under a directory tree.

    Recursively walks through the directory tree starting from root_dir, checking each .parquet file's schema
    against the provided schema. If a schema mismatch is found, attempts to rewrite the file by casting columns
    to match the target schema types. Only attempts casts that are safe and lossless, like int64 to float64.

    Args:
        root_dir (str): Root directory to recursively search for .parquet files
        schema (pyarrow.Schema): Reference schema to validate and correct files against

    Raises:
        ValueError: If a file is missing required columns or contains incompatible column types
    """
    # Walk the directory tree and gather .parquet files
    all_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for file in filenames:
            if file.endswith(".parquet"):
                all_files.append(os.path.join(dirpath, file))

    # Sort files lexicographically
    all_files.sort()

    # Validate each .parquet file
    for file_path in all_files:
        # Read only the schema of the parquet file without inferring hive partitioning
        parquet_file = pq.ParquetFile(file_path)
        file_schema = parquet_file.schema_arrow

        if file_schema != schema:
            try:
                rewrite_parquet_file(file_path, schema)
            except Exception as e:
                print(f"Failed to rewrite file {file_path}: {e}")

def rewrite_parquet_file(file_path: str, schema: pa.Schema):
    """
    Rewrites a Parquet file to match the provided schema by safely casting columns to the target types.

    This function reads the existing Parquet file, attempts to cast each column to match the target schema,
    and writes the transformed data back to the same file location. It only performs safe casts like
    int64 to float64 or null to string, and will raise an error for unsafe casts.

    Args:
        file_path (str): The full path of the parquet file to rewrite
        schema (pyarrow.Schema): The target schema to conform the file to

    Raises:
        ValueError: If a required column is missing or if a column cannot be safely cast to the target type
    
    Note:
        The function uses a temporary file during rewriting to ensure atomicity. If the process fails,
        the original file remains unchanged.
    """
    # Read the table from the existing file
    table = pq.ParquetFile(file_path).read()

    # Attempt to cast columns to match the schema
    casted_columns = []
    for field in schema:
        column_name = field.name
        if column_name in table.column_names:
            original_column = table[column_name]
            if original_column.type != field.type:
                if pa.types.is_int64(original_column.type) and pa.types.is_float64(field.type):
                    casted_columns.append(original_column.cast(field.type))
                elif pa.types.is_null(original_column.type) and pa.types.is_string(field.type):
                    casted_columns.append(original_column.cast(field.type))
                elif pa.types.is_large_string(original_column.type) and pa.types.is_string(field.type):
                    casted_columns.append(original_column.cast(field.type))
                else:
                    raise ValueError(f"Cannot cast column {column_name} from {original_column.type} to {field.type}")
            else:
                casted_columns.append(original_column)
        else:
            raise ValueError(f"Missing column {column_name} in file {file_path}")

    # Create a new table with the casted columns
    new_table = pa.Table.from_arrays(casted_columns, schema=schema)

    # Write to a temporary file
    temp_file_path = f"{file_path}.tmp"
    pq.write_table(new_table, temp_file_path)

    # Remove the original file
    os.remove(file_path)

    # Replace the original file with the temporary file
    shutil.move(temp_file_path, file_path)

def parquet_file_exists(con: DuckDBConnectionWrapper, data_type: str, file_date: datetime.date) -> bool:
    """
    Checks if a Parquet file exists for the specified date and data type in the database.

    The function constructs the expected file path based on the data type and date,
    using a year-based directory structure (e.g., year=2023/2023-01-01.parquet).

    Args:
        con (DuckDBConnectionWrapper): Database connection wrapper containing the 
            configured data directories.
        data_type (str): Type of data to check. Valid values are:
            - 'stocks_day': Daily stock data
            - 'stocks_min': Minute-level stock data
            - 'options_day': Daily options data  
            - 'options_min': Minute-level options data
        file_date (datetime.date): Date to check for file existence.

    Returns:
        bool: True if the Parquet file exists at the expected location,
              False if either the file or parent directories don't exist.

    Raises:
        ValueError: If the data_type directory is not configured or doesn't exist.
    """
    # Get the data directory for the specified data type
    data_dir = con.data_dirs.get(data_type)
    if not data_dir or not os.path.exists(data_dir):
        raise ValueError(f"Data directory for {data_type} does not exist: {data_dir}")

    # Construct the year-based subdirectory and the expected file name
    year_dir = os.path.join(data_dir, f"year={file_date.year}")
    file_name = f"{file_date.strftime('%Y-%m-%d')}.parquet"
    file_path = os.path.join(year_dir, file_name)

    # Check if the file exists
    return os.path.exists(file_path)

