import duckdb


class DuckDBConnectionWrapper:
    """
    Wrapper class for DuckDB connection that provides additional functionality.
    
    Manages database connections, view initialization, and query execution while
    providing a context manager interface.
    
    Args:
        connection (duckdb.DuckDBPyConnection): The DuckDB connection object
        data_dirs (dict): Dictionary mapping data directory names to paths
        root_path (str): Root path for the database files
    
    Attributes:
        views_inited (bool): Flag indicating if views have been initialized
    """

    def __init__(self, connection: duckdb.DuckDBPyConnection, data_dirs: dict, root_path: str):
        self.con = connection
        self.data_dirs = data_dirs
        self.root_path = root_path
        self.views_inited = False

    def __getattr__(self, name):
        """Delegate attribute access to the DuckDB connection if not found in the wrapper."""
        return getattr(self.con, name)
    
    def init_views(self):
        """
        Initializes database views for all configured data directories.
        Creates views only if they haven't been initialized already.
        """
        if not self.views_inited:
            for name, path in self.data_dirs.items():
                self.execute(f"""
                    CREATE VIEW {name} AS 
                    SELECT * 
                    FROM read_parquet('{self.root_path}/{name}/**/*', hive_partitioning=True);
                """)
            self.views_inited = True

    def genex(self, query: str):
        """
        Generator wrapper for executing SQL queries.
        
        Args:
            query (str): SQL query to execute
            
        Yields:
            tuple: Individual rows from the query result
        """
        return gen(self.con.execute(query))

    # Add context management methods
    def __enter__(self):
        # Returning `self` allows `with DuckDBWrapper() as db:`
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Ensure the connection is closed when exiting the context
        self.con.close()
        
def gen(result, chunk_size=1000):
    """
    Creates a generator to fetch rows incrementally from a DuckDB result.
    
    Args:
        result: DuckDB result object from execute()
        chunk_size (int, optional): Number of rows to fetch per chunk. Defaults to 1000.
        
    Yields:
        tuple: Individual rows from the result set
        
    Note:
        This helps manage memory usage when dealing with large result sets
        by fetching data in smaller chunks.
    """
    while True:
        rows = result.fetchmany(chunk_size)
        if not rows:  # Break when no more rows are available
            break
        for row in rows:
            yield row

