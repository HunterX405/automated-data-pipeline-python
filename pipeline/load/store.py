from pathlib import Path
from logging import Logger, getLogger
import pyarrow.parquet as pq
import pyarrow as pa

log: Logger = getLogger('store')

def to_parquet_file(table: pa.Table | None, filename: str, directory: str = '') -> None:
    if not table:
        log.error(f"No table provided for {filename}")
        return

    directory_path  = Path.cwd() / directory
    # Create the directory if there isn't one
    directory_path.mkdir(parents=True, exist_ok=True)
    filepath = directory_path / f"{filename}.parquet"

    # Output parquet file to directory
    pq.write_table(table, filepath)
    log.info(f"Successfully created {filepath.as_uri()}")