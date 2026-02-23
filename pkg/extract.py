# extract.py
"""This file calls 'globals.py'."""
from pkg.globals import *


def get_file_paths(table_name: str, root_path: Path) -> Optional[Path]:
    """Obtain the full path and file extension."""
    file_path_csv = root_path / f"{table_name}.csv"
    file_path_txt = root_path / f"{table_name}.txt"
    if file_path_csv.exists():
        return file_path_csv
    if file_path_txt.exists():
        return file_path_txt
    return None


def extract_from_batch(table_name: str, root_path: Path) -> pl.DataFrame:
    """Read files and construct DataFrames."""
    file_path = get_file_paths(table_name, root_path)
    if not file_path:
        raise FileNotFoundError(
            f"No se encontró el archivo para '{table_name}'.")

    df = pl.read_csv_batched(
        file_path,
        separator='|',
        has_header=True,
        encoding="utf8-lossy",
        ignore_errors=True,         # Useful if there are damaged rows
        truncate_ragged_lines=True,
        infer_schema_length=0,
        batch_size=BATCH_SIZE
    )
    return df


def get_df_sample(table_name: str, root_path: Path) -> None:
    """Read file and construct a sample of the DataFrames."""
    file_path = get_file_paths(table_name, root_path)
    if not file_path:
        raise FileNotFoundError(
            f"No se encontró el archivo para '{table_name}'.")

    df = pl.read_csv(
        file_path,
        separator='|',
        has_header=True,
        encoding="utf8-lossy",      # Avoid errors caused by unusual characters
        ignore_errors=True,         # Useful if there are damaged rows
        low_memory=True,            # Reduce RAM usage
        n_rows=1000
    )

    try:
        df.write_excel(f'{table_name}_sample_raw.xlsx')
        print(f"La muestra de la tabla '{table_name}' se guardó exitosamente.")
        print(df.schema)
    except Exception as e:
        print(
            f"\nNo puedo escribir si el archivo está abierto")
        print(f"'{e}'")
