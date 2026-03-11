# extract.py
"""This file calls 'globals.py'."""
from pkg.globals import *
import codecs


def get_file_paths(table_name: str, root_path: Path) -> Optional[Path]:
    """Obtain the full path and file extension."""
    file_path_csv = root_path / f"{table_name}.csv"
    file_path_txt = root_path / f"{table_name}.txt"
    if file_path_csv.exists():
        return file_path_csv
    if file_path_txt.exists():
        return file_path_txt
    return None


def normalize_to_utf8_streaming(
    src: Path,
    src_encoding: str = "windows-1252",
    chunk_size: int = 8 * 1024 * 1024,  # 8 MB
) -> Path:
    """Normalize all file to utf8 (by chunks)"""
    dst = src.with_suffix(src.suffix + ".utf8")
    if dst.exists():
        return dst

    decoder = codecs.getincrementaldecoder(src_encoding)(errors="replace")
    with src.open("rb") as f_in, dst.open("w", encoding="utf8", errors="replace", newline="") as f_out:
        while True:
            chunk = f_in.read(chunk_size)
            if not chunk:
                break
            text = decoder.decode(chunk)
            f_out.write(text)

        # Vaciar el decoder (por si quedó estado interno)
        tail = decoder.decode(b"", final=True)
        if tail:
            f_out.write(tail)

    print("Archivo convertido a utf8 correctamente!")
    return dst


def extract_from_batch(table_name: str, root_path: Path):
    """Read files and construct DataFrames."""
    file_path = get_file_paths(table_name, root_path)
    if not file_path:
        raise FileNotFoundError(
            f"No se encontró el archivo para '{table_name}'.")

    utf8_path = normalize_to_utf8_streaming(Path(file_path), "windows-1252")
    df = pl.read_csv_batched(
        utf8_path,
        separator='|',
        has_header=True,
        encoding="utf8",
        ignore_errors=True,         # Useful if there are damaged rows
        truncate_ragged_lines=True,
        infer_schema_length=0,
        batch_size=BATCH_SIZE,
        quote_char=None,   # <- clave: desactiva parsing de comillas
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
        truncate_ragged_lines=True,
        n_rows=100
    )

    try:
        df.write_excel(f'{table_name}_sample_raw.xlsx')
        print(f"La muestra de la tabla '{table_name}' se guardó exitosamente.")
        print(df.schema)
    except Exception as e:
        print(
            f"\nNo puedo escribir si el archivo está abierto")
        print(f"'{e}'")
