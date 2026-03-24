#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL con pausa/reanudacion por fila, sin clases y solo con funciones.

Caracteristicas:
- Reanudacion automatica con checkpoints JSON.
- Reanudacion manual desde una fila con --resume-row.
- Pausa graciosa creando el archivo pause.flag.
- Carga idempotente por fila agregando la columna tecnica _etl_source_row.
  Antes de reanudar, elimina desde esa fila hacia adelante para evitar duplicados.

Uso:
    python main_resume_functions.py
    python main_resume_functions.py --resume-row 250001
    python main_resume_functions.py --resume-table GERG_AECF_1891_Anexo6F --resume-row 250001
"""

from pkg.extract import extract_from_batch
from pkg.transform import transform
from pkg.globals import BATCH_SIZE, n_lotes, LONG_TEXT_COLS, logging
from pkg.config import get_connection_string
import polars as pl
import argparse
import json
import time
from pathlib import Path
from sqlalchemy import create_engine, text


ENGINE = create_engine(
    get_connection_string(),
    fast_executemany=True,
    pool_pre_ping=True,
)

BASE_DIR = Path(__file__).resolve().parent
CHECKPOINT_DIR = BASE_DIR / "checkpoints"
PAUSE_FLAG = BASE_DIR / "pause.flag"
CHECKPOINT_DIR.mkdir(exist_ok=True)
TECHNICAL_ROW_COL = "_etl_source_row"


def parse_args():
    parser = argparse.ArgumentParser(
        description="ETL con pausa/reanudacion por fila")
    parser.add_argument(
        "--resume-row",
        type=int,
        default=None,
        help="Fila de datos desde la que se desea reanudar (1 = primera fila de datos)",
    )
    parser.add_argument(
        "--root-data-path",
        type=Path,
        required=True,
        help="Ruta raíz donde se encuentran las tablas fuente (.csv/.txt)",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        required=True,
        help="Lista de tablas a procesar, separadas por espacio",
    )
    parser.add_argument(
        "--resume-table",
        type=str,
        default=None,
        help="Procesar solo una tabla especifica",
    )
    return parser.parse_args()


def get_checkpoint_path(table_name: str) -> Path:
    safe_name = table_name.replace("/", "_").replace("\\", "_")
    return CHECKPOINT_DIR / f"{safe_name}.json"


def default_checkpoint(table_name: str) -> dict:
    return {
        "table_name": table_name,
        "last_completed_batch": 0,
        "rows_loaded": 0,
        "next_row": 1,
        "status": "new",
        "updated_at": None,
    }


def load_checkpoint(table_name: str) -> dict:
    checkpoint_path = get_checkpoint_path(table_name)
    if not checkpoint_path.exists():
        return default_checkpoint(table_name)

    with checkpoint_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    checkpoint = default_checkpoint(table_name)
    checkpoint.update(payload)
    return checkpoint


def save_checkpoint(table_name: str, last_completed_batch: int, rows_loaded: int, next_row: int, status: str):
    checkpoint_path = get_checkpoint_path(table_name)
    payload = {
        "table_name": table_name,
        "last_completed_batch": last_completed_batch,
        "rows_loaded": rows_loaded,
        "next_row": next_row,
        "status": status,
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    with checkpoint_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def clear_checkpoint(table_name: str):
    checkpoint_path = get_checkpoint_path(table_name)
    if checkpoint_path.exists():
        checkpoint_path.unlink()


def pause_requested() -> bool:
    return PAUSE_FLAG.exists()


def normalize_table_name(table_name: str) -> str:
    return table_name.replace("-", "_")


def map_polars_to_sql(colname: str, dtype: pl.DataType):
    if dtype == pl.Utf8 and colname in LONG_TEXT_COLS:
        return "NVARCHAR(255)"
    if colname in ("EmisorRFC", "ReceptorRFC", "ReceptorBanco"):
        return "NVARCHAR(50)"

    mapping = {
        pl.Int8: "TINYINT",
        pl.Int16: "SMALLINT",
        pl.Int32: "INT",
        pl.Int64: "BIGINT",
        pl.Boolean: "BIT",
        pl.Float32: "REAL",
        pl.Float64: "FLOAT",
        pl.Utf8: "NVARCHAR(255)",
        pl.Date: "DATE",
        pl.Datetime: "DATETIME2",
    }
    return mapping.get(dtype, "NVARCHAR(255)")


def add_source_row_column(df: pl.DataFrame, start_row: int) -> pl.DataFrame:
    row_count = df.height
    return df.with_columns(
        pl.Series(TECHNICAL_ROW_COL, list(
            range(start_row, start_row + row_count)), dtype=pl.Int64)
    )


def create_table_from_df(engine, table_name: str, df: pl.DataFrame):
    full_name_for_object_id = f"dbo.{table_name}"
    full_name_bracket = f"[dbo].[{table_name}]"
    columns_sql = []
    for col, dtype in df.schema.items():
        sql_type = map_polars_to_sql(col, dtype)
        nullability = "NOT NULL" if col == TECHNICAL_ROW_COL else "NULL"
        columns_sql.append(f"[{col}] {sql_type} {nullability}")

    create_sql = f"""
    IF OBJECT_ID(N'{full_name_for_object_id}', 'U') IS NOT NULL
        DROP TABLE {full_name_bracket};

    CREATE TABLE {full_name_bracket} (
        {', '.join(columns_sql)},
        CONSTRAINT PK_{table_name}_{TECHNICAL_ROW_COL} PRIMARY KEY ([{TECHNICAL_ROW_COL}])
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))


def table_exists(engine, table_name: str) -> bool:
    sql = "SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'dbo' AND t.name = :table_name"
    with engine.begin() as conn:
        result = conn.execute(text(sql), {"table_name": table_name}).scalar()
    return result == 1


def delete_rows_from_row(engine, table_name: str, start_row: int):
    sql = text(
        f"DELETE FROM [dbo].[{table_name}] WHERE [{TECHNICAL_ROW_COL}] >= :start_row")
    with engine.begin() as conn:
        conn.execute(sql, {"start_row": start_row})


def load_table_idempotent(engine, df: pl.DataFrame, table_name: str, recreate_table: bool = False):
    table_name = normalize_table_name(table_name)
    if recreate_table or not table_exists(engine, table_name):
        create_table_from_df(engine, table_name, df)
        print(f"Subiendo tabla '{table_name}' a SQL Server...")

    try:
        df.write_database(
            table_name=table_name,
            connection=engine,
            if_table_exists="append",
        )
    except Exception as e:
        print(f"❌ Error: {e}")
        logging.info(f"Error: {e}")
        raise


def get_resume_row(table_name: str, resume_row_arg: int | None) -> int:
    checkpoint = load_checkpoint(table_name)
    checkpoint_next_row = int(checkpoint.get("next_row", 1) or 1)
    if resume_row_arg is not None:
        return max(1, resume_row_arg)
    return max(1, checkpoint_next_row)


def trim_batch_for_resume(df_batch: pl.DataFrame, batch_start_row: int, resume_row: int) -> pl.DataFrame:
    if resume_row <= batch_start_row:
        return df_batch

    offset = resume_row - batch_start_row
    if offset >= df_batch.height:
        return df_batch.clear()

    return df_batch.slice(offset)


def prepare_target_for_resume(engine, table_name: str, resume_row: int):
    sql_table_name = normalize_table_name(table_name)
    if table_exists(engine, sql_table_name) and resume_row > 1:
        print(
            f"Limpiando filas desde la fila {resume_row} en '{sql_table_name}' para evitar duplicados...")
        delete_rows_from_row(engine, sql_table_name, resume_row)


def process_table(table_name: str, root_data_path: Path, resume_row_arg: int | None) -> str:
    reader = extract_from_batch(table_name, root_data_path)
    checkpoint = load_checkpoint(table_name)
    resume_row = get_resume_row(table_name, resume_row_arg)
    current_row_pointer = 1
    batch_count = 0
    completed_batches = 0
    total_rows_loaded_this_run = 0
    step = 0
    prepared_target = False
    first_insert_done = False

    if resume_row > 1:
        print(f"Reanudando '{table_name}' desde la fila {resume_row}.")
        logging.info(f"Reanudando '{table_name}' desde la fila {resume_row}.")
    else:
        print(f"Iniciando '{table_name}' desde la primera fila.")

    while True:
        batches = reader.next_batches(1)
        if not batches:
            break

        for df_batch in batches:
            batch_count += 1
            original_batch_rows = df_batch.height
            batch_start_row = current_row_pointer
            batch_end_row = current_row_pointer + original_batch_rows - 1
            current_row_pointer += original_batch_rows

            if batch_end_row < resume_row:
                completed_batches = batch_count
                continue

            df_work = trim_batch_for_resume(
                df_batch, batch_start_row, resume_row)
            if df_work.height == 0:
                completed_batches = batch_count
                continue

            effective_start_row = max(batch_start_row, resume_row)
            df_trans = transform(df_work)
            df_trans = add_source_row_column(df_trans, effective_start_row)

            if not prepared_target:
                prepare_target_for_resume(ENGINE, table_name, resume_row)
                prepared_target = True

            if not first_insert_done:
                recreate_table = resume_row == 1
                load_table_idempotent(
                    ENGINE, df_trans, table_name, recreate_table=recreate_table)
                first_insert_done = True
                print(f"Dimensiones del DataFrame: {df_trans.shape}")
                print(df_trans.schema)
                logging.info(f"Dimensiones del DataFrame: {df_trans.shape}")
                logging.info(df_trans.schema)
            else:
                load_table_idempotent(
                    ENGINE, df_trans, table_name, recreate_table=False)

            completed_batches = batch_count
            total_rows_loaded_this_run += df_trans.height
            next_row = effective_start_row + df_trans.height
            total_rows_effective = next_row - 1
            save_checkpoint(
                table_name=table_name,
                last_completed_batch=completed_batches,
                rows_loaded=total_rows_effective,
                next_row=next_row,
                status="running",
            )

            step += 1
            if step == 1:
                print(
                    f"\n  Procesando lote {batch_count} (filas {effective_start_row}-{next_row - 1})...")
                inicio = time.perf_counter()
            if step == n_lotes:
                fin = time.perf_counter()
                print(
                    f"Tiempo procesando {n_lotes * BATCH_SIZE} filas: {fin - inicio:.4f} s")
                step = 0
                logging.info(f"Filas subidas: {total_rows_effective}")

            if pause_requested():
                save_checkpoint(
                    table_name=table_name,
                    last_completed_batch=completed_batches,
                    rows_loaded=total_rows_effective,
                    next_row=next_row,
                    status="paused",
                )
                message = (
                    f"Pausa solicitada. '{table_name}' quedo en la fila {next_row} "
                    f"despues del lote {batch_count}. Para reanudar la carga, borre el archivo pause.flag"
                )
                print(f"\n{message}")
                logging.info(message)
                return "paused"

    final_next_row = max(resume_row, checkpoint.get(
        "next_row", 1)) + total_rows_loaded_this_run
    final_rows = final_next_row - \
        1 if total_rows_loaded_this_run > 0 else int(
            checkpoint.get("rows_loaded", 0))
    print(
        f"\nTabla '{table_name}' procesada con exito. Filas cargadas hasta: {final_rows}.")
    logging.info(
        f"Tabla '{table_name}' procesada con exito. Filas cargadas hasta: {final_rows}.")
    clear_checkpoint(table_name)
    return "completed"


def get_tables_to_process(tables: list[str], resume_table: str | None):
    if resume_table:
        return [resume_table]
    return tables


def main():
    args = parse_args()

    if args.resume_row is not None and args.resume_row < 1:
        raise ValueError("--resume-row debe ser mayor o igual a 1")

    if not args.root_data_path.exists():
        raise FileNotFoundError(f"No existe la ruta: {args.root_data_path}")

    if pause_requested():
        print(
            "Se encontro 'pause.flag'. El proceso no iniciara hasta eliminar ese archivo.")
        return

    root_data_path = args.root_data_path
    tables_to_process = get_tables_to_process(args.tables, args.resume_table)

    for table_name in tables_to_process:
        print("\n" + "=" * 25)
        print(f"Procesando tabla: '{table_name}'")
        logging.info(f"* Procesando tabla: '{table_name}'")
        print("=" * 25)
        try:
            status = process_table(table_name, root_data_path, args.resume_row)
            if status == "paused":
                print("\n--- PIPELINE PAUSADO ---")
                return
        except Exception as e:
            checkpoint = load_checkpoint(table_name)
            save_checkpoint(
                table_name=table_name,
                last_completed_batch=int(
                    checkpoint.get("last_completed_batch", 0)),
                rows_loaded=int(checkpoint.get("rows_loaded", 0)),
                next_row=int(checkpoint.get("next_row", 1)),
                status="failed",
            )
            print(f"\nFallo critico para {table_name}.")
            print(f"'{e}'")
            print("=" * 25)

    print("\n--- PIPELINE FINALIZADO ---")


if __name__ == "__main__":
    main()
