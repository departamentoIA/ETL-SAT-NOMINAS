#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Project:        ETL-SAT-NOMINAS
File:           main.py
Author:         Antonio Arteaga
Last Updated:   2025-02-23
Version:        1.0
Description:    DataFrames are obtained from CSV files, they are cleaned and 
                transformed. Finally, all DataFrames are loaded to SQL Server (ETL).
Dependencies:   polars==1.38.1, openpyxl==3.1.5, xlsxwriter==3.2.9, pyarrow==23.0.1,
Usage:          CSV files are requested to run this script.
"""

from pkg.extract import *
from pkg.transform import *
from pkg.load import *
import time


def etl_for_batch(table_name: str, ROOT_DATA_PATH: str) -> None:
    """Perform ETL process for every batch of DataFrame"""
    # 1. Extraction (E)
    reader = extract_from_batch(table_name, ROOT_DATA_PATH)
    batch_count = 0
    step = 0
    n_rows = 0
    while True:
        batches = reader.next_batches(1)    # Extract next batch
        if not batches:
            break                           # End of file

        for df_batch in batches:
            batch_count += 1
            n_rows += df_batch.shape[0]
            # 2. Transformation (T)
            df_trans = transform(df_batch)
            if batch_count == 1:
                print(f"Dimensiones del DataFrame: {df_trans.shape}")
                print(df_trans.schema)
                # df_trans.write_excel(f'{table_name}_clean.xlsx')
                logging.info(f"Dimensiones del DataFrame: {df_trans.shape}")
                logging.info(df_trans.schema)
            # 3. Load to SQL Server (L)
            load_table(df_trans, f'{table_name}', batch_count)
            step += 1
            if step == 1:
                print(f"\nProcesando lote {batch_count}...")
                inicio = time.perf_counter()
            if step == n_lotes:
                fin = time.perf_counter()
                print(
                    f"Tiempo procesando {n_lotes*BATCH_SIZE} filas: {fin - inicio:.4f} s")
                step = 0
                logging.info(
                    f"Filas subidas: {n_rows}")
            """
            if batch_count > 2000000:
                print(f"Sólo {n_rows} registros procesados")
                return
            """
    print(
        f"\nTabla '{table_name}' con {n_rows} registros fue procesada con éxito.")
    logging.info(
        f"\nTabla '{table_name}' con {n_rows} registros fue procesada con éxito.")


def main():
    """E-T-L process."""
    for table_name in TABLES_TO_PROCESS:
        print("\n" + "=" * 25)
        print(f"📊 Procesando Tabla: '{table_name}'")
        logging.info(f"* Procesando Tabla: '{table_name}'")
        print("=" * 25)
        try:
            # get_df_sample(table_name, ROOT_DATA_PATH)
            etl_for_batch(table_name, ROOT_DATA_PATH)

        except Exception as e:
            print(
                f"\n❌ FALLO CRÍTICO para {table_name}.\n")
            print(f"'{e}'")
            print("=" * 25)

    print("\n--- PIPELINE FINALIZADO ---")


if __name__ == '__main__':
    main()
