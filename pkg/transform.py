# transform.py
"""This file calls 'globals.py' and 'config.py'."""
from pkg.globals import *
from typing import Iterable, Mapping


def cast_columns(df: pl.DataFrame, columns: Iterable[str], dtype: pl.DataType
                 ) -> pl.DataFrame:
    """Cast the specified columns to the specified type.
    Apply the cast only if the column exists."""
    return df.with_columns(
        [
            pl.col(col).cast(pl.Float64, strict=False)
            .cast(dtype)
            for col in columns
            if col in df.columns
        ]
    )


def parse_datetime_columns(df: pl.DataFrame, columns: Iterable[str],
                           fmt: str = "%Y-%m-%d %H:%M:%S", strict: bool = False) -> pl.DataFrame:
    """Converts text columns (Utf8) to pl.Datetime using strptime.
    The conversion only applies if the column exists and is Utf8."""
    return df.with_columns(
        [
            pl.col(col)
              .str.strptime(pl.Datetime, format=fmt, strict=strict)
              .alias(col)
            for col in columns
            if col in df.columns and df.schema[col] == pl.Utf8
        ]
    )


def to_cleaned_str(df: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
    """Clean data and convert to uppercase."""
    return df.with_columns(
        [
            pl.col(col).str.strip_chars().str.to_uppercase().alias(col)
            for col in columns
            if col in df.columns
        ]
    )


def truncate_str_to_255(df: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
    """Truncate string to 255 character if applicable."""
    cols_set = set(df.columns)
    long_text_set = set(LONG_TEXT_COLS)
    candidate_cols = [
        c for c in columns if c in cols_set and c not in long_text_set]
    if not candidate_cols:
        return df

    return df.with_columns([pl.col(c).str.slice(0, 255) for c in candidate_cols])


def _build_expr(col_name: str, mapeo: Mapping[str, str]) -> pl.Expr:
    """Build clean expression"""
    expr = pl.col(col_name).cast(pl.Utf8).str.to_uppercase()
    for roto, real in mapeo.items():
        expr = expr.str.replace_all(roto, real)

    # Additional cleaning
    expr = (
        expr
        .str.replace_all(r'[?\\"*,:;.]', " ")   # ? \" * -> espacio
        .str.replace_all(r"\s+", " ")       # colapsar espacios
        .str.strip_chars()                  # quitar espacios al inicio/fin
    )
    return expr.alias(col_name)


def manual_encoding(df: pl.DataFrame, cols: Iterable[str], mapeo: Mapping[str, str]) -> pl.DataFrame:
    """Apply manual encoding to the correspondig columns."""
    cols_existentes = [c for c in cols if c in df.columns]
    if not cols_existentes:
        return df

    exprs = [_build_expr(c, mapeo) for c in cols_existentes]
    return df.with_columns(exprs)


def transform(df: pl.DataFrame) -> pl.DataFrame:
    """Apply cast and formating to the DataFrames."""
    df = cast_columns(df, col_int32, pl.Int32)
    df = cast_columns(df, col_float, pl.Float64)
    df = parse_datetime_columns(df, col_date)
    df = to_cleaned_str(df, col_str)
    df = truncate_str_to_255(df, col_str_trucated)
    df = manual_encoding(df, col_encode, mapeo)
    return df
