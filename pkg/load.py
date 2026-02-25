# load.py
"""This file calls 'globals.py' and 'config.py'."""
from pkg.globals import *
from sqlalchemy import create_engine
from pkg.config import get_connection_string
from sqlalchemy import text


def map_polars_to_sql(colname: str, dtype: pl.DataType):
    """Get the SQL type of every column."""
    if dtype == pl.Utf8 and colname in LONG_TEXT_COLS:
        return "NVARCHAR(MAX)"
    if colname == 'EmisorRFC' or colname == 'ReceptorRFC':
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
        pl.Datetime: "DATETIME2"
    }
    return mapping.get(dtype, "NVARCHAR(255)")


def create_table_from_df(engine, table_name: str, df: pl.DataFrame,
                         primary_key: str | None = None) -> None:
    """Create table in SQL Server by using both SQL commands and DataFrame scheme."""
    full_name_for_object_id = f"dbo.{table_name}"
    full_name_bracket = f"[dbo].[{table_name}]"
    columns_sql = []
    for col, dtype in df.schema.items():
        sql_type = map_polars_to_sql(col, dtype)
        columns_sql.append(f"[{col}] {sql_type}")

    pk_sql = ""
    if primary_key:
        pk_sql = f", CONSTRAINT PK_{table_name} PRIMARY KEY ({primary_key})"
    else:
        print(f"No se crea Primary Key para la tabla '{table_name}'")

    create_sql = f"""
    IF OBJECT_ID(N'{full_name_for_object_id}', 'U') IS NOT NULL
        DROP TABLE {full_name_bracket};

    CREATE TABLE {full_name_bracket} (
        {', '.join(columns_sql)}
        {pk_sql}
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))


def load_table(df: pl.DataFrame, table_name: str, batch_count: int) -> None:
    """Load the DataFrame to SQL Server by using SQLAlchemy."""
    # 'fast_executemany=True' is the secret to speed in SQL Server
    engine = create_engine(get_connection_string(), fast_executemany=True)
    table_name = table_name.replace("-", "_")
    if batch_count == 1:
        create_table_from_df(engine, table_name, df)
        print(f"Subiendo tabla '{table_name}' a SQL Server...")
    try:
        df.write_database(
            table_name=table_name,
            connection=engine,
            if_table_exists="append"
        )

    except Exception as e:
        print(f"❌ Error: {e}")
