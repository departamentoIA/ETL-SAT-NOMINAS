# load.py
"""This file calls 'globals.py' and 'config.py'."""
from pkg.globals import *
from sqlalchemy import create_engine
from pkg.config import get_connection_string
from sqlalchemy import text


def map_polars_to_sql(dtype):
    """Get the SQL type of every column."""
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


def create_table(engine) -> None:
    """Create table in SQL Server by using both SQL commands and DataFrame scheme."""
    create_sql = f"""
    CREATE TABLE [dbo].[GERG_AECF_1891_Anexo3C] (
    [UUID] VARHAR(40),
    [EmisorRFC] VARHAR(13),
    [ReceptorRFC] VARHAR(13),
    [TipoNomina] VARHAR(10),
    [FechaPago] DATE,
    [FechaPago2] DATE,
    [FechaFinalPago] DATE,
    [NumDiasPagados] INT,
    [TotalPercepciones] REAL,
    [TotalDeducciones] REAL,
    [TotalOtrosPagos] REAL,
    [PercepcionesTotalGravado] REAL,
    [PercepcionesTotalExento] REAL,
    [TotalOtrasDeducciones] REAL,
    [NominaTotalImpuestosRetenidos] REAL,
    [EmisorCurp] VARHAR(18),
    [EmisorEntidadSNCFOrigenRecurso] VARHAR(MAX),
    [EmisorEntidadSNCFMontoRecursoPropio] VARHAR(MAX),
    [ReceptorNumSeguridadSocial] BIGINT,
    [ReceptorFechaInicioRelLaboral] DATE,
    [ReceptorTipoContrato] INT,
    [ReceptorTipoRegimen] INT,
    [ReceptorNumEmpleado] BIGINT,
    [ReceptorDepartamento] VARHAR(MAX),
    [ReceptorPuesto] VARHAR(MAX),
    [ReceptorPeriodicidadPago] INT,
    [ReceptorCuentaBancaria] BIGINT,
    [ReceptorBanco] INT,
    [FechaCancelacion] DATE,
);
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))


def load_table(df: pl.DataFrame, table_name: str, batch_count: int) -> None:
    """Load the DataFrame to SQL Server by using SQLAlchemy."""
    # 'fast_executemany=True' is the secret to speed in SQL Server
    engine = create_engine(get_connection_string(), fast_executemany=True)
    table_name = table_name.replace("-", "_")
    print(f"Subiendo tabla '{table_name}' a SQL Server...")
    if batch_count == 1:
        pass
        # create_table(engine)
    try:
        df.write_database(
            table_name=table_name,
            connection=engine,
            if_table_exists="append"
        )

    except Exception as e:
        print(f"❌ Error: {e}")
