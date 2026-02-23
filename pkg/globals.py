# globals.py
from pathlib import Path
import polars as pl
from pathlib import Path
from typing import Dict, List, Optional, Any

ROOT_DATA_PATH = Path(
    r"\\sia\AECF\DGATIC\LOTA\Bases de Datos\SAT")

BATCH_SIZE = 10_000     # It should be lower than 100_000

TABLES_TO_PROCESS = [
    'GERG_AECF_1891_Anexo3C',
]

# Columns with very long text
LONG_TEXT_COLS = ['UUID']

# Columns 'Int32' type for all tables
col_int32 = ['NumDiasPagados', 'ReceptorTipoContrato', 'ReceptorTipoRegimen',
             'ReceptorPeriodicidadPago', 'ReceptorBanco'
             ]

# Columns 'DATE' type for all tables
col_date = ['ReceptorFechaInicioRelLaboral', '[FechaCancelacion]']

# String Columns to be converted to string, to be clean and to be converted to uppercase
col_str = ['UUID', 'EmisorRFC', 'ReceptorRFC', 'TipoNomina', 'EmisorCurp',
           'EmisorEntidadSNCFOrigenRecurso', 'EmisorEntidadSNCFMontoRecursoPropio',
           'ReceptorDepartamento', 'ReceptorPuesto',
           ]

# Columns 'Float64' type for all tables
col_float = ['TotalPercepciones', 'TotalDeducciones', 'TotalOtrosPagos',
             'PercepcionesTotalGravado', 'PercepcionesTotalExento',
             'TotalOtrasDeducciones', 'NominaTotalImpuestosRetenidos'
             ]
