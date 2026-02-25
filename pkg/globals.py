# globals.py
from pathlib import Path
import polars as pl
from pathlib import Path
from typing import Dict, List, Optional, Any

ROOT_DATA_PATH = Path(
    r"\\sia\AECF\DGATIC\LOTA\Bases de Datos\SAT")

BATCH_SIZE = 1000     # It should be lower than 100_000

TABLES_TO_PROCESS = [
    'GERG_AECF_1891_Anexo5E'
    # 'GERG_AECF_1891_Anexo3C', 'GERG_AECF_1891_Anexo4D'
]

# Columns with very long text
LONG_TEXT_COLS = ['UUID']

# Columns 'Int32' type for all tables
col_int32 = ['NumDiasPagados', 'ReceptorTipoContrato', 'ReceptorTipoRegimen',
             'ReceptorPeriodicidadPago', 'ReceptorBanco', 'TipoPercepcion',
             'DeduccionTipoDeduccion'
             ]

# Columns 'DATE' type for all tables
col_date = ['ReceptorFechaInicioRelLaboral', 'FechaCancelacion']

# String Columns to be converted to string, to be clean and to be converted to uppercase
col_str = ['UUID', 'EmisorRFC', 'ReceptorRFC', 'TipoNomina', 'EmisorCurp',
           'EmisorEntidadSNCFOrigenRecurso', 'EmisorEntidadSNCFMontoRecursoPropio',
           'ReceptorDepartamento', 'ReceptorPuesto', 'PercepcionClave',
           'PercepcionConcepto', 'DeduccionClave', 'DeduccionConcepto'
           ]

# Columns 'Float64' type for all tables
col_float = ['TotalPercepciones', 'TotalDeducciones', 'TotalOtrosPagos',
             'PercepcionesTotalGravado', 'PercepcionesTotalExento',
             'TotalOtrasDeducciones', 'NominaTotalImpuestosRetenidos',
             'PercepcionImporteGravado', 'PercepcionImporteExento',
             'DeduccionesImporte'
             ]

# Columns to be encoded manually
col_encode = ['ReceptorPuesto', 'PercepcionConcepto', 'DeduccionConcepto']

mapeo = {
    r"([AEIOUaeiou])\ufffd([AEIOUaeiou])": r"${1}Ñ${2}",
    r"([AEIOUaeiou])\ufffdN(\s|$)": r"${1}ÓN ",
    r"PEDAG\ufffdGICA": "PEDAGÓGICA", r"GEN\ufffdRICA": "GENÉRICA",
    r"DID\ufffdCTICO": "DIDÁCTICO", r"ESPEC\ufffdFICA": "ESPECÍFICA",
    r"MAESTR\ufffdA": "MAESTRÍA", r"B\ufffdSICA": "BÁSICA",
    r"EST\ufffdMULO": "ESTÍMULO", r"M\ufffdLTIPLE": "MÚLTIPLE",
    r"ACAD\ufffdMICO": "ACADÉMICO", r"N\ufffdMINA": "NÓMINA",
    r"CR\ufffdDITO": "CRÉDITO", r"PR\ufffdSTAMO": "PRÉSTAMO",
    r"AC\ufffdRCATE": "ACÉRCATE", r"CESANT\ufffdA": "CESANTÍA"
}
