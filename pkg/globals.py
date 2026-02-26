# globals.py
from pathlib import Path
import polars as pl
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging

ROOT_DATA_PATH = Path(
    r"\\sia\AECF\DGATIC\LOTA\Bases de Datos\SAT")

BATCH_SIZE = 100                # It should be lower than 1000
n_lotes = 1000

TABLES_TO_PROCESS = [
    'GERG_AECF_1891_Anexo7G'
    # 'GERG_AECF_1891_Anexo3C', 'GERG_AECF_1891_Anexo4D',
    # 'GERG_AECF_1891_Anexo5E', 'GERG_AECF_1891_Anexo6F',
    # Subidas:
    # 'GERG_AECF_1891_Anexo7G'
]

# Columns with very long text
LONG_TEXT_COLS = ['UUID']

# Columns 'Int32' type for all tables
col_int32 = ['NumDiasPagados', 'ReceptorTipoContrato', 'ReceptorTipoRegimen',
             'ReceptorBanco', 'TipoPercepcion', 'DeduccionTipoDeduccion',
             'SeparacionIndemnizacionNumAniosServicio',
             'SecuenciaOtrosPagos',
             ]

# Columns 'DATE' type for all tables
col_date = ['FechaCancelacion']

# String Columns to be converted to string, to be clean and to be converted to uppercase
col_str = ['UUID', 'EmisorRFC', 'ReceptorRFC', 'TipoNomina', 'EmisorCurp',
           'EmisorEntidadSNCFOrigenRecurso', 'EmisorEntidadSNCFMontoRecursoPropio',
           'ReceptorDepartamento', 'ReceptorPuesto', 'PercepcionClave',
           'PercepcionConcepto', 'DeduccionClave', 'DeduccionConcepto',
           'Concepto', 'SaldoAFavor', 'Anio', 'RemanenteSalFav', 'ReceptorFechaInicioRelLaboral',
           ]

# Columns 'Float64' type for all tables
col_float = ['TotalPercepciones', 'TotalDeducciones', 'TotalOtrosPagos',
             'PercepcionesTotalGravado', 'PercepcionesTotalExento',
             'TotalOtrasDeducciones', 'NominaTotalImpuestosRetenidos',
             'PercepcionImporteGravado', 'PercepcionImporteExento',
             'DeduccionesImporte', 'PercepcionesTotalSueldos',
             'PercepcionesTotalSeparacionIndemnizacion',
             'PercepcionesTotalJubilacionPensionRetiro',
             'JubilacionPensionRetiroTotalUnaExhibicion',
             'JubilacionPensionRetiroTotalParcialidad',
             'JubilacionPensionRetiroMontoDiario',
             'JubilacionPensionRetiroIngresoAcumulable',
             'JubilacionPensionRetiroIngresoNoAcumulable',
             'SeparacionIndemnizacionTotalPagado', 'SubsidioCausado',
             'SeparacionIndemnizacionUltimoSueldoMensOrd',
             'SeparacionIndemnizacionIngresoAcumulable',
             'SeparacionIndemnizacionIngresoNoAcumulable', 'Importe'
             ]

# Columns to be trucated
col_str_trucated = [
    'ReceptorDepartamento', 'ReceptorPuesto', 'PercepcionConcepto',
    'DeduccionConcepto', 'Concepto'
]

# Columns to be encoded manually
col_encode = ['ReceptorDepartamento', 'ReceptorPuesto', 'PercepcionConcepto',
              'DeduccionConcepto', 'Concepto']

mapeo = {
    r"([AEIOUaeiou])\ufffd([AEIOUaeiou])": r"${1}Ñ${2}",
    r"([AEIOUaeiou])\ufffdN(\s|$)": r"${1}ÓN ",
    r"PEDAG\ufffdGICA": "PEDAGÓGICA", r"GEN\ufffdRICA": "GENÉRICA",
    r"DID\ufffdCTICO": "DIDÁCTICO", r"ESPEC\ufffdFICA": "ESPECÍFICA",
    r"MAESTR\ufffdA": "MAESTRÍA", r"B\ufffdSICA": "BÁSICA",
    r"EST\ufffdMULO": "ESTÍMULO", r"M\ufffdLTIPLE": "MÚLTIPLE",
    r"ACAD\ufffdMICO": "ACADÉMICO", r"N\ufffdMINA": "NÓMINA",
    r"CR\ufffdDITO": "CRÉDITO", r"PR\ufffdSTAMO": "PRÉSTAMO",
    r"AC\ufffdRCATE": "ACÉRCATE", r"CESANT\ufffdA": "CESANTÍA",
    r"VI\ufffdTICOS": "VIÁTICOS"
}

logging.basicConfig(
    filename="log.txt",
    level=logging.INFO,
    format="%(asctime)s - %(message)s"
)
