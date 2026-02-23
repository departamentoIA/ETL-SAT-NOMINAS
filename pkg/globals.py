# globals.py
from pathlib import Path
import polars as pl
from pathlib import Path
from typing import Dict, List, Optional, Any

ROOT_DATA_PATH = Path(
    r"\\sia\AECF\DGATIC\LOTA\Bases de Datos\SAT")

BATCH_SIZE = 10_000     # It should be lower than 50_000

TABLES_TO_PROCESS = [
    'GERG_AECF_1891_Anexo1A-QA',
]

# Columns 'Int32' type for all tables
col_int32 = ['FormaPago', 'EmisorRegimenFiscal',
             ]

# Columns 'DATE' type for all tables
col_date = ['FechaEmision', 'FechaCertificacion', 'FechaCancelacion']

# String Columns to be converted to string, to be clean and to be converted to uppercase
col_str = ['UUID', 'Moneda', 'TipoDeComprobante', 'EmisorRFC', 'ReceptorRFC', 'Serie',
           'Folio', 'CondicionesDePago', 'MetodoPago', 'ReceptorNombre', 'EmisorNombre',
           ]

# Columns 'Float64' type for all tables
col_float = ['Descuento', 'SubTotal', 'Total', 'TrasladosIVA', 'TrasladosIEPS',
             'TotalImpuestosTrasladados', 'RetenidosIVA', 'RetenidosISR',
             'TotalImpuestosRetenidos', 'TipoCambio',
             ]

# Columns to be encoded manually
col_encode = ['EmisorRFC', 'EmisorNombre', 'ReceptorRFC', 'ReceptorNombre']

# Catalogs
path_cat = r"\\sia\AECF\DGATIC\LOTA\Bases de Datos\CATALOGOS\CatalogoAPF_2026.xlsx"
catalogoAPF = pl.read_excel(path_cat)
path_TIC = r"\\sia\AECF\DGATIC\LOTA\Bases de Datos\CATALOGOS\Proveedores_TIC_2026.xlsx"
proveeedoresTIC = pl.read_excel(path_TIC)
path_sancionado = r"\\sia\AECF\DGATIC\LOTA\Bases de Datos\CATALOGOS\Proveedores_Contratistas_Sancionados.xlsx"
sancionado = pl.read_excel(path_sancionado)
path_lista69 = r"\\sia\AECF\DGATIC\LOTA\Bases de Datos\CATALOGOS\Listado_Completo_69-B.xlsx"
lista69 = pl.read_excel(path_lista69)

mapeo = {
    'Ã‘': 'Ñ', 'Ã±': 'Ñ', 'ÃƑ': 'Ñ',
    'Ã¡': 'Á', 'Ã ': 'Á', 'Ã': 'Á', 'Ã€': 'Á',
    'Ã©': 'É', 'Ã‰': 'É', 'Ã¨': 'É',
    'Ã­': 'Í', 'Ã': 'Í', 'Ã\xad': 'Í', 'Ã¬': 'Í',
    'Ã³': 'Ó', 'Ã“': 'Ó', 'Ã’': 'Ó', 'Ã²': 'Ó',
    'Ãº': 'Ú', 'ÃŠ': 'Ú',
    'Ã¼': 'Ü', 'Ãœ': 'Ü', 'ÃŒ': 'Ü', 'Ã›': 'Ü',
    'ÃA': 'ÍA', 'UÃ': 'UÍ', 'Â´': '´', '`': '´',
    r'ACU\#A': 'ACUÑA', r'ACU\?A': 'ACUÑA', r'CU\?A': 'CUAÑA',
    r'ACUÃ\?A': 'ACUÑA', r'BOLA\?OS': 'BOLAÑOS', r'A\?UELO': 'AÑUELO',
    r'AVENDA\?O': 'AVENDAÑO',  r'NIER\?A': 'NIERÍA',
    r'ALC\?NT': 'ALCÁNT', r'\?LV': 'ÁLV', r'R\?NDI': 'RÉNDI',
    r'CI\?N\s': 'CIÓN ', r'MAR\?A': 'MARÍA', r'GR\?A': 'GRÍA',
    r'JOS\?': 'JOSÉ', r'BEN\?TEZ': 'BENÍTEZ', r'CUAUHT\?MOC': 'CUAUHTÉMOC',
    r'COMPA\?IA': 'COMPAÑÍA', r'\?NGEL': 'ÁNGEL', r'CI\?N$': 'CIÓN',
    r'BRISE\?O': 'BRISEÑO',  r'ALAN\?S': 'ALANÍS', r'MARTÃ\?NEZ': 'MARTÍNEZ',
    r'AVI\?A': 'AVIÑA', r'G\?N\s': 'GÓN ', r'ALBARR\?N': 'ALBARRÁN',
    r'AR\?VALO': 'ARÉVALO', r'BA\?OS': 'BAÑOS', r'BRISE\?O': 'BRISEÑO',
    r'CASTA\?EDA': 'CASTAÑEDA', r'AGUI\?IGA': 'AGUIÑIGA', r'ALCAL\?': 'ALCALÁ',
    r'ADRI\?N': 'ADRIÁN', r'BARRAG\?N': 'BARRAGÁN', r'BELTR\?N': 'BELTRÁN',
    r'C\?RDENAS': 'CÁRDENAS', r'BRICE\?O': 'BRICEÑO', r'SULTOR\?A': 'SULTORÍA',
    r'M\?XICO': 'MÉXICO', r'CARRE\?O': 'CARREÑO', r'BORB\?N': 'BORBÓN',
    r'BARE\?O': 'BAREÑO', r'CABA\?AS': 'CABAÑAS', r'CEDE\?O': 'CEDEÑO',
    r'BURGUE\?O': 'BURGUEÑO', r'S\?NCHEZ': 'SÁNCHEZ', r'PE\?A': 'PEÑA',
    r'CAMPAÃ\?A': 'CAMPAÑA', r'MISI\?N': 'MISIÓN', r'DISE\?O': 'DISEÑO',
    r'GONZÃ\?LEZ': 'GONZÁLEZ', r'MUÃ\?OZ': 'MUÑOZ', r'YAÃ\?EZ': 'YAÑEZ',
    r'YA\?EZ': 'YAÑEZ', r'ZU\?IGA': 'ZUÑIGA', r'L\?PEZ': 'LÓPEZ',
    r'ELECTR\?N': 'ELECTRÓN', r'MAGA\?A': 'MAGAÑA', r'GOÃ\?I': 'GOÑI',
    r'IBA\?EZ': 'IBAÑEZ', r'FARMACÃ\?UTICA': 'FARMACÉUTICA', r'SE\?OR': 'SEÑOR',
    r'NU\?EZ': 'NUÑEZ', r'NUÃ\?EZ': 'NUÑEZ', r'PEÃ\?ON': 'PEÑÓN',
    r'PIÃ\?A': 'PIÑA', r'ORTU\?O': 'ORTUÑO', r'QUIÃ\?ONES': 'QUIÑONES',
    r'ENSEÃ\?ANZA': 'ENSEÑANZA', r'ESPAÃ\?': 'ESPAÑ', r'ESPA\?A': 'ESPAÑA',
    r'M\?LTIPLE': 'MÚLTIPLE', r'SAÃ\?UDO': 'SAÑUDO', r'TUR\?STIC': 'TURÍSTIC',
    r'MÃ\?XICO': 'MÉXICO', r'C\?SAR': 'CÉSAR', r'R\?OS': 'RÍOS',
    r'QUER\?TA': 'QUERÉTA', 'MÃˆXI': 'MÉXI', 'MARÃŒA': 'MARÍA', 'GALVÃ N': 'GALVÁN',
    r'B\?RCENAS': 'BÁRCENAS', r'ANG\?LICA': 'ANGÉLICA', r'ROC\?O': 'ROCÍO',
    r'GARC\?A': 'GARCÍA', r'ANDR\?S': 'ANDRÉS', r'SALDA\?A': 'SALDAÑA',
}
