# ETL-SAT-NOMINAS
Full ETL process with Polars and SQL Server. All DataFrames (tables) are obtain from csv files.

## 🌎 Repository Structure
```
ETL-SAT-NOMINAS/
├── main.py
├── .gitignore
├── env/                # Virtual enviroment (not provided)
└── requirements.txt
└── pkg                 # Contains all needed files (Python package)
    └── __init__.py     # Specifies that folder 'pkg' is a Python package
    └── extract.py      # Contains all functions related to extraction process
    └── transform.py    # Contains all functions related to transform process
    └── globals.py      # Contains all global variables
    └── config.py       # Contains all configuration params
    └── .env            # Contains all secret data (not provided)
```


## ✨ Details
**main_resume_functions.py:** This script calls 'extract.py' to obtain the DataFrames corresponding to the tables, then 'transform.py' script is called to clean data, to convert the columns into the correct format and to load to SQL Server. The corresponding table is created by using SQL commands before loading data. It has the following features:
- Automatic resumption with JSON checkpoints ('checkpoints' folder is created to save the JSON file with the corresponding information to restart the process for each table).
- Manual resumption from a row using `--resume-row`.
- Graceful pause by creating the `pause.flag` file.
- Idempotent row loading by adding the `_etl_source_row` technical column.

Before resuming, deletes from that row forward to avoid duplicates.

**main.py:** Performences the ETL process of 'main_resume_functions.py', but restart is not possible. It is possible to save the processed data into a CSV file with the 'save_batch_to_csv' function.

## 🚀 How to run locally
1. Clone this repository:
```
git clone https://github.com/departamentoIA/ETL-SAT-NOMINAS.git
```
2. Set virtual environment and install dependencies.

For Windows:
```
python -m venv env
env/Scripts/activate
pip install -r requirements.txt
```
For Linux:
```
python -m venv env && source env/bin/activate && pip install -r requirements.txt
```
3. Create your ".env" file, which has the following form:
```
DB_SERVER=10.0.00.00,5000
DB_NAME=SAT
DB_USER=caarteaga
DB_PASSWORD=pa$$word
```
4. Run "main_resume_functions.py":
```
python main_resume_functions.py --root-data-path "\\sia\AECF\DGATIC\LOTA\Bases de Datos\SAT" --tables AECF_0101_Anexo5 AECF_0101_Anexo6
```
To run specific tables and conditions:
```
python main_resume_functions.py ^
  --root-data-path "\\sia\AECF\DGATIC\LOTA\Bases de Datos\SAT" ^
  --tables AECF_0101_Anexo5 AECF_0101_Anexo6 ^
  --resume-table AECF_0101_Anexo6 ^
  --resume-row 250001
```