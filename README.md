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

**main.py:** This script calls 'extract.py' to obtain the DataFrames corresponding to the tables, then 'transform.py' script is called to clean data, to convert the columns into the correct format and to load to SQL Server. The corresponding table is created by using SQL commands before loading data.

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
4. Run "main.py".
