import csv
from datetime import datetime
from src.insert_database import InsertDatabase
from src.function_query import FunctionQuery
from src.table_manager import TableManager

BATCH_SIZE = 100000
ROUND_NUMBER = 50

FILE_INSERTION = 'output/insertion_times.csv'
HEADER_INSERTION = ['table_name', 'insertion_time', 'current_week', 'round_number', 'ram_usage', 'swap_usage', 'storage']
FILE_QUERY = 'output/query_times.csv'
HEADER_QUERY = ['table_name', 'query_time', 'query_type', 'round_number', 'ram_usage', 'swap_usage']

DATABASES = [
    {"name": "mariadb_innodb", "type": "InnoDB", "port": 3308, "function": InsertDatabase.insert_mariadb},
    {"name": "mariadb_innodb_optimized", "type": "InnoDB", "port": 3309, "function": InsertDatabase.insert_mariadb_structured},
    {"name": "mariadb_myrocks", "type": "ROCKSDB", "port": 3310, "function": InsertDatabase.insert_mariadb_structured},
    {"name": "mariadb_columnstore", "type": "ColumnStore", "port": 3307, "function": InsertDatabase.insert_mariadb},
    {"name": "influxdb", "type": "InfluxDB", "function": InsertDatabase.insert_influxdb},
]

def create_tables() -> None:
    """
    Cria todas as tabelas necessárias no banco de dados.
    """
    table_manager = TableManager()
    table_manager.create_all_tables()
    print("Tabelas criadas.")

def process_insertion() -> None:
    """
    Processa a inserção de dados em todos os bancos de dados configurados.
    """
    print("Iniciando inserção de dados...")
    for db in DATABASES:
        print(f"Processando inserção para: {db['name']} ({db['type']})")
        insert_data(db)
        print(f"Finalizada inserção para: {db['name']}\n")

def insert_data(db: dict) -> None:
    """
    Insere os dados no banco de dados especificado.
    
    :param db: Dicionário contendo informações do banco de dados (nome, tipo, porta, função de inserção).
    """
    data_to_insert_maria, data_to_insert_influx = [], []
    current_week = None
    
    with open('data/sensor_data_2_years.csv', newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header
        
        for row in reader:
            row_date = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
            year_week = row_date.isocalendar()[:2]  # (year, week)
            
            if current_week and current_week != year_week:
                insert_to_db(db, data_to_insert_maria, data_to_insert_influx, current_week)
                data_to_insert_maria, data_to_insert_influx = [], []
            
            current_week = year_week
            data_to_insert_maria.append(row)
            
            data_to_insert_influx.append({
                "measurement": "sensor_data",
                "tags": {"sensor_name": row[2]},
                "time": row[0] if row[0].endswith("Z") else row_date.isoformat() + "Z",
                "fields": {"temperature": float(row[1])}
            })
    
    if data_to_insert_maria:
        insert_to_db(db, data_to_insert_maria, data_to_insert_influx, current_week)

def insert_to_db(db: dict, data_to_insert_maria: list, data_to_insert_influx: list, current_week: tuple) -> None:
    """
    Executa a inserção de dados no banco de dados correspondente.
    
    :param db: Dicionário contendo informações do banco de dados.
    :param data_to_insert_maria: Lista de dados a serem inseridos no MariaDB.
    :param data_to_insert_influx: Lista de dados a serem inseridos no InfluxDB.
    :param current_week: Tupla contendo o ano e a semana correspondente aos dados.
    """
    print(f"Inserindo dados da semana {current_week}...")
    if db["type"] == "InfluxDB":
        db["function"](ROUND_NUMBER, BATCH_SIZE, data_to_insert_influx, current_week, FILE_INSERTION)
    else:
        db["function"](db["name"], db["type"], ROUND_NUMBER, BATCH_SIZE, data_to_insert_maria, current_week, FILE_INSERTION, db["port"])

def process_queries() -> None:
    """
    Processa as consultas nos bancos de dados e armazena os tempos de execução.
    """
    print("Iniciando consultas...")
    with open(FILE_QUERY, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(HEADER_QUERY)
    
    for round_number in range(1, ROUND_NUMBER):
        FunctionQuery.query_mariadb("mariadb_innodb", 3308, round_number, FILE_QUERY)
        FunctionQuery.query_mariadb_structured("mariadb_innodb_optimized", 3309, round_number, FILE_QUERY)
        FunctionQuery.query_mariadb_structured("mariadb_myrocks", 3310, round_number, FILE_QUERY)
        FunctionQuery.query_mariadb("mariadb_columnstore", 3307, round_number, FILE_QUERY)
        FunctionQuery.query_influxdb(round_number, FILE_QUERY)

def main() -> None:
    """
    Função principal para execução do script.
    """
    print("Start")
    create_tables()
    process_insertion()
    process_queries()
    print("Processo finalizado.")

if __name__ == "__main__":
    main()