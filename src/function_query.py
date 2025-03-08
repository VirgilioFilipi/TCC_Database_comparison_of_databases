from src.save_data import SaveData
import pymysql
import time
import psutil
import pandas as pd
from influxdb_client import InfluxDBClient
from src.query_database import QueryDatabase
import configparser

class FunctionQuery:
    """Classe para executar consultas em bancos de dados e salvar métricas."""

    @classmethod
    def query_mariadb(cls, db_name: str, port: int, round_number: int, file_name_query: str) -> None:
        print(f'### {db_name} ###')

        QDB = QueryDatabase()
        queries = [
            (QDB.query_1_year_a, '1_year_a'),
            (QDB.query_1_day_full, '1_day_full'),
            (QDB.query_group_mean_week_b, 'group_mean_6months_week_b'),
            (QDB.query_group_sum_month_a, 'group_sum_month_a'),
            (QDB.query_group_mean_min_a, 'group_mean_min_a'),
            (QDB.query_max_min_days_full, 'max_min_10days_full'),
            (QDB.query_count_line_full, 'count_line_full')
        ]
        try:
            config = configparser.ConfigParser()
            config.read('config.ini')

            DB_USER = config['database']['user']
            DB_PASSWORD = config['database']['password']
            conn = pymysql.connect(
                host='localhost', port=port, user=DB_USER, password=DB_PASSWORD, database=db_name
            )
            cursor = conn.cursor()

            for query_function, label in queries:
                query = query_function()
                results, query_time = cls.execute_query(cursor, query)
                cls.save_metrics(db_name, query_time, label, round_number, file_name_query)        
                print(f"Número de linhas retornadas: {len(results)}")
                print(f"{label} {query_time} segundos")

        except pymysql.MySQLError as e:
            print(f"Erro ao conectar ao MariaDB: {e}")
        
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()

    @classmethod
    def query_mariadb_structured(cls, db_name: str, port: int, round_number: int, file_name_query: str) -> None:
        print(f'### {db_name} ###')
        QDB = QueryDatabase()
        queries = [
            (QDB.query_1_year_a_structured, '1_year_a'),
            (QDB.query_1_day_full_structured, '1_day_full'),
            (QDB.query_group_mean_week_b_structured, 'group_mean_6months_week_b'),
            (QDB.query_group_sum_month_a_structured, 'group_sum_month_a'),
            (QDB.query_group_mean_min_a_structured, 'group_mean_min_a'),
            (QDB.query_max_min_days_full_structured, 'max_min_10days_full'),
            (QDB.query_count_line_full_structured, 'count_line_full')
        ]
        try:
            config = configparser.ConfigParser()
            config.read('config.ini')

            DB_USER = config['database']['user']
            DB_PASSWORD = config['database']['password']
            
            conn = pymysql.connect(
                host='localhost', port=port, user=DB_USER, password=DB_PASSWORD, database=db_name
            )
            cursor = conn.cursor()

            for query_function, label in queries:
                query = query_function()
                results, query_time = cls.execute_query(cursor, query)
                cls.save_metrics(db_name, query_time, label, round_number, file_name_query)    
                
                print(f"Número de linhas retornadas: {len(results)}")
                print(f"{label} {query_time} segundos")

        except pymysql.MySQLError as e:
            print(f"Erro ao conectar ao MariaDB: {e}")

        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()

    @classmethod
    def query_influxdb(cls, round_number: int, file_name_query: str) -> None:

        config = configparser.ConfigParser()
        config.read('config.ini')
       
        INFLUX_URL = config['influxdb']['url']
        INFLUX_TOKEN = config['influxdb']['token']
        INFLUX_ORG = config['influxdb']['org']
        INFLUX_BUCKET = config['influxdb']['bucket']
        
        print('### InfluxDB ###')     

        QDB = QueryDatabase() 

        queries = [
            (QDB.query_1_year_a_influx, '1_year_a'),
            (QDB.query_1_day_full_influx, '1_day_full'),
            (QDB.query_group_mean_week_b_influx, 'group_mean_6months_week_b'),
            (QDB.query_group_sum_month_a_influx, 'group_sum_month_a'),
            (QDB.query_group_mean_min_a_influx, 'group_mean_min_a'),
            (QDB.query_max_min_days_full_influx, 'max_min_10days_full'),
            (QDB.query_count_line_full_influx, 'count_line_full')
        ]
        try:
            # Abre a conexão uma única vez
            client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
            query_api = client.query_api()

            for query_function, label in queries:
                query = query_function()
                results, query_time = cls.execute_query_influx(query_api, query, INFLUX_ORG)
                cls.save_metrics("influxdb", query_time, label, round_number, file_name_query)
                print(f"Número de linhas retornadas: {len(results)}")
                print(f"{label} {query_time:.4f} segundos")

        except Exception as e:
            print(f"Erro ao executar queries no InfluxDB: {e}")

        finally:
            # Fecha a conexão corretamente no final
            if 'client' in locals():
                client.close()

    @classmethod
    def execute_query_influx(cls, query_api, query, org):
        start_time = time.time()
        results = query_api.query(query=query, org=org)
        query_time = time.time() - start_time
        return results, query_time
    
    @staticmethod
    def execute_query(cursor, query):
        start_time = time.time()
        cursor.execute(query)
        results = cursor.fetchall()
        end_time = time.time()
        query_time = end_time - start_time
        return results, query_time

    @staticmethod
    def save_metrics(db_name, query_time, query_label, round_number, file_name_query):
        memory_info = psutil.virtual_memory()
        swap_info = psutil.swap_memory()
        ram_usage = memory_info.used / (1024 ** 3)  # Convert to GB
        swap_usage = swap_info.used / (1024 ** 3)  # Convert to GB

        SaveData.save_query_time_to_csv(
            db_name, query_time, query_label, round_number, file_name_query, ram_usage, swap_usage
        )

    @staticmethod
    def save_query_results_to_csv(results, file_name):
        """Salva o resultado da consulta em um arquivo CSV."""
        df = pd.DataFrame(results)
        df.to_csv(file_name, index=False)
        print(f"Resultado da consulta salvo em: {file_name}")

