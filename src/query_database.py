from src.save_data import SaveData
import pymysql
import time
import psutil
import pandas as pd
from influxdb_client import InfluxDBClient
import configparser

class QueryDatabase:
    """Classe para executar consultas em bancos de dados e salvar métricas."""

    @staticmethod
    def save_query_results_to_csv(results, file_name):
        """Salva o resultado da consulta em um arquivo CSV."""
        df = pd.DataFrame(results)
        df.to_csv(file_name, index=False)
        print(f"Resultado da consulta salvo em: {file_name}")

    @classmethod
    def query_1_year_a(cls, cursor, table_name):
        query = f"""
            SELECT event_timestamp, temperature, sensor_name
            FROM {table_name} 
            WHERE event_timestamp >= '2023-01-01 00:00:00' 
            AND event_timestamp < '2024-01-01 00:00:00'
            AND sensor_name = 'Sensor A';
        """
        return cls.execute_query(cursor, query)

    @classmethod
    def query_1_day_full(cls, cursor, table_name):
        query = f"""
            SELECT event_timestamp, temperature, sensor_name
            FROM {table_name} 
            WHERE event_timestamp >= '2023-01-02 00:00:00' 
            AND event_timestamp < '2023-01-03 00:00:00';
        """
        return cls.execute_query(cursor, query)

    @classmethod
    def query_group_mean_week_b(cls, cursor, table_name):
        query = f"""
        SELECT 
            YEARWEEK(event_timestamp, 1) AS week_interval,
            AVG(temperature) AS avg_temp
            FROM sensor_data
            WHERE sensor_name = 'Sensor B'
            AND event_timestamp >= '2023-01-02 00:00:00' 
            AND event_timestamp < '2023-06-01 00:00:00'
            GROUP BY week_interval
            ORDER BY week_interval;
        """
        return cls.execute_query(cursor, query)

    @classmethod
    def query_group_sum_month_a(cls, cursor, table_name):
        query = f"""
            SELECT 
                DATE_FORMAT(event_timestamp, '%Y-%m') AS month_start, 
                SUM(temperature) AS sum_temperature
            FROM {table_name}
            WHERE sensor_name = 'Sensor A'
            GROUP BY month_start
            ORDER BY month_start;
        """
        return cls.execute_query(cursor, query)

    @classmethod
    def query_group_mean_min_a(cls, cursor, table_name):
        query = f"""
        SELECT 
            FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_timestamp) / (15 * 60)) * (15 * 60)) AS interval_15min,
            AVG(temperature) AS avg_temp
        FROM {table_name}
        WHERE sensor_name = 'Sensor A'
        GROUP BY interval_15min
        ORDER BY interval_15min;
        """
        return cls.execute_query(cursor, query)
    
    @classmethod
    def query_max_min_days_full(cls, cursor, table_name):
        query = f"""
            SELECT 
                MAX(temperature) AS max_temp, 
                MIN(temperature) AS min_temp
            FROM {table_name}
            WHERE event_timestamp >= '2023-01-01 00:00:00' 
            AND event_timestamp < '2023-01-10 00:00:00';
        """
        return cls.execute_query(cursor, query)
    
    @classmethod
    def query_count_line_full(cls, cursor, table_name):
        query = f"""
            SELECT COUNT(*) 
            FROM {table_name};
        """
        return cls.execute_query(cursor, query)

    @classmethod
    def query_mariadb(cls, db_name: str, port: int, round_number: int, file_name_query: str) -> None:
        print(f'### {db_name} ###')

        table_name = "sensor_data"

        queries = [
            (cls.query_1_year_a, '1_year_a'),
            (cls.query_1_day_full, '1_day_full'),
            (cls.query_group_mean_week_b, 'group_mean_6months_week_b'),
            (cls.query_group_sum_month_a, 'group_sum_month_a'),
            (cls.query_group_mean_min_a, 'group_mean_min_a'),
            (cls.query_max_min_days_full, 'max_min_10days_full'),
            (cls.query_count_line_full, 'count_line_full')
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
                results, query_time = query_function(cursor, table_name)
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

    @classmethod
    def query_1_year_a_structured(cls, cursor, table_name):
        query = f"""
            SELECT event_timestamp, temperature, sensor_name
            FROM {table_name} 
            WHERE year_number = 2023
            AND sensor_name = 'Sensor A';
        """
        return cls.execute_query(cursor, query)

    @classmethod
    def query_1_day_full_structured(cls, cursor, table_name):
        query = f"""
            SELECT event_timestamp, temperature, sensor_name
            FROM {table_name} 
            WHERE year_number = 2023
            AND event_timestamp BETWEEN '2023-01-02 00:00:00' AND '2023-01-02 23:59:59';

        """
        return cls.execute_query(cursor, query)

    @classmethod
    def query_group_mean_week_b_structured(cls, cursor, table_name):
        query = f"""
        SELECT 
            YEARWEEK(event_timestamp, 1) AS week_interval,
            AVG(temperature) AS avg_temp
        FROM {table_name} 
        WHERE year_number = 2023
        AND sensor_name = 'Sensor B'
        AND event_timestamp BETWEEN '2023-01-02 00:00:00' AND '2023-05-31 23:59:59'

        GROUP BY week_interval
        ORDER BY week_interval;
        """
        return cls.execute_query(cursor, query)

    @classmethod
    def query_group_sum_month_a_structured(cls, cursor, table_name):
        query = f"""
        SELECT 
            YEAR(event_timestamp) AS year,
            MONTH(event_timestamp) AS month, 
            SUM(temperature) AS sum_temperature
        FROM {table_name}
        WHERE sensor_name = 'Sensor A'
        GROUP BY year, month
        ORDER BY year, month;
        """
        return cls.execute_query(cursor, query)

    @classmethod
    def query_group_mean_min_a_structured(cls, cursor, table_name):
        query = f"""
        SELECT 
            FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_timestamp) / (15 * 60)) * (15 * 60)) AS interval_15min,
            AVG(temperature) AS avg_temp
        FROM {table_name}
        WHERE sensor_name = 'Sensor A'
        GROUP BY interval_15min
        ORDER BY interval_15min;
        """
        return cls.execute_query(cursor, query)
    
    @classmethod
    def query_max_min_days_full_structured(cls, cursor, table_name):
        query = f"""
            SELECT 
                MAX(temperature) AS max_temp, 
                MIN(temperature) AS min_temp
            FROM {table_name}
            WHERE year_number = 2023
            AND event_timestamp BETWEEN '2023-01-01 00:00:00' AND '2023-01-09 23:59:59';

        """
        return cls.execute_query(cursor, query)
    
    @classmethod
    def query_count_line_full_structured(cls, cursor, table_name):
        query = f"""
            SELECT COUNT(*) 
            FROM {table_name};
        """
        return cls.execute_query(cursor, query)


    @classmethod
    def query_mariadb_structured(cls, db_name: str, port: int, round_number: int, file_name_query: str) -> None:
        print(f'### {db_name} ###')
        table_name = "sensor_data"

        queries = [
            (cls.query_1_year_a_structured, '1_year_a'),
            (cls.query_1_day_full_structured, '1_day_full'),
            (cls.query_group_mean_week_b_structured, 'group_mean_6months_week_b'),
            (cls.query_group_sum_month_a_structured, 'group_sum_month_a'),
            (cls.query_group_mean_min_a_structured, 'group_mean_min_a'),
            (cls.query_max_min_days_full_structured, 'max_min_10days_full'),
            (cls.query_count_line_full_structured, 'count_line_full')
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
                results, query_time = query_function(cursor, table_name)
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
    def execute_query_influx(cls, query_api, query, org):
        start_time = time.time()
        results = query_api.query(query=query, org=org)
        query_time = time.time() - start_time
        return results, query_time

    @classmethod
    def query_1_year_a_influx(cls, query_api, org):
        query = """
            from(bucket: "influx_bucket")
            |> range(start: 2023-01-01T00:00:00Z, stop: 2024-01-01T00:00:00Z)
            |> filter(fn: (r) => r._measurement == "sensor_data")
            |> filter(fn: (r) => r._field == "temperature")
            |> filter(fn: (r) => r.sensor_name == "Sensor A")
            |> keep(columns: ["_time", "_value", "sensor_name"])
            |> yield(name: "complete_data")
        """
        return cls.execute_query_influx(query_api, query, org)

    @classmethod
    def query_1_day_full_influx(cls, query_api, org):
        query = """
            from(bucket: "influx_bucket")
            |> range(start: 2023-01-02T00:00:00Z, stop: 2023-01-03T00:00:00Z)
            |> filter(fn: (r) => r._measurement == "sensor_data")
            |> filter(fn: (r) => r._field == "temperature")
            |> keep(columns: ["_time", "_value", "sensor_name"])
            |> yield(name: "complete_data")
        """
        return cls.execute_query_influx(query_api, query, org)

    @classmethod
    def query_group_mean_week_b_influx(cls, query_api, org):
        query = """
        from(bucket: "influx_bucket")
        |> range(start: 2023-01-02T00:00:00Z, stop: 2023-06-01T00:00:00Z)  
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> filter(fn: (r) => r.sensor_name == "Sensor B")
        |> filter(fn: (r) => r._field == "temperature")
        |> keep(columns: ["_time", "_value", "sensor_name"])

        |> group()
        |> aggregateWindow(every: 1w, fn: mean, createEmpty: false) 

        """
        return cls.execute_query_influx(query_api, query, org)

    @classmethod
    def query_group_sum_month_a_influx(cls, query_api, org):
        query = """
        from(bucket: "influx_bucket")
        |> range(start: 0)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> filter(fn: (r) => r.sensor_name == "Sensor A")
        |> filter(fn: (r) => r._field == "temperature")
        |> keep(columns: ["_time", "_value", "sensor_name"])
        |> group() 
        |> aggregateWindow(every: 1mo, fn: sum, createEmpty: false)
        """
        return cls.execute_query_influx(query_api, query, org)

    @classmethod
    def query_group_mean_min_a_influx(cls, query_api, org):
        query = """
        from(bucket: "influx_bucket")
        |> range(start: 0)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> filter(fn: (r) => r.sensor_name == "Sensor A")
        |> filter(fn: (r) => r._field == "temperature")
        |> aggregateWindow(every: 15m, fn: mean, createEmpty: false) // Agrupar e calcular média
        |> group(columns: ["_time"])  // Agrupar por intervalo de tempo
        |> map(fn: (r) => ({ _time: r._time, _value: r._value }))
        """
        return cls.execute_query_influx(query_api, query, org)
    
    @classmethod
    def query_max_min_days_full_influx(cls, query_api, org):
        query = """
        from(bucket: "influx_bucket")
        |> range(start: 2023-01-01T00:00:00Z, stop: 2023-01-10T00:00:00Z)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> filter(fn: (r) => r._field == "temperature")
        |> keep(columns: ["_time", "_value", "sensor_name"])
        |> reduce(
            identity: {max: float(v: "-inf"), min: float(v: "inf")},
            fn: (r, accumulator) => ({
                max: if r._value > accumulator.max then r._value else accumulator.max,
                min: if r._value < accumulator.min then r._value else accumulator.min
            })
        )
        """
        return cls.execute_query_influx(query_api, query, org)

    @classmethod
    def query_count_line_full_influx(cls, query_api, org):
        query = """
            from(bucket: "influx_bucket")
            |> range(start: 0)
            |> filter(fn: (r) => r._measurement == "sensor_data")
            |> count(column: "_value")  
            |> yield(name: "row_count")
        """
        return cls.execute_query_influx(query_api, query, org)

    @classmethod
    def query_influxdb(cls, round_number: int, file_name_query: str) -> None:

        config = configparser.ConfigParser()
        config.read('config.ini')
       
        INFLUX_URL = config['influxdb']['url']
        INFLUX_TOKEN = config['influxdb']['token']
        INFLUX_ORG = config['influxdb']['org']
        INFLUX_BUCKET = config['influxdb']['bucket']
        
        print('### InfluxDB ###')      

        queries = [
            (cls.query_1_year_a_influx, '1_year_a'),
            (cls.query_1_day_full_influx, '1_day_full'),
            (cls.query_group_mean_week_b_influx, 'group_mean_6months_week_b'),
            (cls.query_group_sum_month_a_influx, 'group_sum_month_a'),
            (cls.query_group_mean_min_a_influx, 'group_mean_min_a'),
            (cls.query_max_min_days_full_influx, 'max_min_10days_full'),
            (cls.query_count_line_full_influx, 'count_line_full')
        ]

        try:
            # Abre a conexão uma única vez
            client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
            query_api = client.query_api()

            for query_function, label in queries:
                results, query_time = query_function(query_api, INFLUX_ORG)
                cls.save_metrics("influxdb", query_time, label, round_number, file_name_query)
                print(f"Número de linhas retornadas: {len(results)}")
                print(f"{label} {query_time:.4f} segundos")

        except Exception as e:
            print(f"Erro ao executar queries no InfluxDB: {e}")

        finally:
            # Fecha a conexão corretamente no final
            if 'client' in locals():
                client.close()

