from src.save_data import SaveData
import pymysql
import time
from datetime import datetime
import psutil  
import subprocess
import json
import configparser
from influxdb_client import InfluxDBClient, Point

class InsertDatabase:
    """Classe para inserir dados em diferentes bancos de dados e salvar o tempo de inserção em um arquivo CSV."""

    @classmethod
    def get_docker_volume_size_by_container(cls, container_name: str) -> str:
        """
        Retorna o tamanho total dos volumes Docker associados a um container.
        
        Args:
            container_name (str): Nome ou ID do container.
            
        Returns:
            str: Tamanho total dos volumes associados ou mensagem de erro.
        """
        try:
            # Inspecionar o container
            inspect_result = subprocess.run(
                ["docker", "inspect", container_name],
                capture_output=True,
                text=True,
            )
            
            if inspect_result.returncode != 0:
                return f"Erro ao inspecionar o container: {inspect_result.stderr.strip()}"
            
            container_info = json.loads(inspect_result.stdout)
            
            # Obter os volumes montados
            mounts = container_info[0].get("Mounts", [])
            if not mounts:
                return "Nenhum volume encontrado para este container."
            
            total_size = 0
            sizes = {}
            
            # Calcular o tamanho de cada volume
            for mount in mounts:
                source_path = mount.get("Source")
                if not source_path:
                    continue
                
                du_result = subprocess.run(
                    ["sudo", "du", "-sb", source_path],
                    capture_output=True,
                    text=True,
                )                
                if du_result.returncode != 0:
                    return f"Erro ao calcular tamanho do volume {source_path}: {du_result.stderr.strip()}"
                
                size_in_bytes = int(du_result.stdout.strip().split()[0])
                sizes[mount["Destination"]] = size_in_bytes
                total_size += size_in_bytes
            
            # Converter o tamanho total para formato legível
            readable_size = cls.convert_size(total_size)
            
            # Montar a resposta detalhada
            details = "\n".join([f"{dest}: {cls.convert_size(size)}" for dest, size in sizes.items()])
            return readable_size
        
        except Exception as e:
            return f"Erro ao calcular tamanho dos volumes: {e}"

    @staticmethod
    def convert_size(size_in_bytes: int) -> str:
        """
        Converte um tamanho em bytes para um formato legível (KB, MB, GB).
        
        Args:
            size_in_bytes (int): Tamanho em bytes.
            
        Returns:
            str: Tamanho em formato legível.
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_in_bytes < 1024:
                return f"{size_in_bytes:.2f} {unit}"
            size_in_bytes /= 1024
        
    @staticmethod
    def get_docker_volume_size_influxdb(volume_name: str) -> str:
        """Retorna o tamanho do volume Docker em formato legível."""
        try:
            result = subprocess.run(
                ["docker", "run", "--rm", "-v", f"{volume_name}:/data", "alpine", "du", "-sh", "/data"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                # Captura a parte antes de " /data"
                size = result.stdout.strip().split()[0]
                return size
            else:
                return f"Erro: {result.stderr.strip()}"
        except Exception as e:
            return f"Erro ao calcular tamanho do volume: {e}"

    @classmethod
    def insert_mariadb( cls,
                        db_name: str, 
                        engine: str, 
                        round_number: int, 
                        batch_size: int, 
                        data_to_insert: list, 
                        current_week: int, 
                        file_name_insertion: str,
                        port: int
                        ) -> None:

        db_config = cls.load_db_config()  # Carregar usuário e senha do config.ini
        conn = pymysql.connect(
            host=db_config["host"],
            port=port,
            user=db_config["user"],
            password=db_config["password"],
            database=db_name
        )
        cursor = conn.cursor()
        table_name = "sensor_data"

        table_size_before = cls.get_docker_volume_size_by_container(db_name)
        print('*********************')
        print(db_name, table_size_before)
        print('*********************')

            
        start_time = time.time()
        for i in range(0, len(data_to_insert), batch_size):
            batch = data_to_insert[i:i + batch_size]
            cursor.executemany(
                f"INSERT INTO {table_name} (event_timestamp, temperature, sensor_name) VALUES (%s, %s, %s)", 
                batch
            )
            conn.commit()

        end_time = time.time()
        insertion_time = end_time - start_time
        print(f"Tempo de inserção no {engine}: {insertion_time} segundos")

        table_size_before = cls.get_docker_volume_size_by_container(db_name)
        print(f"Tamanho da tabela '{table_name}' antes da inserção: {table_size_before} MB")

        memory_info = psutil.virtual_memory()
        swap_info = psutil.swap_memory()

        ram_usage = memory_info.used / (1024 ** 3)  # Converte para GB
        swap_usage = swap_info.used / (1024 ** 3)  # Converte para GB

        SaveData.save_insertion_time_to_csv(db_name, insertion_time, current_week, round_number, ram_usage, swap_usage, table_size_before, file_name_insertion)

        cursor.close()
        conn.close()

    @classmethod
    def insert_mariadb_structured( cls,
                        db_name: str, 
                        engine: str, 
                        round_number: int, 
                        batch_size: int, 
                        data_to_insert: list, 
                        current_week: int, 
                        file_name_insertion: str,
                        port: int
    ) -> None:
        """
        Insere dados estruturados com informações de mês, ano e semana em um banco de dados MariaDB.
        
        Args:
            db_name (str): Nome do banco de dados.
            engine (str): Nome do mecanismo de banco de dados.
            round_number (int): Número da rodada de inserção.
            batch_size (int): Tamanho do lote de inserção.
            data_to_insert (list): Lista de dados para inserir.
            current_week (int): Semana atual.
            file_name_insertion (str): Nome do arquivo CSV para salvar os dados de inserção.
        
        Returns:
            None
        """
        db_config = cls.load_db_config()  # Carregar usuário e senha do config.ini
        conn = pymysql.connect(
            host=db_config["host"],
            port=port,
            user=db_config["user"],
            password=db_config["password"],
            database=db_name
        )
        cursor = conn.cursor()
        table_name = "sensor_data"
    
        data_with_month_year_week = []
        for row in data_to_insert:
            event_timestamp = row[0]
            date_obj = datetime.strptime(event_timestamp, '%Y-%m-%d %H:%M:%S')
            year_number = date_obj.year
            data_with_month_year_week.append((event_timestamp, row[1], row[2], year_number))

        table_size_before = cls.get_docker_volume_size_by_container(db_name)
        print('*********************')
        print(db_name, table_size_before)
        print('*********************')

        start_time = time.time()
        for i in range(0, len(data_with_month_year_week), batch_size):
            batch = data_with_month_year_week[i:i + batch_size]
            cursor.executemany(
                f"INSERT INTO {table_name} (event_timestamp, temperature, sensor_name, year_number) "
                f"VALUES (%s, %s, %s, %s)", 
                batch
            )
            conn.commit()

        end_time = time.time()
        insertion_time = end_time - start_time
        print(f"Tempo de inserção no {db_name}: {insertion_time} segundos")

        table_size_before = cls.get_docker_volume_size_by_container(db_name)
        print(f"Tamanho da tabela '{table_name}' antes da inserção: {table_size_before} MB")

        memory_info = psutil.virtual_memory()
        swap_info = psutil.swap_memory()

        ram_usage = memory_info.used / (1024 ** 3)  # Converte para GB
        swap_usage = swap_info.used / (1024 ** 3)  # Converte para GB

        SaveData.save_insertion_time_to_csv(db_name, insertion_time, current_week, round_number, ram_usage, swap_usage, table_size_before, file_name_insertion)

        cursor.close()
        conn.close()

    @classmethod
    def insert_influxdb(cls,
            round_number: int, 
            batch_size: int, 
            data_to_insert: list, 
            current_week: tuple, 
            file_name_insertion: str
        ) -> None:

        config = configparser.ConfigParser()
        config.read('config.ini')

        influx_url = config.get("influxdb", "url")
        influx_token = config.get("influxdb", "token")
        influx_org = config.get("influxdb", "org")
        influx_bucket = config.get("influxdb", "bucket")

        client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
        write_api = client.write_api()
        start_time = time.time()

        try:
            for i in range(0, len(data_to_insert), batch_size):
                batch = data_to_insert[i:i + batch_size]
                points = []

                for record in batch:
                    # Verifique se o registro contém as chaves esperadas
                    if "measurement" not in record or "fields" not in record or "time" not in record:
                        print(f"Registro inválido: {record}")
                        continue  # Pule o registro inválido

                    # Criação de um ponto para cada registro no lote
                    point = Point(record["measurement"]) \
                        .tag("week", f"{current_week[0]}-{current_week[1]}") \
                        .time(record["time"])

                    # Adiciona as tags
                    for tag_key, tag_value in record.get("tags", {}).items():
                        point = point.tag(tag_key, tag_value)

                    # Adiciona os campos
                    for field_key, field_value in record["fields"].items():
                        point = point.field(field_key, field_value)

                    points.append(point)

                # Envio dos pontos para o InfluxDB
                if points:  # Certifique-se de que há pontos válidos antes de escrever
                    write_api.write(bucket=influx_bucket, org=influx_org, record=points)
                    # print(f"Inserido lote de {len(points)} pontos no bucket '{influx_bucket}'.")

            # Tempo de inserção
            end_time = time.time()
            insertion_time = end_time - start_time
            print(f"Tempo de inserção no InfluxDB: {insertion_time:.2f} segundos")

            bucket_size = cls.get_docker_volume_size_influxdb('influxdb-data')
            memory_info = psutil.virtual_memory()
            swap_info = psutil.swap_memory()

            ram_usage = memory_info.used / (1024 ** 3)  # Converte para GB
            swap_usage = swap_info.used / (1024 ** 3)  # Converte para GB
            
            SaveData.save_insertion_time_to_csv('InfluxDB', insertion_time, current_week, round_number, ram_usage, swap_usage, bucket_size, file_name_insertion)

            client.close()

        except Exception as e:
            print(f"Erro ao inserir dados no InfluxDB: {e}")

        finally:
            write_api.__del__()  # Fecha a conexão com a API de escrita

    @staticmethod
    def load_db_config():
        config = configparser.ConfigParser()
        config.read("config.ini")

        return {
            "host": config.get("database", "host", fallback="localhost"),
            "user": config.get("database", "user"),
            "password": config.get("database", "password")
        }


