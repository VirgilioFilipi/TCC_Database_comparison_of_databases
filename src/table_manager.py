import subprocess
import pymysql
import configparser
import csv
import json
from influxdb_client import InfluxDBClient, Point


class TableManager:
    """
    Classe responsável pela criação e gerenciamento de tabelas em diferentes bancos de dados (MariaDB e InfluxDB).
    """

    def __init__(self):
        """Inicializa as credenciais de acesso aos bancos de dados a partir do arquivo config.ini."""
        config = configparser.ConfigParser()
        config.read("config.ini")

        self.credentials = {
            db_name: {
                "host": config.get("mariadb", f"{db_name}_host"),
                "port": config.getint("mariadb", f"{db_name}_port"),
            }
            for db_name in [
                "mariadb_columnstore",
                "mariadb_innodb",
                "mariadb_innodb_optimized",
                "mariadb_myrocks",
            ]
        }

        self.user = config.get("database", "user")
        self.password = config.get("database", "password")
        self.influx_url = config.get("influxdb", "url")
        self.influx_token = config.get("influxdb", "token")
        self.influx_org = config.get("influxdb", "org")
        self.influx_bucket = config.get("influxdb", "bucket")

    def create_all_tables(self) -> list:
        """
        Cria todas as tabelas dos bancos de dados MariaDB e InfluxDB.

        Returns:
            list: Lista com os status de cada criação de tabela.
        """
        file_name = "output/insertion_times.csv"
        header = ["table_name", "insertion_time", "current_week", "round_number", "ram_usage", "swap_usage", "storage"]

        with open(file_name, mode="w", newline="") as file:
            csv.writer(file).writerow(header)

        results = []
        results.append(self.create_influx_database())

        for db_name in self.credentials.keys():
            results.append(self.create_table(db_name))

        return results

    def create_table(self, db_name: str) -> str:
        """
        Cria um banco de dados e sua tabela associada.

        Args:
            db_name (str): Nome do banco de dados a ser criado.

        Returns:
            str: Mensagem de sucesso ou erro.
        """
        print(f"----------------------\nCriando {db_name}")

        try:
            creds = self.credentials[db_name]
            with pymysql.connect(host=creds["host"], port=creds["port"], user=self.user, password=self.password) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SHOW DATABASES LIKE '{db_name}'")
                    if cursor.fetchone():
                        print(f"Banco '{db_name}' já existe. Apagando e recriando...")
                        cursor.execute(f"DROP DATABASE {db_name}")
                    cursor.execute(f"CREATE DATABASE {db_name}")
                    print(f"Banco '{db_name}' criado.")

            with pymysql.connect(host=creds["host"], port=creds["port"], user=self.user, password=self.password, database=db_name) as conn:
                with conn.cursor() as cursor:
                    print(f"Tentando criar a tabela 'sensor_data' em {db_name}...")
                    cursor.execute(self.get_table_schema(db_name))
                    print("Tabela 'sensor_data' criada ou já existia.")

            size = self.get_docker_volume_size_by_container(db_name)
            print(f"*********************\n{db_name} {size}\n*********************")
            return f"Banco {db_name} criado com sucesso."

        except pymysql.MySQLError as e:
            return f"Erro ao criar banco de dados ou tabela {db_name}: {e}"

    def get_table_schema(self, db_name: str) -> str:
        """
        Retorna o esquema SQL adequado para cada banco de dados.

        Args:
            db_name (str): Nome do banco de dados.

        Returns:
            str: Comando SQL para criação da tabela.
        """
        schemas = {
            "mariadb_columnstore": """
                CREATE TABLE sensor_data (
                    event_timestamp TIMESTAMP NOT NULL,
                    temperature FLOAT(4) NOT NULL,
                    sensor_name VARCHAR(10) NOT NULL
                ) ENGINE=ColumnStore;
            """,
            "mariadb_innodb": """
                CREATE TABLE IF NOT EXISTS sensor_data (
                    event_timestamp TIMESTAMP NOT NULL,
                    temperature FLOAT(4) NOT NULL,
                    sensor_name VARCHAR(10) NOT NULL
                ) ENGINE=InnoDB;
            """,
            "mariadb_innodb_optimized": """
                CREATE TABLE sensor_data (
                    event_timestamp TIMESTAMP NOT NULL,
                    temperature FLOAT(4) NOT NULL,
                    sensor_name VARCHAR(10) NOT NULL,
                    year_number INT NOT NULL,
                    PRIMARY KEY (year_number, sensor_name, event_timestamp)
                ) ENGINE=InnoDB;
            """,
            "mariadb_myrocks": """
                CREATE TABLE sensor_data (
                    event_timestamp TIMESTAMP NOT NULL,
                    temperature FLOAT(4) NOT NULL,
                    sensor_name VARCHAR(10) NOT NULL,
                    year_number INT NOT NULL,
                    PRIMARY KEY (year_number, sensor_name, event_timestamp)
                ) ENGINE=ROCKSDB;
            """,
        }

        return schemas.get(db_name, "")

    def create_influx_database(self) -> str:
        """
        Cria um bucket no InfluxDB.

        Returns:
            str: Mensagem de sucesso ou erro.
        """
        print("----------------------\nCriando InfluxDB")
        try:
            client = InfluxDBClient(url=self.influx_url, token=self.influx_token, org=self.influx_org)
            buckets_api = client.buckets_api()

            existing_bucket = buckets_api.find_bucket_by_name(self.influx_bucket)
            if existing_bucket:
                print(f"Bucket '{self.influx_bucket}' já existe. Excluindo...")
                buckets_api.delete_bucket(existing_bucket.id)

            print(f"Criando bucket '{self.influx_bucket}'...")
            buckets_api.create_bucket(bucket_name=self.influx_bucket, org=self.influx_org)
            print(f"Bucket '{self.influx_bucket}' criado com sucesso.")
            client.close()

            return f"Bucket {self.influx_bucket} criado com sucesso."

        except Exception as e:
            return f"Erro ao criar bucket no InfluxDB: {e}"

    @staticmethod
    def get_docker_volume_size_by_container(container_name: str) -> str:
        """
        Retorna o tamanho total dos volumes Docker associados a um container.

        Args:
            container_name (str): Nome do container Docker.

        Returns:
            str: Tamanho do volume ou mensagem de erro.
        """
        try:
            inspect_result = subprocess.run(["docker", "inspect", container_name], capture_output=True, text=True)
            if inspect_result.returncode != 0:
                return f"Erro ao inspecionar container: {inspect_result.stderr.strip()}"

            container_info = json.loads(inspect_result.stdout)
            mounts = container_info[0].get("Mounts", [])
            if not mounts:
                return "Nenhum volume encontrado."

            total_size = sum(int(subprocess.run(["sudo", "du", "-sb", mount.get("Source")], capture_output=True, text=True).stdout.strip().split()[0]) for mount in mounts if mount.get("Source"))
            return TableManager.convert_size(total_size)

        except Exception as e:
            return f"Erro ao calcular tamanho do volume: {e}"

    @staticmethod
    def convert_size(size_in_bytes: int) -> str:
        """
        Converte bytes para formato legível (KB, MB, GB).

        Args:
            size_in_bytes (int): Tamanho em bytes.

        Returns:
            str: Tamanho formatado.
        """
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_in_bytes < 1024:
                return f"{size_in_bytes:.2f} {unit}"
            size_in_bytes /= 1024
