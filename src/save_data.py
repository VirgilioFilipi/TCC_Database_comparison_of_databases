import csv

class SaveData:
    """Classe para salvar tempos de inserção e consulta em arquivos CSV."""

    @staticmethod
    def save_insertion_time_to_csv(
        table_name: str, 
        insertion_time: float, 
        current_week: int, 
        round_number: int, 
        ram_usage: int,
        swap_usage: int,
        table_size_before: int,
        file_name_insertion: str
    ) -> None:
        """
        Salva o tempo de inserção em um arquivo CSV.
        """
        with open(file_name_insertion, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([table_name, insertion_time, current_week, round_number, ram_usage, swap_usage, table_size_before])

    @staticmethod
    def save_query_time_to_csv(
        table_name: str, 
        query_time: float, 
        query_type: str, 
        round_number: int, 
        file_name_query: str,
        ram_usage: int,
        swap_usage: int
    ) -> None:
        """
        Salva o tempo de consulta em um arquivo CSV.
        """
        with open(file_name_query, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([table_name, query_time, query_type, round_number, ram_usage, swap_usage])
