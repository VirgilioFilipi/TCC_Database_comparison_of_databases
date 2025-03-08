# 📊 Comparação de Bancos de Dados: MariaDB vs. InfluxDB

Este repositório contém a implementação do estudo **"Avaliação de Bancos de Dados para Séries Temporais: Um Estudo Comparativo entre MariaDB e InfluxDB"**, desenvolvido como parte do Trabalho de Conclusão de Curso (TCC) em Engenharia de Telecomunicações no **Instituto Federal de Santa Catarina (IFSC)**.

O objetivo do projeto é comparar o desempenho dos bancos de dados **MariaDB** e **InfluxDB** em termos de:
- 🔹 **Tempo de inserção**
- 🔹 **Tempo de consulta**
- 🔹 **Uso de armazenamento**

---

## 📖 Resumo

O avanço da inteligência artificial e o crescimento do uso de dispositivos IoT aumentaram significativamente a necessidade de armazenamento eficiente de séries temporais. Bancos de dados relacionais tradicionais, como o **MariaDB**, apresentam desafios ao lidar com esse tipo de dado devido à sua estrutura, impactando o desempenho em operações de inserção e consulta à medida que o volume de dados cresce. 

Este estudo compara o **tempo de inserção**, **tempo de recuperação de dados** e **uso de armazenamento** entre bancos relacionais e especializados em séries temporais, analisando especificamente o **MariaDB** e o **InfluxDB**. Foram definidas métricas para avaliar a eficiência dessas soluções, fornecendo uma análise comparativa que destaca as vantagens e desvantagens de cada uma. 

Os resultados indicam que o **MariaDB** apresenta limitações no gerenciamento de grandes volumes de dados, especialmente quando a estrutura e os índices não são otimizados. Em contrapartida, o **InfluxDB**, projetado para séries temporais, oferece melhor gerenciamento e otimização do espaço de armazenamento, tornando-se uma alternativa mais eficiente para aplicações que dependem de grandes volumes de dados temporais.

---

## 📌 Conteúdo do Repositório

O projeto está organizado em diferentes módulos, cada um responsável por uma etapa específica do experimento:

📂 **`src/`** - Implementação principal do estudo:
- [`main.py`](main.py) - Script principal que executa os experimentos.
- [`insert_database.py`](src/insert_database.py) - Insere dados nos bancos MariaDB e InfluxDB.
- [`query_database.py`](src/query_database.py) - Executa consultas nos bancos.
- [`function_query.py`](src/function_query.py) - Funções auxiliares para consultas.
- [`save_data.py`](src/save_data.py) - Salva métricas de tempo de inserção e consulta.
- [`table_manager.py`](src/table_manager.py) - Gerencia a criação das tabelas nos bancos.

📂 **`output/`** - Resultados dos testes:
- `insertion_times.csv` - Resultados das inserções.
- `query_times.csv` - Resultados das consultas.

📄 **`config.ini`** - Arquivo de configuração dos bancos de dados.

📄 **`requirements.txt`** - Lista de dependências do projeto.

---

## 🏗️ Configuração do Ambiente

### 1️⃣ **Clonar o Repositório e Acessar a Pasta**
```bash
git clone https://github.com/VirgilioFilipi/TCC_Database_comparison_of_databases.git
cd TCC_Database_comparison_of_databases
```

---

### 2️⃣ **Criar e Ativar um Ambiente Virtual**

#### 🔹 Para Linux/macOS:
```bash
python3 -m venv venv
source venv/bin/activate
```

#### 🔹 Para Windows (PowerShell):
```powershell
python -m venv venv
venv\Scripts\Activate
```

---

### 3️⃣ **Instalar as Dependências**
Após ativar o ambiente virtual, instale os pacotes necessários:
```bash
pip install -r requirements.txt