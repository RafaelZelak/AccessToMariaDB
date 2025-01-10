import os
import pyodbc

def get_table_names(mdb_path):
    """Obter nomes de todas as tabelas de um arquivo .mdb"""
    conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={mdb_path};"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    tables = [row.table_name for row in cursor.tables() if row.table_type == "TABLE"]

    conn.close()
    return tables

def get_mdb_schema(mdb_path):
    """Gerar queries de CREATE TABLE para um arquivo .mdb"""
    conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={mdb_path};"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    tables = [row.table_name for row in cursor.tables() if row.table_type == "TABLE"]
    create_table_queries = []

    odbc_type_mapping = {
        -7: "TINYINT",      # SQL_BIT
        -6: "TINYINT",      # SQL_TINYINT
        -5: "BIGINT",       # SQL_BIGINT
        4: "INT",           # SQL_INTEGER
        5: "SMALLINT",      # SQL_SMALLINT
        6: "FLOAT",         # SQL_FLOAT
        7: "REAL",          # SQL_REAL
        8: "DOUBLE",        # SQL_DOUBLE
        12: "VARCHAR(255)", # SQL_VARCHAR
        91: "DATE",         # SQL_DATE
        93: "DATETIME"      # SQL_TIMESTAMP
    }

    for table in tables:
        cursor.execute(f"SELECT * FROM {table} WHERE 1=0")  # Retornar apenas o esquema, sem dados
        columns = cursor.description

        seen_columns = set()  # Usado para verificar duplicatas
        sql_columns = []
        for column in columns:
            name = column[0]
            if name in seen_columns:
                continue  # Ignorar colunas duplicadas
            seen_columns.add(name)

            data_type = column[1]
            sql_type = odbc_type_mapping.get(data_type, "TEXT")  # Padrão para tipos não mapeados

            sql_columns.append(f"`{name}` {sql_type}")

        create_table_query = f"CREATE TABLE `{table}` (\n  {',\n  '.join(sql_columns)}\n);"
        create_table_queries.append(create_table_query)

    conn.close()
    return create_table_queries


if __name__ == "__main__":
    # Lista de todos os arquivos .mdb, incluindo o principal
    mdb_files = [
        "./database.mdb",
        "./database_mensagens.mdb",
        "./pipelines.mdb",
        "./tb_cobrancas_t.mdb",
        "./tb_receber.mdb",
        "./tb_receber_mensagens.mdb"
    ]

    all_tables = {}  # Dicionário para armazenar todas as tabelas e seus arquivos
    log_output = []  # Lista para armazenar o log final
    all_queries = []  # Lista para armazenar todas as queries geradas

    for mdb_path in mdb_files:
        print(f"\nAnalisando o arquivo: {mdb_path}")
        log_output.append(f"\nAnalisando o arquivo: {mdb_path}")

        # Obter tabelas do arquivo atual
        current_tables = get_table_names(mdb_path)

        for table in current_tables:
            if table in all_tables:
                # Se a tabela já foi encontrada em outro arquivo, logar como duplicada
                log_output.append(f"Tabela duplicada encontrada: `{table}` (já existe no arquivo {all_tables[table]})")
            else:
                # Adicionar tabela ao dicionário
                all_tables[table] = mdb_path

        # Gerar queries para todas as tabelas do arquivo atual
        queries = get_mdb_schema(mdb_path)
        all_queries.extend(queries)

    # Adicionar todas as queries ao log
    log_output.append("\nQueries de CREATE TABLE geradas:")
    log_output.extend(all_queries)

    # Salvar o log em um arquivo txt
    with open("./output_log.txt", "w", encoding="utf-8") as file:
        file.write("\n".join(log_output))

    print("Processo concluído. O log foi salvo em 'output_log.txt'.")
