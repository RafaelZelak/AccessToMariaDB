import os
import pyodbc
import mysql.connector

# Lista de arquivos .mdb
mdb_files = [
    "./pipelines.mdb",
    "./tb_cobrancas_t.mdb",
    "./tb_receber.mdb",
    "./tb_receber_mensagens.mdb"
    ]

# Configurações de conexão com MariaDB
maria_conn = mysql.connector.connect(
    host="192.168.15.237",
    user="digitalup",
    password="kFxHgO5^59cZ7ff",
    database="sistema_financeiro"
)
maria_cursor = maria_conn.cursor()

for mdb_path in mdb_files:
    if not os.path.isfile(mdb_path):
        print(f"Arquivo não encontrado: {mdb_path}")
        continue

    print(f"\nIniciando importação do arquivo: {mdb_path}")

    # Conectar ao arquivo MDB
    conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={mdb_path};"
    mdb_conn = pyodbc.connect(conn_str)
    mdb_cursor = mdb_conn.cursor()

    # Obter lista de tabelas do MDB
    mdb_tables = [row.table_name for row in mdb_cursor.tables() if row.table_type == "TABLE"]

    for table in mdb_tables:
        print(f"\nPopulando tabela `{table}`...")

        # Obter colunas da tabela MDB
        mdb_cursor.execute(f"SELECT * FROM {table} WHERE 1=0")
        mdb_columns = [column[0] for column in mdb_cursor.description]

        # Obter colunas reais do MariaDB
        maria_cursor.execute(f"DESCRIBE `{table}`")
        maria_columns = [row[0] for row in maria_cursor.fetchall()]

        # Mapear colunas MDB para colunas MariaDB (remover acentos e normalizar)
        column_mapping = {mdb_col.lower().replace('á', 'a').replace('ú', 'u').replace('í', 'i'): maria_col
                          for mdb_col, maria_col in zip(mdb_columns, maria_columns)}

        # Selecionar todos os dados da tabela MDB
        mdb_cursor.execute(f"SELECT * FROM {table}")
        rows = mdb_cursor.fetchall()

        # Montar a query INSERT para MariaDB usando as colunas mapeadas
        column_names = ", ".join(f"`{column_mapping[mdb_col.lower().replace('á', 'a').replace('ú', 'u').replace('í', 'i')]}`" for mdb_col in mdb_columns)
        placeholders = ", ".join(["%s"] * len(mdb_columns))
        insert_query = f"INSERT INTO `{table}` ({column_names}) VALUES ({placeholders})"

        # Inserir os dados no MariaDB com logs de progresso
        total_inserted = 0
        for row in rows:
            try:
                maria_cursor.execute(insert_query, tuple(row))  # Converter Row para tupla
                total_inserted += 1
                if total_inserted % 100 == 0:  # Exibir log a cada 100 inserções
                    print(f"  Dados enviados: {total_inserted}")
            except Exception as e:
                print(f"Erro ao inserir dados na tabela {table}: {e}")

        # Confirmar a transação após inserir os dados de cada tabela
        maria_conn.commit()
        print(f"Tabela `{table}` populada com sucesso. Total de registros inseridos: {total_inserted}")

    # Fechar conexão com o arquivo MDB
    mdb_cursor.close()
    mdb_conn.close()

# Fechar conexão com MariaDB
maria_cursor.close()
maria_conn.close()

print("\nImportação concluída com sucesso!")
