import psycopg2
from supabase import create_client

# Conexão com o PostgreSQL
def connect_postgres():
    conn = psycopg2.connect(
        host="seu host",    
        database="seu banco",
        user="seu user",
        password="sua senha"
    )
    return conn

# Função para buscar dados do PostgreSQL
def fetch_data_from_postgres(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    cur.close()
    return results

# Conexão com o Supabase
def connect_supabase():
    url = "sua url supabase"
    key = "sua key supabase"
    supabase = create_client(url, key)
    return supabase

# Função para inserir dados no Supabase
def insert_data_to_supabase(supabase, table, data):
    for row in data:
        # Verificação para clientes
        if table == "clientes":
            existing = supabase.table(table).select("*").eq("cpf", row["cpf"]).execute()
            if existing.data:
                print(f"Registro já existe em {table}: {row}")
                continue  # pula para o próximo registro após ter sido verificadado 
        #  Verificação para produtos
        elif table == "produtos":
            existing = supabase.table(table).select("*").eq("nome", row["nome"]).execute()
            if existing.data:
                print(f"Registro já existe em {table}: {row}")
                continue # pula para o próximo registro após ter sido verificadado 
        #  Verificação para vendas
        elif table == "vendas":
            existing = supabase.table(table).select("*").eq("cod_produto", row["cod_produto"]).execute()
            if existing.data:
                print(f"Registro já existe em {table}: {row}")
                continue # pula para o próximo registro após ter sido verificadado 

        # Inserção do novo registro
        res = supabase.table(table).insert(row).execute()
        print(f"Inserido em {table}: {row}")






