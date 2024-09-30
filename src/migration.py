from database import connect_postgres, fetch_data_from_postgres, connect_supabase, insert_data_to_supabase

def migrate_data():
    # Conectar ao PostgreSQL
    conn_postgres = connect_postgres()

    # Buscar dados das tabelas
    clientes = fetch_data_from_postgres(conn_postgres, "SELECT * FROM clientes;")
    produtos = fetch_data_from_postgres(conn_postgres, "SELECT * FROM produtos;")
    vendas = fetch_data_from_postgres(conn_postgres, "SELECT * FROM vendas;")

    # Filtrar os dados para corresponder às colunas do Supabase
    filtered_clientes = [{"nome": row[1], "cpf": row[2], "endereco": row[3]} for row in clientes]
    filtered_produtos = [{"nome": row[1], "valor": float(row[2]), "quantidade": row[3]} for row in produtos]  # Converte Decimal para float
    filtered_vendas = [{"cod_produto": row[1], "valor_total": float(row[2]), "cod_cliente": row[3]} for row in vendas]  # Converte Decimal para float

    # Conectar ao Supabase
    supabase = connect_supabase()

    # Migrar dados para o Supabase
    insert_data_to_supabase(supabase, "clientes", filtered_clientes)
    insert_data_to_supabase(supabase, "produtos", filtered_produtos)
    insert_data_to_supabase(supabase, "vendas", filtered_vendas)

    # Fechar conexão com PostgreSQL
    conn_postgres.close()

if __name__ == "__main__":
    migrate_data()
