import sys
import threading
import time
from PIL import Image, ImageDraw
from pystray import MenuItem, Icon
from database import connect_postgres, fetch_data_from_postgres, connect_supabase, insert_data_to_supabase

# Criar um evento para controlar a migração
stop_event = threading.Event()

# Função para criar um ícone
def create_blue_circle_icon():
    width, height = 64, 64  # Tamanho do ícone
    image = Image.new("RGBA", (width, height), (255, 255, 255, 0))  # Fundo
    draw = ImageDraw.Draw(image)
    draw.ellipse((0, 0, width, height), fill=(0, 0, 255))  # Círculo
    return image

def migrate_data():
    while not stop_event.is_set():  # Verifica se o evento foi sinalizado
        try:
            # Conectar ao PostgreSQL
            conn_postgres = connect_postgres()

            # Buscar dados das tabelas
            clientes = fetch_data_from_postgres(conn_postgres, "SELECT * FROM clientes;")
            produtos = fetch_data_from_postgres(conn_postgres, "SELECT * FROM produtos;")
            vendas = fetch_data_from_postgres(conn_postgres, "SELECT * FROM vendas;")

            # Filtrar os dados para corresponder às colunas do Supabase
            filtered_clientes = [{"nome": row[1], "cpf": row[2], "endereco": row[3]} for row in clientes]
            filtered_produtos = [{"nome": row[1], "valor": float(row[2]), "quantidade": row[3]} for row in produtos]
            filtered_vendas = [{"cod_produto": row[1], "valor_total": float(row[2]), "cod_cliente": row[3]} for row in vendas]

            # Conectar ao Supabase
            supabase = connect_supabase()

            # Migrar dados para o Supabase
            insert_data_to_supabase(supabase, "clientes", filtered_clientes)
            insert_data_to_supabase(supabase, "produtos", filtered_produtos)
            insert_data_to_supabase(supabase, "vendas", filtered_vendas)

            # Fechar conexão com PostgreSQL
            conn_postgres.close()

            # tempo antes de executar novamente
            time.sleep(60)  # A cada hora (60 segundos)

        except Exception as e:
            print(f"Ocorreu um erro: {e}")

def run_migration():
    thread = threading.Thread(target=migrate_data)
    thread.start()

def on_quit(icon, item):
    stop_event.set()  # Sinaliza para parar a migração
    icon.stop()
    sys.exit()

# Criar um ícone de bandeja
icon = Icon("migracao_icon", create_blue_circle_icon())
icon.menu = (
    MenuItem("Iniciar Migração", run_migration),
    MenuItem("Sair", on_quit)
)

icon.run()
