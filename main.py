import requests
import time
from datetime import datetime, timedelta
import hashlib
import re
import csv
import os
import sqlite3
from urllib.parse import urljoin

# --- Configurações ---
URL_BASE = 'https://the-eye.eu/public/'
MAX_LIMIT_TIMEOUT = 3600
MAX_CONNECT_TIMEOUT = 60
HOURS_TO_WAIT = 1
SECONDS_TO_WAIT = HOURS_TO_WAIT * 3600
REQUEST_DELAY = 5 # Delay de 5 segundos entre as consultas HTTP

# Arquivos e Diretórios
DB_NAME = 'scraper_data.db'
CONTROL_FILE = 'last_cycle_timestamp.txt' 
DUMP_DIR = 'url_dumps' 

# Variável global para rastrear o timestamp do ciclo de dump atual
current_dump_timestamp = 0 

# --- Funções de Banco de Dados SQLite ---

def setup_db():
    """Cria a tabela de URLs se ela não existir, incluindo a coluna 'file_path'."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS urls (
            id INTEGER PRIMARY KEY,
            url TEXT UNIQUE NOT NULL,
            sha256 TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            is_directory INTEGER NOT NULL,
            file_path TEXT 
        )
    ''')
    conn.commit()
    conn.close()

def insert_url(url, current_timestamp, is_directory):
    """Insere uma URL e seu path no banco de dados, ignorando duplicatas."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    sha256_hash = hashlib.sha256(url.encode('utf-8')).hexdigest()
    
    # Extrai o caminho (path)
    if url.startswith(URL_BASE):
        file_path = url[len(URL_BASE):]
    else:
        file_path = url 
    
    try:
        cursor.execute('''
            INSERT INTO urls (url, sha256, timestamp, is_directory, file_path)
            VALUES (?, ?, ?, ?, ?)
        ''', (url, sha256_hash, current_timestamp, is_directory, file_path))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def was_url_visited(url):
    """VERIFICAÇÃO CHAVE: Verifica se a URL já foi inserida no banco de dados."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM urls WHERE url = ?", (url,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

# --- Funções de Controle de Ciclo ---

def get_last_cycle_time():
    """Lê o timestamp do último ciclo salvo no arquivo de controle."""
    if os.path.exists(CONTROL_FILE):
        with open(CONTROL_FILE, 'r') as f:
            try:
                return int(f.read().strip())
            except ValueError:
                return 0
    return 0

def save_last_cycle_time(timestamp):
    """Salva o timestamp do início do novo ciclo."""
    with open(CONTROL_FILE, 'w') as f:
        f.write(str(timestamp))

# --- Funções de Scraping Recursivo ---

def extract_urls(html_content, base_url):
    """
    Extrai URLs, distinguindo entre diretórios (terminados em /) e arquivos.
    Retorna (directories, files).
    """
    # Usando BeautifulSoup seria mais robusto, mas o regex funciona para índices de diretório.
    links = re.findall(r'<a.*?href=["\'](.*?)(?:["\'].*?>)', html_content, re.IGNORECASE)
    
    directories = set()
    files = set()
    
    for link in links:
        if link in ('', '/', '../', './', '#'):
            continue
        
        full_url = urljoin(base_url, link)
        
        if not full_url.startswith(URL_BASE):
            continue
            
        if link.endswith('/'):
            directories.add(full_url)
        else:
            files.add(full_url)
    
    return sorted(list(directories)), sorted(list(files))


def recursive_scrape(url):
    """
    Função recursiva para navegar e salvar URLs no DB.
    Usa o banco de dados para checar se deve pular a URL.
    """
    global current_dump_timestamp 
    
    # 1. VERIFICAÇÃO DE CONTINUIDADE
    if was_url_visited(url):
        # Se o diretório já está no DB, ele foi processado. Pula a requisição HTTP e a exploração.
        print(f"[Skip] Diretório já processado (DB): {url}")

    insert_url(url, current_dump_timestamp, 1)

    print(f"[Scraping] Buscando em: {url}")
    
    try:

        continuar_tentativa = 0;
        while continuar_tentativa < 3:
            try:
                response = requests.get(
                    url,
                    timeout=(MAX_CONNECT_TIMEOUT, MAX_LIMIT_TIMEOUT)
                )
                response.raise_for_status()

                # --- DELAY PARA EVITAR BLOQUEIO ---
                time.sleep(REQUEST_DELAY)
                print(f"[Delay] Aguardando {REQUEST_DELAY}s...")

                break
            except requests.exceptions.RequestException as e:
                print(f"ERRO ao acessar {url}: {e}. Continuando...")
                print(f"tentativa {continuar_tentativa} ...")
                continuar_tentativa = continuar_tentativa + 1;

        if continuar_tentativa >= 3:
            return;
        # ---------------------------------------------
        
        discovered_directories, discovered_files = extract_urls(response.text, url)

        # 1. Salva os arquivos (is_directory = 0)
        for file_url in discovered_files:
            # Não verifica no DB antes de inserir arquivos; apenas insere.
            # A checagem UNIQUE no DB cuidará das duplicatas.
            insert_url(file_url, current_dump_timestamp, 0)
        
        # 2. Chama a recursão para os diretórios
        for dir_url in discovered_directories:
            # Chama a recursão que fará a checagem "was_url_visited" no próximo passo.
            recursive_scrape(dir_url)
            
    except requests.exceptions.RequestException as e:
        print(f"ERRO ao acessar {url}: {e}. Continuando...")


# --- Funções de Dump CSV ---

def dump_all_urls_to_csv():
    """Exporta TODOS os dados ATUAIS do banco de dados para um NOVO arquivo CSV, incluindo o file_path."""
    global current_dump_timestamp

    os.makedirs(DUMP_DIR, exist_ok=True)
    
    dump_filename = os.path.join(DUMP_DIR, f"DUMP_{current_dump_timestamp}.csv")
    
    print(f"\n[Dump] Gerando novo dump CSV: {dump_filename}")
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute("SELECT timestamp, sha256, url, file_path, is_directory FROM urls ORDER BY url")
    rows = cursor.fetchall()
    
    with open(dump_filename, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        
        csv_writer.writerow(['timestamp_ciclo', 'hash_sha256', 'url', 'file_path', 'is_directory'])
        csv_writer.writerows(rows)
        
    conn.close()
    print(f"[Dump] Concluído. {len(rows)} URLs salvas.")


# --- Lógica Principal (Loop Contínuo) ---

def main_loop():
    global current_dump_timestamp
    setup_db()
    
    # 1. Inicializa o tempo do ciclo
    last_cycle_time = get_last_cycle_time()
    current_time = int(time.time())

    # Inicializa ou continua o ciclo de tempo (o timestamp usado para nomear arquivos/inserções)
    if (current_time - last_cycle_time) >= SECONDS_TO_WAIT or last_cycle_time == 0:
        current_dump_timestamp = current_time
        save_last_cycle_time(current_time)
    else:
        current_dump_timestamp = last_cycle_time
    
    print(f"*** INICIANDO SCRAPING CONTÍNUO ***")
    print(f"Timestamp do ciclo inicial: {datetime.fromtimestamp(current_dump_timestamp)}")
    
    while True:
        current_time = int(time.time())
        
        # 2. Checagem do Tempo de Troca de Arquivo (1h)
        if (current_time - current_dump_timestamp) >= SECONDS_TO_WAIT:
            
            # --- MUDANÇA DE CICLO ---
            print("\n=======================================================")
            print(f"*** {HOURS_TO_WAIT} HORA SE PASSOU! GERANDO DUMP E INICIANDO NOVO CICLO. ***")
            
            dump_all_urls_to_csv()
            
            # Inicia o novo ciclo
            current_dump_timestamp = current_time 
            save_last_cycle_time(current_time)
            
            print(f"Novo Timestamp do Ciclo: {datetime.fromtimestamp(current_dump_timestamp)}")
            print("=======================================================\n")

        # 3. Execução do Scraping
        print("\n[Ciclo de Exploração] Iniciando nova passagem (continuando/verificando pendências)...")
        # A recursão usa o DB para pular diretórios já visitados e continua a busca.
        recursive_scrape(URL_BASE)

        # Espera mais longa no loop principal, pois o trabalho pesado (HTTP requests)
        # já tem um delay de 5s embutido.
        print("\n[Ciclo de Exploração] Passagem concluída. Aguardando 60 segundos antes de verificar o tempo novamente.")
        time.sleep(5) # Aumentado para 60s para evitar busy-waiting
                        
if __name__ == "__main__":
    main_loop()
