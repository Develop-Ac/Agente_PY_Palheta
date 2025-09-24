# producer/producer.py
import fdb
import pika
import json
import os
import sys
import psycopg2

# --- CONFIGURAÇÕES (serão lidas das variáveis de ambiente no EasyPanel) ---
PG_CONFIG = {'host': os.getenv('PG_HOST'), 'dbname': os.getenv('PG_DBNAME'), 'user': os.getenv('PG_USER'), 'password': os.getenv('PG_PASS')}
FB_CONFIG = {'host': os.getenv('FB_HOST'), 'database': os.getenv('FB_DB_PATH'), 'user': os.getenv('FB_USER'), 'password': os.getenv('FB_PASS'), 'charset': 'UTF8'}
RABBIT_HOST = os.getenv('RABBIT_HOST')

def main():
    if len(sys.argv) < 2:
        print("Erro: Forneça o nome do job a ser executado.")
        sys.exit(1)
    job_name_to_run = sys.argv[1]

    try:
        # 1. Busca a configuração do Job no PostgreSQL
        pg_conn = psycopg2.connect(**PG_CONFIG)
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute("SELECT source_query, rabbitmq_queue FROM etl_jobs WHERE job_name = %s", (job_name_to_run,))
        job_config = pg_cursor.fetchone()
        if not job_config: raise Exception(f"Job '{job_name_to_run}' não encontrado.")
        source_query, queue_name = job_config
        pg_conn.close()

        # 2. Conecta no Firebird e no RabbitMQ
        fb_conn = fdb.connect(**FB_CONFIG)
        fb_cursor = fb_conn.cursor()
        rabbit_conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
        channel = rabbit_conn.channel()
        channel.queue_declare(queue=queue_name, durable=True)

        # 3. Executa a query e publica as mensagens
        fb_cursor.execute(source_query)
        column_names = [desc[0].lower() for desc in fb_cursor.description]
        count = 0
        for row in fb_cursor:
            message_body = dict(zip(column_names, row))
            channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(message_body, default=str), properties=pika.BasicProperties(delivery_mode=2))
            count += 1
        
        print(f"Job '{job_name_to_run}' concluído. {count} mensagens publicadas na fila '{queue_name}'.")

    except Exception as e:
        print(f"Erro ao executar o job '{job_name_to_run}': {e}")
    finally:
        # Fecha todas as conexões
        if 'fb_conn' in locals() and fb_conn: fb_conn.close()
        if 'rabbit_conn' in locals() and rabbit_conn.is_open: rabbit_conn.close()

if __name__ == '__main__':
    main()