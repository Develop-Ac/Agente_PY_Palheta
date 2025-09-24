# consumer/consumer.py
import pika
import psycopg2
import json
import os
import time

# --- CONFIGURAÇÕES (lidas das variáveis de ambiente) ---
PG_CONFIG = {'host': os.getenv('PG_HOST'), 'dbname': os.getenv('PG_DBNAME'), 'user': os.getenv('PG_USER'), 'password': os.getenv('PG_PASS')}
RABBIT_HOST = os.getenv('RABBIT_HOST')
QUEUE_NAME = os.getenv('QUEUE_NAME') # Ex: 'fila_paletas'
PROCEDURE_NAME = os.getenv('PROCEDURE_NAME') # Ex: 'processar_paletas'

def main():
    if not all([RABBIT_HOST, QUEUE_NAME, PROCEDURE_NAME]):
        print("Erro: Variáveis de ambiente RABBIT_HOST, QUEUE_NAME, e PROCEDURE_NAME devem ser definidas.")
        return

    while True:
        try:
            # Conecta ao RabbitMQ e ao PostgreSQL
            rabbit_conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST, heartbeat=600, blocked_connection_timeout=300))
            channel = rabbit_conn.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            pg_conn = psycopg2.connect(**PG_CONFIG)

            print(f' [*] Ouvindo a fila "{QUEUE_NAME}". Para sair, pressione CTRL+C')

            def callback(ch, method, properties, body):
                try:
                    with pg_conn.cursor() as pg_cursor:
                        # Chama a procedure do PostgreSQL passando o JSON
                        pg_cursor.execute(f"CALL {PROCEDURE_NAME}(%s::jsonb)", (body.decode(),))
                        pg_conn.commit()
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    print(f"Erro ao processar mensagem: {e}")
                    pg_conn.rollback()

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
            channel.start_consuming()
        except Exception as e:
            print(f"Erro na conexão ou no loop principal: {e}. Tentando reconectar em 10 segundos...")
            time.sleep(10)

if __name__ == '__main__':
    main()