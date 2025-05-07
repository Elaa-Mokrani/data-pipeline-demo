from kafka import KafkaProducer
import psycopg2
import json
import time

def get_db_connection():
    return psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )

def fetch_factures():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT numero, client, date, montant FROM facture")
    factures = cur.fetchall()
    cur.close()
    conn.close()
    return factures

def create_producer():
    return KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def send_factures_to_kafka():
    producer = create_producer()
    factures = fetch_factures()
    
    for facture in factures:
        facture_dict = {
            'numero': facture[0],
            'client': facture[1],
            'date': str(facture[2]),
            'montant': float(facture[3])
        }
        producer.send('factures', value=facture_dict)
        print(f"Sent: {facture_dict}")
        time.sleep(1)

if __name__ == "__main__":
    send_factures_to_kafka()