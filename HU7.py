#!/usr/bin/env python3
import json
import time
import boto3
from botocore.config import Config
from datetime import datetime
import subprocess
import re

# -----------------------------------------------
# Configuración de Kafka y ruta al binario
# -----------------------------------------------
KAFKA_NODES = [
    "b-1-public.testkafka3.l7t9mi.c2.kafka.eu-south-2.amazonaws.com:9198",
    "b-2-public.testkafka3.l7t9mi.c2.kafka.eu-south-2.amazonaws.com:9198"
]
KAFKA_SERVERS_STR = ",".join(KAFKA_NODES)
KAFKA_TOPIC = "imat3a_ADA"
PRODUCER_KEY = "A"  # La key debe ser A o B según el grupo de la clase
KAFKA_DIR = "/home/ec2-user/kafka_2.13-3.6.0"

# -----------------------------------------------
# Función para arreglar manualmente el JSON mal formado
# -----------------------------------------------
def fix_malformed_json(text):
    """
    Convierte un string con formato:
      {price: 76812.03, timestamp: 2025-04-08 17:37:53}
    a un JSON válido:
      {"price": 76812.03, "timestamp": "2025-04-08 17:37:53"}
    """
    text = text.strip()
    if text.startswith("{") and text.endswith("}"):
        inner = text[1:-1]  # elimina la primera { y la última }
    else:
        inner = text
    parts = inner.split(",")
    fixed_parts = []
    for part in parts:
        kv = part.split(":", 1)
        if len(kv) < 2:
            continue
        key = kv[0].strip()
        value = kv[1].strip()
        # Asegurar que la clave esté entre comillas
        if not (key.startswith('"') and key.endswith('"')):
            key = f'"{key}"'
        # Si el valor parece una fecha (contiene '-' y ':') y no tiene comillas, agrégalas
        if not (value.startswith('"') and value.endswith('"')):
            if '-' in value and ':' in value:
                value = f'"{value}"'
        fixed_parts.append(f"{key}: {value}")
    fixed_json = "{" + ", ".join(fixed_parts) + "}"
    return fixed_json

# -----------------------------------------------
# Función que invoca kafka-console-consumer.sh
# -----------------------------------------------
def consume_from_kafka():
    kafka_cmd = (
        f"{KAFKA_DIR}/bin/kafka-console-consumer.sh "
        f"--bootstrap-server {KAFKA_SERVERS_STR} "
        f"--consumer.config {KAFKA_DIR}/config/client.properties "
        f"--topic {KAFKA_TOPIC} "
        f"--property parse.key=true "
        f"--property key.separator=: "
        f"--from-beginning"
    )
    proc = subprocess.Popen(
        kafka_cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )
    return proc

# -----------------------------------------------
# Clase para escribir en Amazon Timestream
# -----------------------------------------------
class TimestreamWriter:
    def __init__(self):
        """
        Inicializa el cliente de Timestream (región 'eu-west-1',
        ya que Timestream no está en eu-south-2).
        """
        self.config = Config(retries={'max_attempts': 10})
        self.client = boto3.client(
            'timestream-write',
            region_name='eu-west-1',
            config=self.config
        )
        
    def write_one_record(self, crypto_symbol, price, ts_millis):
        """
        Inserta un registro en la tabla 'CryptoMonedas' de la base 'CryptoIcaiDatabase'.
          - crypto_symbol: por ejemplo, "ADA", "BTC", "DOT".
          - price: valor numérico a guardar.
          - ts_millis: timestamp en milisegundos (string).
        """
        dimensions = [{'Name': 'ICAI', 'Value': 'Crypto'}]
        record = {
            'MeasureName': crypto_symbol,
            'Dimensions': dimensions,
            'MeasureValue': str(price),
            'Time': ts_millis
        }
        try:
            result = self.client.write_records(
                DatabaseName='CryptoIcaiDatabase',
                TableName='CryptoMonedas',
                Records=[record],
                CommonAttributes={}
            )
            print(f"WriteRecords Status: [{result['ResponseMetadata']['HTTPStatusCode']}]")
        except self.client.exceptions.RejectedRecordsException as err:
            print("Error: RejectedRecordsException =>", err)
        except Exception as err:
            print("Error insertando en Timestream:", err)

# -----------------------------------------------
# Función principal del consumer
# -----------------------------------------------
def main():
    writer = TimestreamWriter()
    print(f"[Consumer] Escuchando mensajes en el topic '{KAFKA_TOPIC}'...")
    
    process = consume_from_kafka()
    crypto_symbol = "ADA"

    while True:
        line = process.stdout.readline()
        if not line:
            break
        line = line.strip()
        if not line:
            continue

        try:
            # En lugar de separar por ":", extraemos el bloque JSON
            import re
            match = re.search(r'(\{.*\})', line)
            if match:
                json_candidate = match.group(1)
            else:
                raise ValueError("No se encontró objeto JSON en la línea.")
            # Arreglar el objeto JSON mal formado
            fixed_payload = fix_malformed_json(json_candidate)
            data = json.loads(fixed_payload)

            price = data.get("price")
            if "timestamp" in data:
                dt = datetime.strptime(data["timestamp"], "%Y-%m-%d %H:%M:%S")
                ts_millis = str(int(dt.timestamp() * 1000))
            else:
                ts_millis = str(int(round(time.time() * 1000)))

            writer.write_one_record(crypto_symbol, price, ts_millis)
            print(f"[Consumer] Recibido: {data} y guardado en Timestream.")
        except Exception as e:
            print("[Consumer] Error procesando mensaje:", e)
            print("Línea problemática:", line)

    stderr_output = process.stderr.read()
    if stderr_output:
        print("[Consumer] Errores del consumer:", stderr_output)

if __name__ == "__main__":
    main()
