import json
import random
import re
import string
import time
import subprocess
from datetime import datetime
from websocket import create_connection, WebSocketConnectionClosedException

# ---------------------------------------------
# CONFIGURACIÓN DE KAFKA
# ---------------------------------------------
KAFKA_NODES = [
    "b-1-public.testkafka3.l7t9mi.c2.kafka.eu-south-2.amazonaws.com:9198",
    "b-2-public.testkafka3.l7t9mi.c2.kafka.eu-south-2.amazonaws.com:9198"
]
KAFKA_SERVERS_STR = ",".join(KAFKA_NODES)

KAFKA_TOPIC = "imat3a_ADA"
PRODUCER_KEY = "A"  # La key debe ser A o B según el grupo de la clase

# Ruta al binario de Kafka
KAFKA_DIR = "/home/ec2-user/kafka_2.13-3.6.0"


def random_session_id():
    """Genera un ID de sesión aleatoria para TradingView."""
    return "qs_" + "".join(random.choice(string.ascii_lowercase) for _ in range(12))


def add_tv_header(payload):
    """Añade el encabezado ~m~...~m~ que requiere TradingView."""
    return f"~m~{len(payload)}~m~{payload}"


def build_tv_json(tv_func, param_array):
    """Construye el objeto JSON { 'm': tv_func, 'p': param_array }."""
    return json.dumps({"m": tv_func, "p": param_array}, separators=(",", ":"))


def complete_tv_payload(tv_func, param_array):
    """
    Combina build_tv_json con add_tv_header
    para formar el mensaje final que se manda al WebSocket.
    """
    return add_tv_header(build_tv_json(tv_func, param_array))


def transmit_ws_msg(sock, tv_func, param_array):
    """
    Envía un mensaje (JSON + cabecera) al WebSocket.
    Reintenta reconexión si está cerrado.
    """
    try:
        sock.send(complete_tv_payload(tv_func, param_array))
    except WebSocketConnectionClosedException:
        print("Conexión cerrada mientras se intentaba enviar un mensaje.")
        ws_reconnect(sock)


def keepalive_ping(sock):
    """Envía el ping ~h~0 a TradingView para mantener la conexión."""
    try:
        sock.send("~h~0")
    except Exception as error:
        print(f"Error enviando ping: {error}")


def produce_kafka(precio, marca_tiempo):
    """
    Envía el precio y la marca de tiempo a Kafka utilizando
    kafka-console-producer.sh vía subprocess.
    """
    mensaje_kafka = json.dumps({"price": precio, "timestamp": marca_tiempo})
    kafka_cmd = f'''echo "{PRODUCER_KEY}:{mensaje_kafka}" | {KAFKA_DIR}/bin/kafka-console-producer.sh \
        --bootstrap-server {KAFKA_SERVERS_STR} \
        --producer.config {KAFKA_DIR}/config/client.properties \
        --topic {KAFKA_TOPIC} \
        --property parse.key=true \
        --property key.separator=:'''

    resultado = subprocess.run(kafka_cmd, shell=True, capture_output=True, text=True)
    if resultado.returncode == 0:
        print(f"[Kafka] Mensaje enviado: {mensaje_kafka}")
    else:
        print(f"[Kafka] Error enviando mensaje: {resultado.stderr}")


def parse_tv_data(tv_data):
    """
    Extrae el 'lp' (último precio) de la respuesta de TradingView
    y lo reenvía a Kafka con marca de tiempo.
    """
    price = tv_data.get("lp", "No disponible")
    moment = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{moment}] Precio: {price}")
    produce_kafka(price, moment)


def ws_reconnect(asset_name):
    """Espera 5 segundos y vuelve a abrir la conexión WebSocket."""
    print("Intentando reconectar...")
    time.sleep(5)
    init_socket(asset_name)


def init_socket(asset_name):
    """
    Abre la conexión a TradingView y establece un bucle infinito
    para recibir los datos de 'lp'. Maneja pings y reconexiones.
    """
    tv_session = random_session_id()
    tv_url = "wss://data.tradingview.com/socket.io/websocket"
    tv_headers = json.dumps({"Origin": "https://data.tradingview.com"})

    try:
        sock = create_connection(tv_url, headers=tv_headers)
        print(f"Conectado a {tv_url}")

        transmit_ws_msg(sock, "quote_create_session", [tv_session])
        transmit_ws_msg(sock, "quote_set_fields", [tv_session, "lp"])
        transmit_ws_msg(sock, "quote_add_symbols", [tv_session, asset_name])

        while True:
            try:
                raw_msg = sock.recv()

                if raw_msg.startswith("~m~"):
                    raw_data_match = re.search(r"\{.*\}", raw_msg)
                    if raw_data_match:
                        json_msg = json.loads(raw_data_match.group(0))
                        if json_msg["m"] == "qsd":
                            parse_tv_data(json_msg["p"][1]["v"])

                elif raw_msg.startswith("~h~"):
                    keepalive_ping(sock)

            except WebSocketConnectionClosedException:
                print("Conexión cerrada inesperadamente.")
                ws_reconnect(asset_name)
                break
            except Exception as error:
                print(f"Error procesando mensaje: {error}")
                continue

    except WebSocketConnectionClosedException as error:
        print(f"Error al conectar: {error}. Reconectando en 5 segundos...")
        ws_reconnect(asset_name)
    except Exception as error:
        print(f"Error inesperado: {error}. Reconectando en 5 segundos...")
        ws_reconnect(asset_name)


if __name__ == "__main__":
    # Se conecta por defecto a BINANCE:BTCUSD.
    crypto_symbol = "BINANCE:ADAUSD"
    init_socket(crypto_symbol)
