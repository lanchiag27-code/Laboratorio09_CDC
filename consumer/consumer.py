
import json
import os
from datetime import datetime, timezone
from uuid import uuid4

import boto3
from confluent_kafka import Consumer, KafkaException

# CONFIGURACION DE KAFKA
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "lab09.public.ventas")

# CONFIGURACION DE FLOCI S3
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://floci:4566")
S3_BUCKET = os.getenv("S3_BUCKET", "ucv-data-lake")

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

# CONFIGURACION DEL CONSUMIDOR
consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "lab09-bronze-consumer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

consumer.subscribe([KAFKA_TOPIC])

print("Consumer iniciado", flush=True)
print(f"Escuchando tópico: {KAFKA_TOPIC}", flush=True)
print(f"Bucket destino: {S3_BUCKET}", flush=True)

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            raise KafkaException(msg.error())

        if msg.value() is None:
            consumer.commit(message=msg, asynchronous=False)
            continue

        # LEER EVENTO CDC
        evento = json.loads(msg.value().decode("utf-8"))

        # CREAR RUTA BRONZE
        fecha = datetime.now(timezone.utc)
        ruta = fecha.strftime(
            "bronze/sales/year=%Y/month=%m/day=%d"
        )

        archivo = (
            f"{ruta}/"
            f"evento_p{msg.partition()}_"
            f"o{msg.offset()}_{uuid4().hex}.jsonl"
        )

        # CONVERTIR EVENTO A JSONL
        contenido = json.dumps(
            evento,
            ensure_ascii=False
        ) + "\n"

        # GUARDAR EN FLOCI S3
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=archivo,
            Body=contenido.encode("utf-8"),
            ContentType="application/x-ndjson",
        )

        # CONFIRMAR OFFSET DESPUES DE GUARDAR
        consumer.commit(
            message=msg,
            asynchronous=False
        )

        operacion = evento.get("payload", {}).get("op")

        # MOSTRAR DATOS PARA LA EVIDENCIA 8
        print(
            f"Evento guardado | "
            f"topic={msg.topic()} | "
            f"partition={msg.partition()} | "
            f"offset={msg.offset()} | "
            f"op={operacion} | "
            f"s3://{S3_BUCKET}/{archivo}",
            flush=True
        )

except KeyboardInterrupt:
    print("Consumer detenido", flush=True)

finally:
    consumer.close()
