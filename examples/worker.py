#!/usr/bin/env python3

import pika
import time

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pika.spec

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()

channel.queue_declare(queue="task_queue", durable=True)
print(" [*] Waiting for messages. To exit press CTRL+C")


def callback(
    ch: pika.BlockingConnection,
    method: pika.spec.Basic.Deliver,
    properties: pika.BasicProperties,
    body: bytes,
):
    print(" [x] Received %r" % body)
    time.sleep(body.count(b"."))
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue="task_queue", on_message_callback=callback)

channel.start_consuming()
