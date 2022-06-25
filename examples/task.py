#!/usr/bin/env python3

import pika
import sys

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pika.spec

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()

channel.queue_declare(queue="task_queue", durable=True)

message = " ".join(sys.argv[1:]) or "Hello World!"
channel.basic_publish(
    exchange="",
    routing_key="task_queue",
    body=message,
    properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE),
)

print(" [x] Sent %r" % message)
connection.close()
