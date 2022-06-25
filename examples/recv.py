#!/usr/bin/env python3

import os
import pika
import sys

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pika.spec


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    channel.queue_declare(queue="hello")

    def hello_callback(
        ch: pika.BlockingConnection,
        method: pika.spec.Basic.Deliver,
        properties: pika.BasicProperties,
        body: bytes,
    ):
        print(" [x] Received %r" % body)

    channel.basic_consume(
        queue="hello", on_message_callback=hello_callback, auto_ack=True
    )

    print(" [*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
