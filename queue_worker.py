#!/usr/bin/env python3

import pickle
import random
import time
import uuid
import pika

from typing import TYPE_CHECKING, List
from typing_extensions import Self

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel
    import pika.spec


COLMAP_QUEUE = "colmap_queue"
TENSORF_QUEUE = "tensorf_queue"


class QueueWorker:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5672,
        queues: List[str] = [COLMAP_QUEUE, TENSORF_QUEUE],
    ) -> Self:
        """
        Unless you really need to modify something, call this method with the default arguments, i.e. QueueWorker().
        """
        if not queues:
            raise ValueError("At least one queue is required")

        self.host = host
        self.port = port
        self.queues = queues

        self.connection: pika.BlockingConnection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host)
        )
        self.channel: BlockingChannel = self.connection.channel()

        self.declare()

    def start(self):
        """
        Start consuming messages from the queues
        """
        self.init_consume()
        self.channel.start_consuming()

    def declare(self):
        """
        Declare queues, can add more later if needed
        """
        for queue in self.queues:
            self.channel.queue_declare(queue=queue, durable=True)
            print(f"  [*] Declared {queue}.")

    def init_consume(self):
        """
        Allow consumin messages from all queues
        """
        self.channel.basic_consume(
            queue=COLMAP_QUEUE, on_message_callback=self.colmap_callback
        )
        self.channel.basic_consume(
            queue=TENSORF_QUEUE, on_message_callback=self.tensorf_callback
        )

    def colmap_callback(
        self,
        ch: pika.BlockingConnection,
        method: pika.spec.Basic.Deliver,
        properties: pika.BasicProperties,
        body: bytes,  # demoing as uuid for now
    ):
        n = random.randint(0, 5)
        print(f" [x] Sleeping for {n} seconds as colmap process {body}")
        time.sleep(n)
        print(f" [x] Done with colmap process {body}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def tensorf_callback(
        self,
        ch: pika.BlockingConnection,
        method: pika.spec.Basic.Deliver,
        properties: pika.BasicProperties,
        body: bytes,  # demoing as uuid for now
    ):
        n = random.randint(0, 5)
        print(f" [x] Sleeping for {n} seconds as colmap process {body}")
        time.sleep(n)
        print(f" [x] Done with colmap process {body}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    @staticmethod
    def Serialize(data) -> bytes:
        """
        Serializes calling object to bytes

        Example::

            qs = QueueWorker()
            data = qs.Serialize("Hello World")

            QueueWorker.Serialize("Hello World")
        """
        return pickle.dumps(data)

    @staticmethod
    def Deserialize(data: bytes) -> Self:
        """
        Deserializes data to a QueueWorker object, qs2 is the same as qs but they are not the same object under the hood (i.e. their pointers are different, cool right?)

        Example::

            qw = QueueWorker()
            data = qw.Serialize()
            qw2 = qw.Deserialize(data)

            QueueWorker.Deserialize(data)
        """
        return pickle.loads(data)

    def close(self):
        """
        Close this object's connection to the RabbitMQ server
        """
        self.connection.close()


if __name__ == "__main__":
    qw = QueueWorker()
    qw.start()
