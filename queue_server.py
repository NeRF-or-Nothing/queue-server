#!/usr/bin/env python3

import pickle
import uuid
import pika
from typing import TYPE_CHECKING, List, Union
from typing_extensions import Self

if TYPE_CHECKING:
    import pika.spec
    from pika.adapters.blocking_connection import BlockingChannel

COLMAP_QUEUE = "colmap_queue"
TENSORF_QUEUE = "tensorf_queue"


class QueueServer:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5672,
        queues: List[str] = [COLMAP_QUEUE, TENSORF_QUEUE],
    ) -> Self:
        """
        Unless you really need to modify something, call this method with the default arguments, i.e. QueueServer().
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
        for i in range(10):
            uid = str(uuid.uuid4())
            if i % 2:
                print(f"Sending to colmap: {uid}")
                self.send_to_colmap(uid)
            else:
                print(f"Sending to tensorf: {uid}")
                self.send_to_tensorf(uid)

    def declare(self):
        """
        Declare queues, can add more later if needed
        """
        for queue in self.queues:
            self.channel.queue_declare(queue=queue, durable=True)
            print(f"  [*] Declared {queue}.")

    def send_to_colmap(self, message: Union[str, bytes]):
        """
        Send a message to the colmap queue, as of now should be a string or bytes, but we can modify this to be a serialized dataclass later (see: Serialize and Deserialize)
        """
        self.channel.basic_publish(
            exchange="",
            routing_key=COLMAP_QUEUE,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ),
        )

    def send_to_tensorf(self, message: Union[str, bytes]):
        """
        Send a message to the tensorf queue, as of now should be a string or bytes, but we can modify this to be a serialized dataclass later (see: Serialize and Deserialize)
        """
        self.channel.basic_publish(
            exchange="",
            routing_key=TENSORF_QUEUE,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ),
        )

    @staticmethod
    def Serialize(data) -> bytes:
        """
        Serializes calling object to bytes

        Example::

            qs = QueueServer()
            data = qs.Serialize("Hello World")

            QueueServer.Serialize("Hello World")
        """
        return pickle.dumps(data)

    @staticmethod
    def Deserialize(data: bytes) -> Self:
        """
        Deserializes data to a QueueServer object, qs2 is the same as qs but they are not the same object under the hood (i.e. their pointers are different, cool right?)

        Example::

            qs = QueueServer()
            data = qs.Serialize()
            qs2 = qs.Deserialize(data)

            QueueServer.Deserialize(data)
        """
        return pickle.loads(data)

    def close(self):
        """
        Close this object's connection to the RabbitMQ server
        """
        self.connection.close()

if __name__ == "__main__":
    qs = QueueServer()
    qs.start()
    qs.close()
    print("Done")