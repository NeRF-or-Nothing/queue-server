#!/usr/bin/env python3

import asyncio
import uuid
import aio_pika
import pickle
from typing import List, Union
from typing_extensions import Self

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

        self.url = f"amqp://guest:guest@{self.host}:{self.port}/"

    async def connect(self):
        self.connection = await aio_pika.connect(host=self.host, port=self.port)

        # async with self.connection:
        #     self.channel = await self.connection.channel()

        #     await self.declare()

    async def declare(self):
        self.colmap_queue: aio_pika.abc.AbstractQueue = (
            await self.channel.declare_queue(name=COLMAP_QUEUE, durable=True)
        )
        print(f"  [*] Declared {COLMAP_QUEUE}.")
        self.tensorf_queue: aio_pika.abc.AbstractQueue = (
            await self.channel.declare_queue(name=TENSORF_QUEUE, durable=True)
        )
        print(f"  [*] Declared {TENSORF_QUEUE}.")

    async def test_queue_server(self):
        """
        Test the QueueServer class
        """
        await self.connect()

        async with self.connection:
            self.channel = await self.connection.channel()

            await self.declare()

            for i in range(10):
                uid = str(uuid.uuid4())
                if i % 2:
                    print(f"Sending to colmap: {uid}")
                    await self.send_to_colmap(uid)
                else:
                    print(f"Sending to tensorf: {uid}")
                    await self.send_to_tensorf(uid)

    async def send_to_colmap(self, body: Union[str, bytes]):
        """
        Send a message to the colmap queue, as of now should be a string or bytes, but we can modify this to be a serialized dataclass later (see: Serialize and Deserialize)
        """
        message = aio_pika.Message(
            bytes(body, "utf-8"),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await self.channel.default_exchange.publish(
            message=message, routing_key=self.colmap_queue.name
        )

    async def send_to_tensorf(self, body: Union[str, bytes]):
        """
        Send a message to the tensorf queue, as of now should be a string or bytes, but we can modify this to be a serialized dataclass later (see: Serialize and Deserialize)
        """
        message = aio_pika.Message(
            bytes(body, "utf-8"),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await self.channel.default_exchange.publish(
            message=message, routing_key=self.tensorf_queue.name
        )

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

    async def close(self):
        """
        Close this object's connection to the RabbitMQ server
        """
        await self.connection.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    qs = QueueServer()
    loop.run_until_complete(qs.test_queue_server())
