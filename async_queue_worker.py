#!/usr/bin/env python3

import asyncio
import os
import random
import aio_pika
import pickle
from typing import List
from typing_extensions import Self

COLMAP_QUEUE = "colmap_queue"
TENSORF_QUEUE = "tensorf_queue"

TOTAL_SECONDS = 0
TOTAL_PROCESSED_SECONDS = 0
TOTAL_JOBS_PROCESSED = 0


class QueueWorker:
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
        #     await self.channel.set_qos(prefetch_count=1)

        #     await self.declare()

    async def test_queue_worker(self):
        """
        Test the QueueWorker class
        """
        await self.connect()

        async with self.connection:
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=1)

            self.colmap_queue = await self.channel.declare_queue(
                name=COLMAP_QUEUE, durable=True
            )
            print(f"  [*] Declared {COLMAP_QUEUE}.")
            self.tensorf_queue = await self.channel.declare_queue(
                name=TENSORF_QUEUE, durable=True
            )
            print(f"  [*] Declared {TENSORF_QUEUE}.")

            await self.colmap_queue.consume(self.colmap_callback)
            await self.tensorf_queue.consume(self.tensorf_callback)

            print(" [*] Waiting for messages. To exit press CTRL+C")
            await asyncio.Future()

    async def declare(self):
        self.colmap_queue: aio_pika.abc.AbstractQueue = (
            await self.channel.declare_queue(name=COLMAP_QUEUE, durable=True)
        )
        print(f"  [*] Declared {COLMAP_QUEUE}.")
        self.tensorf_queue: aio_pika.abc.AbstractQueue = (
            await self.channel.declare_queue(name=TENSORF_QUEUE, durable=True)
        )
        print(f"  [*] Declared {TENSORF_QUEUE}.")

    async def colmap_callback(
        self, message: aio_pika.abc.AbstractIncomingMessage
    ):  # demoing as uuid for now
        global TOTAL_SECONDS, TOTAL_PROCESSED_SECONDS, TOTAL_JOBS_PROCESSED
        # n = random.randint(1, 2)
        n = 1
        TOTAL_SECONDS += n
        print(
            f" [x] Sleeping for {n} seconds as colmap process {message.body}\t{TOTAL_PROCESSED_SECONDS}/{TOTAL_SECONDS}"
        )
        await asyncio.sleep(n)
        TOTAL_PROCESSED_SECONDS += n
        TOTAL_JOBS_PROCESSED += 1
        print(f" [x] Done with colmap process {message.body}")
        await message.ack()
        if TOTAL_JOBS_PROCESSED == 10:
            print(f" [x] Done with all jobs")
            os._exit(0)

    async def tensorf_callback(
        self, message: aio_pika.abc.AbstractIncomingMessage
    ):  # demoing as uuid for now
        global TOTAL_SECONDS, TOTAL_PROCESSED_SECONDS, TOTAL_JOBS_PROCESSED
        # n = random.randint(1, 2)
        n = 1
        TOTAL_SECONDS += n
        print(
            f" [x] Sleeping for {n} seconds as tensorf process {message.body}\t{TOTAL_PROCESSED_SECONDS}/{TOTAL_SECONDS}"
        )
        await asyncio.sleep(n)
        TOTAL_PROCESSED_SECONDS += n
        TOTAL_JOBS_PROCESSED += 1
        print(f" [x] Done with tensorf process {message.body}")
        await message.ack()
        if TOTAL_JOBS_PROCESSED == 10:
            print(f" [x] Done with all jobs")
            os._exit(0)

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
    qw = QueueWorker()
    loop.run_until_complete(qw.test_queue_worker())
