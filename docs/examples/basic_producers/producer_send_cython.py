import asyncio
import time

from rstream import Producer
from rstream.amqp import AMQPMessageCython

STREAM = "my-test-stream"
MESSAGES = 1_000_000


async def publish():

    async with Producer("localhost", username="guest", password="guest") as producer:
        # create a stream if it doesn't already exist
        await producer.delete_stream(STREAM, missing_ok=True)
        await producer.create_stream(STREAM, exists_ok=True)

        # sending a million of messages in AMQP format
        start_time = time.perf_counter()

        for i in range(MESSAGES):
            amqp_message = AMQPMessageCython(
                body=b"hello",
            )
            # send is asynchronous
            await producer.send(stream=STREAM, message=amqp_message)

        end_time = time.perf_counter()
        print(f"Sent {MESSAGES} messages from {amqp_message.__class__.__name__} in {end_time - start_time:0.4f} seconds")


asyncio.run(publish())
