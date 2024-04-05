import asyncio
import signal
import time

from rstream import (
    AMQPMessage,
    Consumer,
    MessageContext,
    amqp_decoder,
)

STREAM = "my-test-stream"


async def consume():
    consumer = Consumer(
        host="localhost",
        port=5552,
        vhost="/",
        username="guest",
        password="guest",
    )

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(consumer.close()))

    async def on_message(msg: AMQPMessage, message_context: MessageContext):

        if message_context.offset % 1_000_000 == 0:
            end_time = time.perf_counter()
            print(f"Got message: {msg} {message_context.offset} in {end_time - start_time:0.4f} seconds")

    await consumer.start()
    start_time = time.perf_counter()
    await consumer.subscribe(stream=STREAM, callback=on_message, decoder=amqp_decoder)
    await consumer.run()


asyncio.run(consume())
