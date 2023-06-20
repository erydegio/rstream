# Copyright 2023 VMware, Inc. All Rights Reserved.
# SPDX-License-Identifier: MIT

import asyncio

from rstream import (
    AMQPMessage,
    ConfirmationStatus,
    MessageContext,
)

captured: list[bytes] = []


async def wait_for(condition, timeout=1):
    async def _wait():
        while not condition():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(_wait(), timeout)


def on_publish_confirm_client_callback(
    confirmation: ConfirmationStatus, confirmed_messages: list[int], errored_messages: list[int]
) -> None:

    if confirmation.is_confirmed is True:
        confirmed_messages.append(confirmation.message_id)
    else:
        errored_messages.append(confirmation.message_id)


def on_publish_confirm_client_callback2(
    confirmation: ConfirmationStatus, confirmed_messages: list[int], errored_messages: list[int]
) -> None:

    if confirmation.is_confirmed is True:
        confirmed_messages.append(confirmation.message_id)
    else:
        errored_messages.append(confirmation.message_id)


async def routing_extractor(message: AMQPMessage) -> str:
    return "0"


async def routing_extractor_key(message: AMQPMessage) -> str:
    return "key1"


async def on_message(
    msg: AMQPMessage, message_context: MessageContext, streams: list[str], offsets: list[int]
):

    stream = await message_context.consumer.stream(message_context.subscriber_name)
    streams.append(stream)
    offset = message_context.offset
    offsets.append(offset)
