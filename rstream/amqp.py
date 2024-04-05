from __future__ import annotations

from typing import Any, Optional, Protocol, cast

from amqp10_codec_faster import message_faster
import uamqp


class _MessageProtocol(Protocol):
    publishing_id: Optional[int] = None

    def __bytes__(self) -> bytes:
        ...


class AMQPMessage(uamqp.Message, _MessageProtocol):
    def __init__(self, *args: Any, publishing_id: Optional[int] = None, **kwargs: Any):
        self.publishing_id = publishing_id
        super().__init__(*args, **kwargs)

    def __bytes__(self) -> bytes:
        return cast(bytes, self.encode_message())


def amqp_decoder(data: bytes) -> AMQPMessage:
    message = AMQPMessage.decode_from_bytes(data)
    return message


class AMQPMessageCython(message_faster.Message, _MessageProtocol):
    def __init__(self, *args: Any, publishing_id: Optional[int] = None, **kwargs: Any):
        self.publishing_id = publishing_id
        super().__init__(*args, **kwargs)

    def __bytes__(self) -> bytes:
        return cast(bytes, self.marshal_c())


def amqp_decoder_cython(data: bytes) -> AMQPMessageCython:
    return message_faster.Message().unmarshal_c(data)
