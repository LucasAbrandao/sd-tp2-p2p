"""Enquadramento de mensagens JSON sobre o fluxo TCP.

TCP pode entregar um cabeçalho ou corpo em várias leituras. O prefixo de quatro
bytes informa exatamente quantos bytes devem ser consumidos para cada mensagem.
"""

import asyncio
import json
import struct

MAX_FRAME = 2 * 1024 * 1024
VERSION = 1


class ProtocolError(ValueError):
    """Mensagem inválida, incompatível ou maior que o limite do protocolo."""


async def read_message(reader: asyncio.StreamReader, timeout: float = 5) -> dict:
    """Lê um quadro completo; o prazo inclui cabeçalho e corpo."""
    async def receive():
        header = await reader.readexactly(4)
        length = struct.unpack("!I", header)[0]
        if not 0 < length <= MAX_FRAME:
            raise ProtocolError("Tamanho do quadro fora do limite")
        try:
            message = json.loads((await reader.readexactly(length)).decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError, RecursionError) as error:
            raise ProtocolError("JSON inválido") from error
        if not isinstance(message, dict):
            raise ProtocolError("A mensagem deve ser um objeto JSON")
        return message
    return await asyncio.wait_for(receive(), timeout)


async def write_message(writer: asyncio.StreamWriter, message: dict,
                        timeout: float = 5) -> None:
    """Envia o quadro e respeita a contrapressão do socket."""
    body = json.dumps(message, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    if not 0 < len(body) <= MAX_FRAME:
        raise ProtocolError("Mensagem excede o limite do protocolo")
    writer.write(struct.pack("!I", len(body)) + body)
    await asyncio.wait_for(writer.drain(), timeout)
