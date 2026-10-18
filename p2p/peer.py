"""Um único processo atende sockets e baixa blocos dos vizinhos em paralelo."""

import asyncio
import base64
import contextlib
import hashlib
import json
import math
import time
from dataclasses import dataclass
from pathlib import Path

from .metadata import Metadata
from .protocol import ProtocolError, VERSION, read_message, write_message
from .storage import BlockStore


@dataclass(frozen=True)
class Config:
    id: str
    host: str
    port: int
    neighbors: tuple[tuple[str, int], ...]
    metadata: Path
    directory: Path
    seed: Path | None = None
    timeout: float = 120
    request_timeout: float = 5
    poll_interval: float = 0.1
    serve_delay: float = 0

    @classmethod
    def load(cls, path: str | Path):
        """Caminhos relativos pertencem à pasta da configuração, não ao terminal."""
        path = Path(path).resolve()
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("Configuração deve ser um objeto JSON")
        def port(value):
            if type(value) is not int or not 1 <= value <= 65535:
                raise ValueError("Porta deve estar entre 1 e 65535")
            return value
        def host(value):
            if not isinstance(value, str) or not value.strip():
                raise ValueError("Host deve ser um endereço não vazio")
            return value
        def location(key):
            value = data.get(key)
            if not isinstance(value, str) or not value:
                raise ValueError(f"Caminho obrigatório: {key}")
            return (path.parent / value).resolve()
        identifier = data.get("id")
        if not isinstance(identifier, str) or not identifier or len(identifier) > 100:
            raise ValueError("id deve ter de 1 a 100 caracteres")
        neighbors = data.get("neighbors")
        if not isinstance(neighbors, list) or not neighbors or len(neighbors) > 64:
            raise ValueError("Configure entre 1 e 64 vizinhos")
        addresses = tuple(dict.fromkeys((host(n["host"]), port(n["port"])) for n in neighbors))
        options = {}
        for key, default in (("timeout", 120), ("request_timeout", 5),
                             ("poll_interval", 0.1), ("serve_delay", 0)):
            value = data.get(key, default)
            if type(value) not in (float, int) or not math.isfinite(value):
                raise ValueError(f"Valor numérico inválido: {key}")
            if value < 0 or (key != "serve_delay" and value == 0):
                raise ValueError(f"Prazo inválido: {key}")
            options[key] = float(value)
        return cls(identifier, host(data.get("host")), port(data.get("port")),
                   addresses, location("metadata"), location("directory"),
                   location("seed") if data.get("seed") is not None else None, **options)


class Peer:
    def __init__(self, config: Config):
        self.config = config
        self.metadata = Metadata.load(config.metadata)
        self.store = BlockStore(self.metadata, config.directory, seed=config.seed)
        self.in_flight: set[int] = set()
        self.completed = asyncio.Event()
        self.connections: set[asyncio.StreamWriter] = set()
        self.handlers: set[asyncio.Task] = set()
        self.started = time.monotonic()

    @property
    def have(self):
        return self.store.have_count

    def log(self, event, **fields):
        print(json.dumps({"time": time.time(), "peer": self.config.id,
                          "event": event, **fields}, ensure_ascii=True), flush=True)

    def hello(self):
        return {"type": "hello", "version": VERSION, "peer": self.config.id,
                "file_id": self.metadata.file_id}

    def check_hello(self, message):
        if (message.get("type") != "hello" or type(message.get("version")) is not int
                or message.get("version") != VERSION
                or message.get("file_id") != self.metadata.file_id
                or not isinstance(message.get("peer"), str)
                or not 0 < len(message["peer"]) <= 100):
            raise ProtocolError("Handshake incompatível com o arquivo ou protocolo")
        return message["peer"]

    async def handle(self, reader, writer):
        """O listener não depende do download: blocos parciais já podem ser servidos."""
        task = asyncio.current_task()
        self.handlers.add(task)
        self.connections.add(writer)
        target = "desconhecido"
        try:
            target = self.check_hello(await read_message(reader, self.config.request_timeout))
            await write_message(writer, self.hello(), self.config.request_timeout)
            while True:
                request = await read_message(reader, self.config.request_timeout)
                kind = request.get("type")
                if kind == "status":
                    response = {"type": "status", "blocks": sorted(self.store.available)}
                elif kind == "get":
                    index = request.get("index")
                    if type(index) is not int or not 0 <= index < self.metadata.block_count:
                        raise ProtocolError("Índice de bloco inválido")
                    try:
                        data = await asyncio.to_thread(self.store.get_block, index)
                    except (KeyError, ValueError, OSError):
                        response = {"type": "missing", "index": index}
                    else:
                        if self.config.serve_delay:
                            await asyncio.sleep(self.config.serve_delay)
                        response = {"type": "block", "index": index,
                                    "data": base64.b64encode(data).decode("ascii")}
                        await write_message(writer, response, self.config.request_timeout)
                        self.log("block_sent", index=index, target=target,
                                 have=self.have, total=self.metadata.block_count)
                        continue
                else:
                    raise ProtocolError("Tipo de solicitação desconhecido")
                await write_message(writer, response, self.config.request_timeout)
        except asyncio.IncompleteReadError:
            pass  # Encerramento normal de uma conexão TCP pelo outro lado.
        except (ValueError, OSError, asyncio.TimeoutError) as error:
            self.log("connection_error", target=target, reason=str(error))
        finally:
            self.connections.discard(writer)
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
            self.handlers.discard(task)

    def inventory(self, response):
        blocks = response.get("blocks")
        if response.get("type") != "status" or not isinstance(blocks, list):
            raise ProtocolError("Resposta de inventário inválida")
        if len(blocks) > self.metadata.block_count or any(
            type(i) is not int or not 0 <= i < self.metadata.block_count for i in blocks
        ):
            raise ProtocolError("Inventário contém índices inválidos")
        return set(blocks)

    async def download_from(self, address, worker_index):
        """Uma conexão persistente por vizinho; erros liberam reservas e reconectam."""
        host, port = address
        endpoint = f"{host}:{port}"
        cursor = (int(hashlib.sha256(self.config.id.encode()).hexdigest()[:8], 16)
                  + worker_index) % max(1, self.metadata.block_count)
        while not self.completed.is_set():
            writer = None
            reserved = None
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port), self.config.request_timeout)
                self.connections.add(writer)
                await write_message(writer, self.hello(), self.config.request_timeout)
                source = self.check_hello(await read_message(reader, self.config.request_timeout))
                self.log("connected", neighbor=endpoint, source=source)
                while not self.completed.is_set():
                    await write_message(writer, {"type": "status"}, self.config.request_timeout)
                    remote = self.inventory(await read_message(reader, self.config.request_timeout))
                    # Atualiza o inventário a cada pequeno lote. Assim também enxergamos
                    # blocos recém-baixados por um vizinho que ainda é leecher.
                    candidates = sorted(remote - self.store.available - self.in_flight,
                                        key=lambda i: (i - cursor) % self.metadata.block_count)
                    requested = 0
                    for index in candidates:
                        if requested >= 32:
                            break
                        if self.store.has_block(index) or index in self.in_flight:
                            continue
                        reserved = index
                        requested += 1
                        self.in_flight.add(reserved)
                        await write_message(writer, {"type": "get", "index": reserved},
                                            self.config.request_timeout)
                        response = await read_message(reader, self.config.request_timeout)
                        if response.get("index") != reserved:
                            raise ProtocolError("Resposta não corresponde ao bloco solicitado")
                        if response.get("type") == "missing":
                            remote.discard(reserved)
                        elif response.get("type") == "block":
                            encoded = response.get("data")
                            if not isinstance(encoded, str):
                                raise ProtocolError("Bloco sem dados base64")
                            data = base64.b64decode(encoded, validate=True)
                            if await asyncio.to_thread(self.store.put_block, reserved, data):
                                self.log("block_received", index=reserved, source=source,
                                         have=self.have, total=self.metadata.block_count)
                        else:
                            raise ProtocolError("Resposta de bloco inválida")
                        self.in_flight.discard(reserved)
                        cursor = (reserved + 1) % self.metadata.block_count
                        reserved = None
                        if self.store.complete:
                            return
                    await asyncio.sleep(self.config.poll_interval)
            except (ValueError, OSError, asyncio.TimeoutError, asyncio.IncompleteReadError) as error:
                self.log("neighbor_error", neighbor=endpoint, reason=str(error))
            finally:
                if reserved is not None:
                    self.in_flight.discard(reserved)
                if writer is not None:
                    self.connections.discard(writer)
                    writer.close()
                    with contextlib.suppress(OSError):
                        await writer.wait_closed()
            if not self.store.complete:
                self.log("retry", neighbor=endpoint)
                await asyncio.sleep(self.config.poll_interval)

    async def await_file(self):
        deadline = self.started + self.config.timeout
        while not self.store.complete:
            if time.monotonic() >= deadline:
                raise TimeoutError("Download não terminou no prazo configurado")
            await asyncio.sleep(min(self.config.poll_interval, 0.1))
        result = await asyncio.to_thread(self.store.assemble)
        self.completed.set()
        self.log("download_complete", path=str(result.resolve()), size=self.metadata.size,
                 sha256=self.metadata.sha256, elapsed_seconds=round(time.monotonic() - self.started, 6))

    async def run(self):
        server = await asyncio.start_server(self.handle, self.config.host, self.config.port)
        self.log("listening", host=self.config.host, port=self.config.port,
                 have=self.have, total=self.metadata.block_count)
        workers = [asyncio.create_task(self.download_from(address, i))
                   for i, address in enumerate(self.config.neighbors)]
        try:
            async with server:
                await self.await_file()
                for worker in workers:
                    worker.cancel()
                await asyncio.gather(*workers, return_exceptions=True)
                # Concluir o arquivo não encerra o peer: seus vizinhos podem precisar
                # dele para terminar os próprios downloads.
                await server.serve_forever()
        finally:
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            for writer in tuple(self.connections):
                writer.close()
            for task in tuple(self.handlers):
                task.cancel()
            await asyncio.gather(*self.handlers, return_exceptions=True)
