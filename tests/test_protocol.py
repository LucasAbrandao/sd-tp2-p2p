"""Contrato TCP e configuração: erros em uma conexão não derrubam o peer."""

import asyncio
import base64
import contextlib
import json
import struct
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from p2p.peer import Config, Peer
from p2p.protocol import MAX_FRAME, ProtocolError, read_message, write_message


def frame(body):
    """Monta bytes brutos para simular mensagens inválidas e entregas parciais."""
    return struct.pack("!I", len(body)) + body


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_fragmented_header_and_body(self):
        reader = asyncio.StreamReader()
        payload = frame(b'{"type":"status"}')
        receiving = asyncio.create_task(read_message(reader))
        # TCP não preserva os limites de cada write do emissor.
        for byte in payload:
            reader.feed_data(bytes((byte,)))
            await asyncio.sleep(0)
        self.assertEqual(await receiving, {"type": "status"})

    async def test_two_frames_remain_separate(self):
        reader = asyncio.StreamReader()
        reader.feed_data(frame(b'{"type":"status"}') + frame(b'{"type":"get","index":7}'))
        self.assertEqual(await read_message(reader), {"type": "status"})
        self.assertEqual(await read_message(reader), {"type": "get", "index": 7})

    async def test_truncated_header_and_body(self):
        for payload in (b"\x00\x00", struct.pack("!I", 10) + b"{}"):
            with self.subTest(payload=payload):
                reader = asyncio.StreamReader()
                reader.feed_data(payload)
                reader.feed_eof()
                with self.assertRaises(asyncio.IncompleteReadError):
                    await read_message(reader)

    async def test_invalid_size_is_rejected_before_body(self):
        for length in (0, MAX_FRAME + 1, 2**32 - 1):
            with self.subTest(length=length):
                reader = asyncio.StreamReader()
                reader.feed_data(struct.pack("!I", length))
                with self.assertRaises(ProtocolError):
                    await read_message(reader)

    async def test_invalid_json_utf8_and_non_object(self):
        for body in (b"{", b"\xff", b"[]", b"null", b'"text"'):
            with self.subTest(body=body):
                reader = asyncio.StreamReader()
                reader.feed_data(frame(body))
                with self.assertRaises(ProtocolError):
                    await read_message(reader)

    async def test_incomplete_live_stream_has_timeout(self):
        reader = asyncio.StreamReader()
        reader.feed_data(b"\x00")
        with self.assertRaises(asyncio.TimeoutError):
            await read_message(reader, timeout=0.02)

    async def test_write_applies_framing_and_backpressure(self):
        class Writer:
            data = b""
            drained = False

            def write(self, data):
                self.data += data

            async def drain(self):
                self.drained = True

        writer = Writer()
        await write_message(writer, {"type": "status"})
        self.assertEqual(writer.data, frame(b'{"type":"status"}'))
        self.assertTrue(writer.drained)
        with self.assertRaises(ProtocolError):
            await write_message(writer, {"data": "a" * MAX_FRAME})

    async def test_listener_survives_bad_connection_and_serves_partial_blocks(self):
        # O teste exercita o handler real com um armazenamento mínimo. Ele possui
        # apenas um dos dois blocos, portanto ainda representa um leecher.
        peer = Peer.__new__(Peer)
        peer.config = SimpleNamespace(id="partial", request_timeout=0.5, serve_delay=0)
        peer.metadata = SimpleNamespace(file_id="file-123", block_count=2)

        def get_block(index):
            if index != 0:
                raise KeyError(index)
            return b"abc"

        peer.store = SimpleNamespace(available={0}, have_count=1, get_block=get_block)
        peer.handlers = set()
        peer.connections = set()
        events = []
        peer.log = lambda event, **fields: events.append((event, fields))
        server = await asyncio.start_server(peer.handle, "127.0.0.1", 0)
        address = server.sockets[0].getsockname()[:2]
        writers = []
        try:
            bad_reader, bad_writer = await asyncio.open_connection(*address)
            writers.append(bad_writer)
            bad_writer.write(frame(b"[]"))
            await bad_writer.drain()
            self.assertEqual(await asyncio.wait_for(bad_reader.read(), 1), b"")
            self.assertTrue(any(event == "connection_error" for event, _ in events))

            reader, writer = await asyncio.open_connection(*address)
            writers.append(writer)
            await write_message(writer, {"type": "hello", "version": 1,
                                         "peer": "other", "file_id": "file-123"})
            self.assertEqual((await read_message(reader))["peer"], "partial")
            await write_message(writer, {"type": "status"})
            self.assertEqual(await read_message(reader), {"type": "status", "blocks": [0]})
            await write_message(writer, {"type": "get", "index": 0})
            block = await read_message(reader)
            self.assertEqual(block["type"], "block")
            self.assertEqual(base64.b64decode(block["data"]), b"abc")
            self.assertTrue(any(event == "block_sent" and fields["have"] == 1
                                for event, fields in events))
            await write_message(writer, {"type": "get", "index": 1})
            self.assertEqual(await read_message(reader), {"type": "missing", "index": 1})
            # Em JSON, bool é distinto de índice numérico, apesar de True == 1
            # em Python. O handler deve rejeitá-lo e fechar somente a conexão.
            await write_message(writer, {"type": "get", "index": True})
            self.assertEqual(await asyncio.wait_for(reader.read(), 1), b"")
        finally:
            for writer in writers:
                writer.close()
                with contextlib.suppress(OSError):
                    await writer.wait_closed()
            server.close()
            await server.wait_closed()
            for handler in tuple(peer.handlers):
                handler.cancel()
            await asyncio.gather(*peer.handlers, return_exceptions=True)


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.path = self.directory / "peer.json"
        self.valid = {"id": "peer-b", "host": "127.0.0.1", "port": 5002,
                      "neighbors": [{"host": "127.0.0.1", "port": 5001}],
                      "metadata": "../shared/file.meta.json", "directory": "downloads"}

    def load(self, changes=None):
        data = {**self.valid, **(changes or {})}
        self.path.write_text(json.dumps(data), encoding="utf-8")
        return Config.load(self.path)

    def test_relative_paths_and_neighbor_deduplication(self):
        config = self.load({"neighbors": self.valid["neighbors"] * 2, "seed": "input.bin"})
        self.assertEqual(config.metadata, (self.directory / "../shared/file.meta.json").resolve())
        self.assertEqual(config.directory, self.directory / "downloads")
        self.assertEqual(config.seed, self.directory / "input.bin")
        self.assertEqual(config.neighbors, (("127.0.0.1", 5001),))

    def test_invalid_ports_hosts_and_required_fields(self):
        for changes in ({"port": True}, {"port": 0}, {"port": 65536}, {"host": " "},
                        {"id": ""}, {"id": "x" * 101}, {"neighbors": []},
                        {"metadata": ""}, {"directory": None},
                        {"neighbors": [{"host": "127.0.0.1", "port": -1}]}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.load(changes)

    def test_invalid_numeric_options(self):
        for changes in ({"timeout": 0}, {"timeout": -1}, {"timeout": True},
                        {"request_timeout": float("nan")}, {"poll_interval": float("inf")},
                        {"serve_delay": -1}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.load(changes)

    def test_zero_serve_delay_is_allowed(self):
        self.assertEqual(self.load({"serve_delay": 0}).serve_delay, 0)


if __name__ == "__main__":
    unittest.main()
