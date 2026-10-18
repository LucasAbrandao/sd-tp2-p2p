"""Testes de integridade, retomada e validação dos dados recebidos."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from p2p.metadata import MAX_BLOCK_SIZE, MAX_BLOCKS, MAX_FILE_SIZE, Metadata
from p2p.storage import BlockStore


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.payload = b"0123456789abcdefXYZ"
        self.seed = self.root / "sample.bin"
        self.seed.write_bytes(self.payload)
        self.metadata = Metadata.from_file(self.seed, block_size=8)
        self.download = self.root / "download"

    def fill_store(self, metadata=None):
        store = BlockStore(metadata or self.metadata, self.download)
        for index in range(store.metadata.block_count):
            begin = index * store.metadata.block_size
            store.put_block(index, self.payload[begin:begin + store.metadata.block_size])
        return store

    def test_metadata_round_trip_and_partial_last_block(self):
        self.assertEqual(self.metadata.block_count, 3)
        self.assertEqual([self.metadata.expected_size(i) for i in range(3)], [8, 8, 3])
        path = self.root / "metadata" / "sample.json"
        self.metadata.save(path)
        restored = Metadata.load(path)
        self.assertEqual(restored, self.metadata)
        self.assertEqual(restored.file_id, self.metadata.file_id)
        canonical = json.dumps(self.metadata.to_dict(), sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        self.assertEqual(restored.file_id, hashlib.sha256(canonical.encode()).hexdigest())

    def test_blocks_assemble_and_duplicate(self):
        store = BlockStore(self.metadata, self.download)
        self.assertTrue(store.put_block(0, self.payload[:8]))
        self.assertFalse(store.put_block(0, self.payload[:8]))
        self.assertEqual(store.have_count, 1)
        external = store.available
        external.add(2)
        self.assertEqual(store.available, {0})
        for index in (2, 1):
            start = index * 8
            self.assertTrue(store.put_block(index, self.payload[start:start + 8]))
        self.assertTrue(store.complete)
        target = store.assemble()
        self.assertEqual(target.read_bytes(), self.payload)
        self.assertEqual(store.assemble(), target)
        self.assertFalse(list(self.download.glob(".assembling-*")))

    def test_invalid_or_missing_block_does_not_become_available(self):
        store = BlockStore(self.metadata, self.download)
        for index, data in ((0, b"bad"), (0, b"XXXXXXXX"), (3, b""), (-1, b""), (True, self.payload[:8])):
            with self.subTest(index=index, data=data):
                with self.assertRaises(ValueError):
                    store.put_block(index, data)
        self.assertEqual(store.available, set())
        with self.assertRaises(KeyError):
            store.get_block(0)
        with self.assertRaises(ValueError):
            store.assemble()

    def test_resume_revalidates_and_recovers_corrupted_block(self):
        store = self.fill_store()
        (store.block_directory / "1.part").write_bytes(b"CORRUPT!")
        (store.block_directory / "999.part").write_bytes(b"ignored")
        (store.block_directory / ".incoming-abandoned").write_bytes(b"ignored")
        resumed = BlockStore(self.metadata, self.download)
        self.assertEqual(resumed.available, {0, 2})
        self.assertFalse(resumed.complete)
        self.assertTrue(resumed.put_block(1, self.payload[8:16]))
        self.assertEqual(resumed.assemble().read_bytes(), self.payload)

    def test_corruption_after_start_is_not_served_or_assembled(self):
        store = self.fill_store()
        (store.block_directory / "0.part").write_bytes(b"changed!")
        with self.assertRaises(KeyError):
            store.get_block(0)
        self.assertNotIn(0, store.available)
        self.assertFalse(store.complete)
        self.assertTrue(store.put_block(0, self.payload[:8]))
        (store.block_directory / "2.part").unlink()
        with self.assertRaises(ValueError):
            store.assemble()
        self.assertFalse((self.download / self.metadata.name).exists())
        self.assertFalse(list(self.download.glob(".assembling-*")))

    def test_empty_file_has_no_blocks_and_can_be_assembled(self):
        empty = self.root / "empty.bin"
        empty.write_bytes(b"")
        metadata = Metadata.from_file(empty)
        self.assertEqual(metadata.block_count, 0)
        store = BlockStore(metadata, self.download, seed=empty)
        self.assertTrue(store.complete)
        self.assertEqual(store.assemble().read_bytes(), b"")
        with self.assertRaises(IndexError):
            metadata.expected_size(0)

    def test_seed_is_validated_and_served_by_block(self):
        store = BlockStore(self.metadata, self.download, seed=self.seed)
        self.assertEqual(store.available, {0, 1, 2})
        self.assertEqual(store.get_block(2), b"XYZ")
        self.assertEqual(list(store.block_directory.iterdir()), [])
        self.seed.write_bytes(b"X" + self.payload[1:])
        with self.assertRaises(KeyError):
            store.get_block(0)
        self.assertNotIn(0, store.available)
        with self.assertRaises(ValueError):
            BlockStore(self.metadata, self.root / "invalid-seed", seed=self.seed)

    def test_seed_rejects_correct_size_with_wrong_whole_hash(self):
        data = self.metadata.to_dict()
        data["sha256"] = "0" * 64
        wrong = Metadata.from_dict(data)
        with self.assertRaises(ValueError):
            BlockStore(wrong, self.download, seed=self.seed)

    def test_assembly_rejects_wrong_whole_hash_and_preserves_existing_file(self):
        data = self.metadata.to_dict()
        data["sha256"] = "0" * 64
        wrong = Metadata.from_dict(data)
        store = self.fill_store(wrong)
        with self.assertRaises(ValueError):
            store.assemble()
        self.assertFalse((self.download / wrong.name).exists())
        good = self.fill_store()
        target = self.download / self.metadata.name
        target.write_bytes(b"precious existing data")
        with self.assertRaises(FileExistsError):
            good.assemble()
        self.assertEqual(target.read_bytes(), b"precious existing data")

    def test_metadata_rejects_unsafe_names_types_hashes_and_counts(self):
        invalid = [
            ("name", "../escape"), ("name", "C:\\escape"), ("name", "con.txt"),
            ("name", "bad:name"), ("name", ".."), ("name", "tail."),
            ("size", -1), ("size", MAX_FILE_SIZE + 1), ("size", True),
            ("block_size", 0), ("block_size", MAX_BLOCK_SIZE + 1), ("block_size", 8.0),
            ("sha256", "z" * 64), ("blocks", ["a" * 64]),
            ("blocks", ["z" * 64] * 3), ("blocks", "invalid"),
        ]
        for field, value in invalid:
            with self.subTest(field=field, value=value):
                data = self.metadata.to_dict()
                data[field] = value
                with self.assertRaises(ValueError):
                    Metadata.from_dict(data)
        data = self.metadata.to_dict()
        data["size"] = MAX_BLOCKS + 1
        data["block_size"] = 1
        with self.assertRaises(ValueError):
            Metadata.from_dict(data)
        for data in ({}, [], {**self.metadata.to_dict(), "extra": 1}):
            with self.assertRaises(ValueError):
                Metadata.from_dict(data)

    def test_metadata_load_rejects_invalid_json_and_non_object(self):
        path = self.root / "invalid.json"
        for content in ("{", "null", "[]"):
            path.write_text(content, encoding="utf-8")
            with self.assertRaises(ValueError):
                Metadata.load(path)

    def test_parallel_assembly_preserves_order_across_batches(self):
        # Mais de 64 blocos exercitam a passagem entre lotes e o bloco parcial.
        payload = bytes(range(256)) * 18 + b"partial-last-data"
        source = self.root / "many-blocks.bin"
        source.write_bytes(payload)
        metadata = Metadata.from_file(source, block_size=64)
        store = BlockStore(metadata, self.root / "many-blocks-download")
        for index in reversed(range(metadata.block_count)):
            start = index * metadata.block_size
            store.put_block(index, payload[start:start + metadata.block_size])
        self.assertGreater(metadata.block_count, 64)
        self.assertEqual(store.assemble().read_bytes(), payload)


if __name__ == "__main__":
    unittest.main()
