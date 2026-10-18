"""Integração por TCP: processos reais, fontes tardias e integridade dos arquivos."""

from pathlib import Path
import tempfile
import unittest

from scripts.benchmark import run_case


class PeerIntegrationTests(unittest.TestCase):
    def run_transfer(self, peers: int, size: int, block_size: int, **options):
        # Diretórios temporários impedem reaproveitar acidentalmente downloads anteriores.
        with tempfile.TemporaryDirectory(prefix="sd-p2p-test-") as directory:
            result = run_case(peers, block_size, size, Path(directory), timeout=30, **options)
        self.assertEqual(result["status"], "passed", result.get("error", result))
        self.assertEqual(len(result["verified_peers"]), peers)
        self.assertTrue(all(item["sha256"] == result["source_sha256"] for item in result["verified_peers"]))
        self.assertTrue(all(item["size_bytes"] == size for item in result["verified_peers"]))
        return result

    def test_two_peers_with_incomplete_final_block(self):
        # O último bloco menor deve ser reconstruído sem bytes extras.
        self.run_transfer(2, 12 * 1024 + 37, 1024)

    def test_file_smaller_than_one_block(self):
        self.run_transfer(2, 117, 4096)

    def test_four_peers_share_before_completing(self):
        result = self.run_transfer(4, 32 * 1024 + 13, 1024, serve_delay=0.005)
        self.assertTrue(result["d_received_from_intermediaries"])
        self.assertGreater(result["partial_sends_to_d"], 0)
        last_peer = next(peer for peer in result["peers"] if peer["peer"] == "D")
        self.assertTrue(set(last_peer["received_from"]).issubset({"B", "C"}))

    def test_missing_seed_becomes_available_later(self):
        # B inicia primeiro e encontra A indisponível; sua descoberta deve continuar.
        # O início da fonte depende do erro observado, sem assumir prazos do SO.
        result = self.run_transfer(2, 8 * 1024 + 5, 1024, wait_for_seed_retry=True)
        receiver = next(peer for peer in result["peers"] if peer["peer"] == "B")
        counts = receiver["event_counts"]
        self.assertGreater(counts.get("neighbor_error", 0) + counts.get("retry", 0), 0)
        self.assertIn("A", receiver["received_from"])


if __name__ == "__main__":
    unittest.main()
