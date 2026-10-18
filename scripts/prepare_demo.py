"""Cria arquivo aleatório, manifesto e configurações estáticas para demonstração."""

import argparse
import json
import os
import sys
from pathlib import Path

# Permite tanto `python scripts/prepare_demo.py` quanto `python -m scripts.prepare_demo`.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from p2p.metadata import Metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path(".runtime/demo"))
    parser.add_argument("--peers", type=int, choices=(2, 4), default=2)
    parser.add_argument("--size", type=int, default=1024 * 1024)
    parser.add_argument("--block-size", type=int, default=1024)
    parser.add_argument("--base-port", type=int, default=9001)
    args = parser.parse_args()
    if not 0 <= args.size <= 128 * 1024 * 1024:
        parser.error("--size deve estar entre 0 e 134217728 bytes")
    if not 1 <= args.base_port <= 65536 - args.peers:
        parser.error("Faixa de portas inválida")
    directory = args.output.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    source = directory / "exemplo.bin"
    if source.exists():
        parser.error("A pasta já contém exemplo.bin; escolha outra pasta para uma nova demonstração")
    source.write_bytes(os.urandom(args.size))
    manifest = Metadata.from_file(source, args.block_size)
    manifest.save(directory / "metadata.json")
    # D não conhece A: sua transferência depende dos blocos compartilhados por B/C.
    topology = {"A": ["B"], "B": ["A"]} if args.peers == 2 else {
        "A": ["B", "C"], "B": ["A", "D"], "C": ["A", "D"], "D": ["B", "C"]}
    ports = {name: args.base_port + i for i, name in enumerate(topology)}
    for name, neighbors in topology.items():
        config = {"id": name, "host": "127.0.0.1", "port": ports[name],
                  "neighbors": [{"host": "127.0.0.1", "port": ports[n]} for n in neighbors],
                  "metadata": "metadata.json", "directory": f"peer-{name}",
                  "timeout": 120, "poll_interval": 0.01}
        if name == "A":
            config["seed"] = "exemplo.bin"
        path = directory / f"{name}.json"
        path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
        print(f'python -m p2p run --config "{path}"')
    print("Execute cada comando em um terminal separado. Encerre com Ctrl+C.")


if __name__ == "__main__":
    main()
