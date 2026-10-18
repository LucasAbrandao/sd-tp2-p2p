"""Interface de terminal: gerar metadados ou iniciar um peer."""

import argparse
import asyncio
import json
import sys

from .metadata import Metadata
from .peer import Config, Peer


def main():
    parser = argparse.ArgumentParser(description="Transferência P2P por blocos (TP2)")
    commands = parser.add_subparsers(dest="command", required=True)
    metadata = commands.add_parser("metadata", help="Gera o manifesto de um arquivo")
    metadata.add_argument("file")
    metadata.add_argument("--block-size", type=int, default=1024)
    metadata.add_argument("--output", required=True)
    run = commands.add_parser("run", help="Inicia cliente e servidor no mesmo processo")
    run.add_argument("--config", required=True)
    args = parser.parse_args()
    try:
        if args.command == "metadata":
            manifest = Metadata.from_file(args.file, args.block_size)
            manifest.save(args.output)
            print(json.dumps({"metadata": args.output, "blocks": manifest.block_count,
                              "sha256": manifest.sha256}))
        else:
            asyncio.run(Peer(Config.load(args.config)).run())
    except KeyboardInterrupt:
        return 0
    except (ValueError, OSError, KeyError, TypeError) as error:
        print(f"Erro: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
