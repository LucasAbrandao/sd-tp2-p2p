#!/usr/bin/env python3
"""Executa transferências reais e preserva evidências reproduzíveis.

Cada combinação é executada uma única vez, em processos locais independentes.
Os tempos incluem inicialização e, quando indicado, atraso artificial de serviço;
não representam médias nem medições de uma rede física.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SIZES = (10 * 1024, 20 * 1024, 1024**2, 5 * 1024**2, 10 * 1024**2, 20 * 1024**2)


def sha256_file(path: Path) -> str:
    """Calcula o hash por partes para não carregar arquivos grandes na memória."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def available_ports(count: int) -> list[int]:
    # Mantemos todas as reservas abertas até obter portas distintas.
    sockets: list[socket.socket] = []
    try:
        for _ in range(count):
            connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connection.bind(("127.0.0.1", 0))
            sockets.append(connection)
        return [connection.getsockname()[1] for connection in sockets]
    finally:
        for connection in sockets:
            connection.close()


class PeerProcess:
    """Processo com coletores contínuos para evitar bloqueio dos pipes de log."""

    def __init__(self, peer_id: str, config: Path, log_dir: Path) -> None:
        self.peer_id = peer_id
        self.config = config
        self.log_dir = log_dir
        self.events: list[dict[str, Any]] = []
        self.invalid_lines = 0
        self._lock = threading.Lock()
        self._threads: list[threading.Thread] = []
        self.process: subprocess.Popen[str] | None = None

    def start(self) -> None:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        environment = os.environ.copy()
        environment["PYTHONUNBUFFERED"] = "1"
        environment["PYTHONPATH"] = str(PROJECT_ROOT) + os.pathsep + environment.get("PYTHONPATH", "")
        self.process = subprocess.Popen(
            [sys.executable, "-m", "p2p", "run", "--config", str(self.config)],
            cwd=PROJECT_ROOT,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        for stream_name in ("stdout", "stderr"):
            thread = threading.Thread(target=self._collect, args=(stream_name,), daemon=True)
            thread.start()
            self._threads.append(thread)

    def _collect(self, stream_name: str) -> None:
        assert self.process is not None
        stream = getattr(self.process, stream_name)
        assert stream is not None
        suffix = "jsonl" if stream_name == "stdout" else "stderr.log"
        with (self.log_dir / f"{self.peer_id}.{suffix}").open("w", encoding="utf-8") as destination:
            for line in stream:
                destination.write(line)
                destination.flush()
                if stream_name == "stdout":
                    try:
                        event = json.loads(line)
                        if not isinstance(event, dict):
                            raise ValueError("evento não é um objeto")
                    except (ValueError, json.JSONDecodeError):
                        with self._lock:
                            self.invalid_lines += 1
                    else:
                        with self._lock:
                            self.events.append(event)
        stream.close()

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self.events)

    def event(self, name: str) -> dict[str, Any] | None:
        return next((event for event in self.snapshot() if event.get("event") == name), None)

    def stop(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        for thread in self._threads:
            thread.join(timeout=5)

    def diagnostic(self) -> str:
        stderr = self.log_dir / f"{self.peer_id}.stderr.log"
        tail = stderr.read_text(encoding="utf-8")[-1500:] if stderr.exists() else ""
        code = self.process.poll() if self.process is not None else "não iniciado"
        return f"peer {self.peer_id}: saída={code}; stderr={tail.strip()}"


def wait_until(peers: list[PeerProcess], predicate: Any, timeout: float, description: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        for peer in peers:
            if peer.process is not None and peer.process.poll() is not None:
                raise RuntimeError(f"Processo encerrou antes de {description}. {peer.diagnostic()}")
        time.sleep(0.01)
    details = "; ".join(peer.diagnostic() for peer in peers)
    raise TimeoutError(f"Tempo esgotado esperando {description} ({timeout:.1f}s). {details}")


def _compact_event(event: dict[str, Any]) -> dict[str, Any]:
    # As evidências publicadas não precisam dos caminhos absolutos do computador.
    result = dict(event)
    if "path" in result:
        result["path"] = Path(str(result["path"])).name
    return result


def summarize_peer(peer: PeerProcess) -> dict[str, Any]:
    events = peer.snapshot()
    counts: dict[str, int] = {}
    for event in events:
        name = str(event.get("event", "unknown"))
        counts[name] = counts.get(name, 0) + 1
    partial_sends = [
        event for event in events
        if event.get("event") == "block_sent" and 0 < event.get("have", 0) < event.get("total", 0)
    ]
    first_received = [event for event in events if event.get("event") == "block_received"][:5]
    selected = [event for event in events if event.get("event") in ("listening", "download_complete")]
    selected.extend(first_received)
    selected.extend(partial_sends[:5])
    return {
        "peer": peer.peer_id,
        "event_counts": counts,
        "invalid_stdout_lines": peer.invalid_lines,
        "partial_block_sends": len(partial_sends),
        "received_from": sorted({str(event.get("source")) for event in events if event.get("event") == "block_received"}),
        "selected_events": [_compact_event(event) for event in selected],
    }


def run_case(
    peer_count: int,
    block_size: int,
    size_bytes: int,
    work_dir: Path,
    *,
    timeout: float | None = None,
    serve_delay: float | None = None,
    startup_gap: float = 0.0,
    wait_for_seed_retry: bool = False,
) -> dict[str, Any]:
    """Cria os arquivos, inicia peers e verifica todos os resultados pelo SHA-256.

    Com quatro peers, D conhece somente B/C; a fonte A é iniciada por último.
    Isso também exercita a descoberta de vizinhos ainda indisponíveis no início.
    Um atraso pequeno, declarado no resultado, torna o compartilhamento parcial
    observável nos arquivos menores sem depender do escalonador do sistema.
    """
    if peer_count not in (2, 4):
        raise ValueError("A topologia de experimento aceita 2 ou 4 peers")
    if block_size <= 0 or size_bytes <= 0:
        raise ValueError("Tamanhos de arquivo e bloco devem ser positivos")
    case_name = f"p{peer_count}_b{block_size}_s{size_bytes}"
    work_dir = Path(work_dir).resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    # Cada tentativa usa uma pasta nova, inclusive ao repetir a matriz no mesmo local.
    case_dir = Path(tempfile.mkdtemp(prefix=case_name + "_", dir=work_dir))
    source = case_dir / "source.bin"
    source.write_bytes(os.urandom(size_bytes))
    source_hash = sha256_file(source)
    metadata = case_dir / "metadata.json"
    timeout = timeout if timeout is not None else max(120.0, 60.0 + size_bytes / (1024 * 1024) * 8.0)
    if serve_delay is None:
        serve_delay = 0.003 if peer_count == 4 and size_bytes <= 20 * 1024 else 0.0
    result: dict[str, Any] = {
        "case": case_name,
        "peer_count": peer_count,
        "block_size": block_size,
        "size_bytes": size_bytes,
        "source_sha256": source_hash,
        "runs": 1,
        "transport": "TCP em 127.0.0.1; processos locais independentes",
        "serve_delay_seconds": serve_delay,
        "startup_gap_seconds": startup_gap,
        "wait_for_seed_retry": wait_for_seed_retry,
        "timeout_seconds": timeout,
        "status": "failed",
        "verified_peers": [],
        "peers": [],
    }
    peers: list[PeerProcess] = []
    started = time.monotonic()
    try:
        command = [sys.executable, "-m", "p2p", "metadata", str(source), "--block-size", str(block_size), "--output", str(metadata)]
        metadata_run = subprocess.run(command, cwd=PROJECT_ROOT, capture_output=True, text=True, encoding="utf-8", timeout=30)
        if metadata_run.returncode != 0:
            raise RuntimeError(f"Falha ao gerar metadados: {metadata_run.stderr.strip()}")
        names = ["A", "B"] if peer_count == 2 else ["A", "B", "C", "D"]
        ports = dict(zip(names, available_ports(peer_count)))
        neighbors = {"A": ["B"], "B": ["A"]} if peer_count == 2 else {
            "A": ["B", "C"], "B": ["A", "D"], "C": ["A", "D"], "D": ["B", "C"],
        }
        for name in names:
            directory = case_dir / f"peer-{name}"
            directory.mkdir(exist_ok=True)
            config: dict[str, Any] = {
                "id": name,
                "host": "127.0.0.1",
                "port": ports[name],
                "neighbors": [{"host": "127.0.0.1", "port": ports[neighbor]} for neighbor in neighbors[name]],
                "metadata": str(metadata),
                "directory": str(directory),
                "timeout": timeout,
                "request_timeout": 1 if wait_for_seed_retry else 10,
                "poll_interval": 0.005,
                "serve_delay": serve_delay,
            }
            if name == "A":
                config["seed"] = str(source)
            config_path = case_dir / f"config-{name}.json"
            config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
            peers.append(PeerProcess(name, config_path, case_dir / "logs"))
        started = time.monotonic()
        for peer in reversed(peers):
            if peer.peer_id == "A" and wait_for_seed_retry:
                # Sincroniza pelo erro observado, sem supor o tempo de recusa do SO.
                wait_until(
                    peers[1:],
                    lambda: any(item.event("neighbor_error") or item.event("retry") for item in peers[1:]),
                    min(timeout, 15),
                    "primeira tentativa sem a fonte A",
                )
            peer.start()
            wait_until([peer], lambda item=peer: item.event("listening") is not None, 15, f"inicialização de {peer.peer_id}")
            if startup_gap:
                time.sleep(startup_gap)
        wait_until(peers, lambda: all(peer.event("download_complete") is not None for peer in peers[1:]), timeout, "conclusão dos downloads")
        result["wall_download_seconds"] = round(time.monotonic() - started, 6)
        completion_times: list[float] = []
        for peer in peers:
            event = peer.event("download_complete")
            downloaded = source if peer.peer_id == "A" else Path(str((event or {}).get("path", case_dir / f"peer-{peer.peer_id}" / source.name)))
            if not downloaded.is_absolute():
                downloaded = PROJECT_ROOT / downloaded
            actual_size = downloaded.stat().st_size
            actual_hash = sha256_file(downloaded)
            if actual_size != size_bytes or actual_hash != source_hash:
                raise AssertionError(f"Integridade inválida no peer {peer.peer_id}: tamanho={actual_size}, SHA-256={actual_hash}")
            if event is not None and (event.get("sha256") != source_hash or event.get("size") != size_bytes):
                raise AssertionError(f"Evento de conclusão inconsistente no peer {peer.peer_id}")
            result["verified_peers"].append({"peer": peer.peer_id, "size_bytes": actual_size, "sha256": actual_hash})
            if peer.peer_id != "A" and event is not None:
                completion_times.append(float(event["elapsed_seconds"]))
        result["max_peer_elapsed_seconds"] = round(max(completion_times, default=0), 6)
        if peer_count == 4:
            received = [event for event in peers[-1].snapshot() if event.get("event") == "block_received"]
            if not received or any(event.get("source") not in ("B", "C") for event in received):
                raise AssertionError("D precisa receber todos os blocos exclusivamente de B/C")
            result["d_received_from_intermediaries"] = True
            partial = [
                event for peer in peers[1:3] for event in peer.snapshot()
                if event.get("event") == "block_sent" and event.get("target") == "D"
                and 0 < event.get("have", 0) < event.get("total", 0)
            ]
            result["partial_sends_to_d"] = len(partial)
            if not partial:
                raise AssertionError("Não foi observado B/C enviando para D antes de completar o próprio arquivo")
        else:
            result["partial_sends_to_d"] = 0
        result["status"] = "passed"
    except Exception as error:
        result["error"] = f"{type(error).__name__}: {error}"
        result.setdefault("wall_download_seconds", round(time.monotonic() - started, 6))
    finally:
        # Mesmo em falhas/timeout, nenhum peer deve permanecer em execução.
        for peer in peers:
            peer.stop()
        result["peers"] = [summarize_peer(peer) for peer in peers]
    return result


def write_results(results: list[dict[str, Any]], output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    document = {
        "method": "Uma execução por combinação, com processos locais reais e TCP em loopback. Sem média estatística ou emulação de rede física.",
        "size_units": "KiB = 1024 bytes; MiB = 1024² bytes",
        "results": results,
    }
    (output / "results.json").write_text(json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    fields = ["case", "peer_count", "block_size", "size_bytes", "status", "wall_download_seconds", "max_peer_elapsed_seconds", "serve_delay_seconds", "partial_sends_to_d", "source_sha256", "error"]
    with (output / "results.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)
    with (output / "selected-events.jsonl").open("w", encoding="utf-8") as stream:
        for result in results:
            for peer in result["peers"]:
                for event in peer["selected_events"]:
                    stream.write(json.dumps({"case": result["case"], **event}, ensure_ascii=False) + "\n")


def parse_size(value: str) -> int:
    normalized = value.strip().lower()
    for suffix, factor in (("mib", 1024**2), ("kib", 1024), ("mb", 1024**2), ("kb", 1024)):
        if normalized.endswith(suffix):
            return int(float(normalized[:-len(suffix)]) * factor)
    return int(normalized)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("evidence"))
    parser.add_argument("--work-dir", type=Path, default=Path(".runtime/benchmark"))
    parser.add_argument("--quick", action="store_true", help="Executa apenas 10 KiB, bloco de 1024, com 2 e 4 peers")
    parser.add_argument("--sizes", nargs="+", type=parse_size, help="Tamanhos opcionais, por exemplo 10KiB 1MiB")
    parser.add_argument("--timeout", type=float, default=None, help="Limite por caso em segundos")
    args = parser.parse_args()
    sizes = args.sizes or ((10 * 1024,) if args.quick else DEFAULT_SIZES)
    blocks = (1024,) if args.quick else (1024, 4096)
    results: list[dict[str, Any]] = []
    for peer_count in (2, 4):
        for block_size in blocks:
            for size in sizes:
                print(f"Executando peers={peer_count}, bloco={block_size}, arquivo={size} bytes...", flush=True)
                result = run_case(peer_count, block_size, size, args.work_dir, timeout=args.timeout)
                results.append(result)
                write_results(results, args.output)
                print(f"  {result['status']}: {result['wall_download_seconds']:.3f}s" + (f" — {result['error']}" if "error" in result else ""), flush=True)
    passed = sum(result["status"] == "passed" for result in results)
    print(f"Concluído: {passed}/{len(results)} casos aprovados. Evidências em {args.output.resolve()}", flush=True)
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
