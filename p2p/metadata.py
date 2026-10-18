"""Descrição verificável do arquivo compartilhado entre os peers."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from dataclasses import dataclass
from typing import Any


MAX_FILE_SIZE = 128 * 1024 * 1024
MAX_BLOCK_SIZE = 1024 * 1024
MAX_BLOCKS = 131_072
_MAX_METADATA_SIZE = 10 * 1024 * 1024
_HASH_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_FIELDS = {"name", "size", "block_size", "sha256", "blocks"}
_RESERVED_NAMES = {"CON", "PRN", "AUX", "NUL"} | {
    f"{prefix}{number}" for prefix in ("COM", "LPT") for number in range(1, 10)
}


def _valid_hash(value: Any) -> bool:
    return isinstance(value, str) and _HASH_PATTERN.fullmatch(value) is not None


def _safe_name(name: Any) -> bool:
    """Aceita apenas um nome de arquivo portátil, sem caminhos ou ADS do Windows."""
    if not isinstance(name, str) or not name or name in (".", ".."):
        return False
    try:
        if len(name.encode("utf-8")) > 255:
            return False
    except UnicodeEncodeError:
        return False
    if name[-1] in (" ", ".") or any(c in '<>:"/\\|?*' for c in name):
        return False
    if any(ord(c) < 32 or ord(c) == 127 for c in name):
        return False
    return name.split(".", 1)[0].upper() not in _RESERVED_NAMES


@dataclass(frozen=True, slots=True)
class Metadata:
    """Metadados imutáveis; cada bloco e o arquivo completo têm SHA-256 próprio."""

    name: str
    size: int
    block_size: int
    sha256: str
    blocks: tuple[str, ...]

    def __post_init__(self) -> None:
        if not _safe_name(self.name):
            raise ValueError("Nome de arquivo inseguro ou inválido nos metadados.")
        if type(self.size) is not int or not 0 <= self.size <= MAX_FILE_SIZE:
            raise ValueError("O tamanho deve estar entre 0 e 128 MiB.")
        if type(self.block_size) is not int or not 1 <= self.block_size <= MAX_BLOCK_SIZE:
            raise ValueError("O tamanho do bloco deve estar entre 1 byte e 1 MiB.")
        expected = (self.size + self.block_size - 1) // self.block_size
        if expected > MAX_BLOCKS:
            raise ValueError("O arquivo excede o limite de 131072 blocos.")
        if not isinstance(self.blocks, tuple) or len(self.blocks) != expected:
            raise ValueError("A quantidade de hashes não corresponde ao tamanho do arquivo.")
        if not _valid_hash(self.sha256) or not all(_valid_hash(h) for h in self.blocks):
            raise ValueError("Os hashes devem ser SHA-256 hexadecimal em letras minúsculas.")
        if self.size == 0 and self.sha256 != hashlib.sha256(b"").hexdigest():
            raise ValueError("O hash informado para o arquivo vazio é inválido.")

    @property
    def block_count(self) -> int:
        return len(self.blocks)

    @property
    def file_id(self) -> str:
        # JSON canônico garante a mesma identificação em todos os peers.
        canonical = json.dumps(
            self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    def expected_size(self, index: int) -> int:
        """O último bloco pode ser menor; índices começam em zero."""
        if type(index) is not int or not 0 <= index < self.block_count:
            raise IndexError("Índice de bloco fora do intervalo.")
        return min(self.block_size, self.size - index * self.block_size)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "size": self.size,
            "block_size": self.block_size,
            "sha256": self.sha256,
            "blocks": list(self.blocks),
        }

    def save(self, path: str | Path) -> None:
        """Salva o JSON por substituição atômica, evitando arquivos pela metade."""
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=target.parent, prefix=".metadata-", delete=False
            ) as output:
                temporary = Path(output.name)
                json.dump(self.to_dict(), output, ensure_ascii=False, indent=2)
                output.write("\n")
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, target)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    @classmethod
    def load(cls, path: str | Path) -> Metadata:
        target = Path(path)
        if target.stat().st_size > _MAX_METADATA_SIZE:
            raise ValueError("Arquivo de metadados grande demais.")
        try:
            with target.open("r", encoding="utf-8") as source:
                data = json.load(source)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("Arquivo de metadados não contém JSON válido.") from exc
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Metadata:
        if not isinstance(data, dict) or set(data) != _FIELDS:
            raise ValueError("Os metadados devem conter exatamente os cinco campos esperados.")
        if not isinstance(data["blocks"], (list, tuple)):
            raise ValueError("O campo blocks deve ser uma lista de hashes.")
        if len(data["blocks"]) > MAX_BLOCKS:
            raise ValueError("Quantidade de blocos acima do limite.")
        return cls(
            name=data["name"], size=data["size"], block_size=data["block_size"],
            sha256=data["sha256"], blocks=tuple(data["blocks"]),
        )

    @classmethod
    def from_file(cls, path: str | Path, block_size: int = 1024) -> Metadata:
        source_path = Path(path)
        if type(block_size) is not int or not 1 <= block_size <= MAX_BLOCK_SIZE:
            raise ValueError("O tamanho do bloco deve estar entre 1 byte e 1 MiB.")
        initial_size = source_path.stat().st_size
        if initial_size > MAX_FILE_SIZE or (initial_size + block_size - 1) // block_size > MAX_BLOCKS:
            raise ValueError("O arquivo excede os limites de tamanho ou quantidade de blocos.")
        digest = hashlib.sha256()
        hashes: list[str] = []
        total = 0
        with source_path.open("rb") as source:
            while data := source.read(block_size):
                total += len(data)
                if total > MAX_FILE_SIZE or len(hashes) >= MAX_BLOCKS:
                    raise ValueError("O arquivo excedeu os limites durante a leitura.")
                digest.update(data)
                hashes.append(hashlib.sha256(data).hexdigest())
        if total != initial_size:
            raise ValueError("O tamanho do arquivo mudou durante a leitura.")
        return cls(source_path.name, total, block_size, digest.hexdigest(), tuple(hashes))
